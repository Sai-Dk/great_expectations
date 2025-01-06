from great_expectations.core import ExpectationSuite
from great_expectations.data_context import FileDataContext
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.actions import PandasTableValidationResult
from great_expectations.actions.validation_operators import ActionListValidationOperator
from great_expectations.data_context.types import DataContextConfig

import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get email settings from environment variables
sender_email = os.getenv('sender_login')
receiver_email = os.getenv('receiver_email')
password = os.getenv('sender_password')

# Function to send an email report
def send_email_report(report_str):
    """Sends an email with the validation report."""
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Customer Data Validation Report'

    body = report_str
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

# Create a DataContext
data_context_config = DataContextConfig(
    **{
        "plugins_directory": "plugins/",
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "checkpoint_store_name": "checkpoint_store",
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "./great_expectations/expectations",
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "./great_expectations/validations",
                },
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "./great_expectations/evaluation_parameters",
                },
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "./great_expectations/checkpoints",
                },
            },
            "datasource": {
                "class_name": "PandasDatasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "default_inferred_data_connector": {
                        "class_name": "InferredAssetFilesystemDataConnector",
                        "base_directory": "./data",
                        "default_regex": r".*\.csv"
                    }
                }
            }
        },
        "data_docs_sites": {},
    }
)

context = FileDataContext(project_config=data_context_config)

# Load the existing Expectation Suite
expectation_suite_name = "customer_suite"
suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)

# Create Batch Requests (Divide data into two batches)
batch_request_1 = BatchRequest(
    datasource_name="datasource",
    data_connector_name="default_inferred_data_connector",
    data_asset_name="customer.csv",
    partition_identifiers={},
    limit=5000  # First 5k records
)

batch_request_2 = BatchRequest(
    datasource_name="datasource",
    data_connector_name="default_inferred_data_connector",
    data_asset_name="customer.csv",
    partition_identifiers={},
    offset=5000,  # Start from the 5001st record
    limit=5000  # Next 5k records
)

# Create Action Lists
action_list_1 = ActionListValidationOperator(
    action_list=[
        PandasTableValidationResult(
            action_callable=lambda validation_result: print(
                f"Validation Results for Batch 1:\n{validation_result.to_json_dict()}"
            )
        )
    ]
)

action_list_2 = ActionListValidationOperator(
    action_list=[
        PandasTableValidationResult(
            action_callable=lambda validation_result: print(
                f"Validation Results for Batch 2:\n{validation_result.to_json_dict()}"
            )
        ),
        PandasTableValidationResult(
            action_callable=lambda validation_result: send_email_report(
                validation_result.to_json_dict()
            )
        )
    ]
)

# Create Checkpoints
checkpoint_config_1 = {
    "name": "customer_validation_checkpoint_1",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request_1,
            "expectation_suite_name": expectation_suite_name,
            "action_list": action_list_1,
        }
    ],
    "schedules": [
        {
            "schedule_type": "cron",
            "cron_string": "0 0 * * *",  # Run daily at UTC 00:00
        }
    ],
}

checkpoint_config_2 = {
    "name": "customer_validation_checkpoint_2",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request_2,
            "expectation_suite_name": expectation_suite_name,
            "action_list": action_list_2,
        }
    ],
    "schedules": [
        {
            "schedule_type": "cron",
            "cron_string": "0 0 * * *",  # Run daily at UTC 00:00
        }
    ],
}

checkpoint_1 = SimpleCheckpoint(
    name="customer_validation_checkpoint_1",
    config_version=1,
    data_context=context,
    **checkpoint_config_1
)

checkpoint_2 = SimpleCheckpoint(
    name="customer_validation_checkpoint_2",
    config_version=1,
    data_context=context,
    **checkpoint_config_2
)

# Run Checkpoints (for initial validation)
checkpoint_1.run()
checkpoint_2.run()