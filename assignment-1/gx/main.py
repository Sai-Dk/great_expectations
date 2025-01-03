import great_expectations as ge
import pandas as pd
from great_expectations.core.batch import BatchRequest

file_path = 'customers.csv'
smtp_address = 'smtp.office365.com'  
smtp_port = 587
sender_login = 'sai.dadvai@accellor.com'
sender_password = 'Ryoiki@tenkai1'  
receiver_email = 'saidikshit9000@gmail.com'

def validate_customer_data(file_path):
    df = pd.read_csv(file_path)
    df_ge = ge.from_pandas(df)

    # Index should be an integer
    df_ge.expect_column_values_to_be_of_type('Index', 'int')

    # Length of customer ID should not be greater than 15
    df_ge.expect_column_value_lengths_to_be_between('Customer Id', min_value=None, max_value=15)

    # First name and last name should be varchar (string)
    df_ge.expect_column_values_to_be_of_type('First Name', 'str')
    df_ge.expect_column_values_to_be_of_type('Last Name', 'str')

    # Subscription date should be a date
    df_ge.expect_column_values_to_match_strftime_format('Subscription Date', '%Y-%m-%d')

    results = df_ge.validate()
    return results

def send_validation_report(file_path, smtp_address, smtp_port, sender_login, sender_password, receiver_email):
    context = ge.get_context()

    # Ensure the datasource configuration
    datasource_config = {
        "name": "my_csv_datasource",
        "class_name": "Datasource",
        "execution_engine": {"class_name": "PandasExecutionEngine"},
        "data_connectors": {
            "my_csv_data_connector": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "C:/Python/assignment-1/gx",
                "default_regex": {
                    "pattern": "(.*)\\.csv",
                    "group_names": ["data_asset_name"]
                }
            }
        }
    }
    context.add_datasource(**datasource_config)

    # Create the batch request
    batch_request = BatchRequest(
        datasource_name="my_csv_datasource",
        data_connector_name="my_csv_data_connector",
        data_asset_name="customers"
    )

    # Define the checkpoint
    checkpoint_name = "my_checkpoint"
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": "customer_suite"
            }
        ],
        "action_list": [
            {
                "name": "email_action",
                "action": {
                    "class_name": "EmailAction",
                    "smtp_address": smtp_address,
                    "smtp_port": smtp_port,
                    "sender_login": sender_login,
                    "sender_password": sender_password,
                    "receiver_email": receiver_email,
                    "subject": "Great Expectations Validation Report"
                }
            }
        ]
    }

    try:
        context.delete_checkpoint(checkpoint_name)
    except Exception as e:
        print(f"Checkpoint '{checkpoint_name}' does not exist or cannot be deleted: {e}")

    # Create and run the checkpoint
    context.add_checkpoint(**checkpoint_config)
    context.run_checkpoint(checkpoint_name=checkpoint_name)

# Run validation
validation_results = validate_customer_data(file_path)
print(validation_results)

# Send email with the validation report
send_validation_report(file_path, smtp_address, smtp_port, sender_login, sender_password, receiver_email)