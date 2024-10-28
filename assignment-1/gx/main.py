import pandas as pd
import great_expectations as ge
from great_expectations.data_context import DataContext  
import json  
import smtplib
from email.mime.text import MIMEText


# Here i was facing error while suite creation so i created suite using terminal
suite_name = "customer_suite"


context = DataContext("C:\\Python\\gx")

existing_suites = context.list_expectation_suite_names()
if suite_name not in existing_suites:
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# Function for data validation
def validate_customer_data(file_name):
    # Step 1: Read the CSV file
    customer_df = pd.read_csv(file_name)

    customer_df_ge = ge.from_pandas(customer_df)
    # Validation rules

    # 1. Index should be integer
    customer_df_ge.expect_table_row_count_to_be_between(1, 1000) 
    customer_df_ge.expect_column_values_to_be_in_set('Index', list(range(len(customer_df))))

    # 2. Length of customer ID should not be greater than 15
    customer_df_ge.expect_column_value_lengths_to_be_between('Customer Id', 1, 15)

    # 3. First name and last name should be varchar (assumed to be strings here)
    customer_df_ge.expect_column_values_to_be_of_type('First Name', 'str')
    customer_df_ge.expect_column_values_to_be_of_type('Last Name', 'str')

    # 4. Subscription date should be a date
    customer_df_ge.expect_column_values_to_be_of_type('Subscription Date', 'datetime64[ns]')

    # Step 5: Validate the data
    results = customer_df_ge.validate()

    # Step 6: Prepare the report in JSON format
    report = {
        "success": results['success'],
        "results": []
    }

    # Extracting relevant information from results
    for result in results['results']:
        
        serializable_result = {
            "element_count": result['result'].get('element_count', 0),
            "unexpected_count": result['result'].get('unexpected_count', 0),                                  #This is for serialization of result
            "unexpected_percent": result['result'].get('unexpected_percent', 0.0),                            #To get proper informaton from the 
            "partial_unexpected_list": result['result'].get('partial_unexpected_list', []),                   #code
            "missing_count": result['result'].get('missing_count', 0),
            "missing_percent": result['result'].get('missing_percent', 0.0),
            "unexpected_percent_total": result['result'].get('unexpected_percent_total', 0.0),
            "unexpected_percent_nonmissing": result['result'].get('unexpected_percent_nonmissing', 0.0),
        }

        # Converting expectation_config to a serializable format
        expectation_config = {
            "expectation_type": result['expectation_config']['expectation_type'],
            "kwargs": result['expectation_config']['kwargs'],
            "meta": result['expectation_config'].get('meta', {})
        }

        report['results'].append({
            "success": result['success'],
            "expectation_config": expectation_config,  
            "result": serializable_result,  
            "meta": result.get('meta', {}),
            "exception_info": result.get('exception_info', {})
        })

    return json.dumps(report, indent=2)  


# email_sender.py

def send_email(report):
    sender_email = "sai.dadvai@accellor.com"  
    receiver_email = "piyush.magarde@accellor.com"  
    password = "Ryoiki@tenkai1"  

    msg = MIMEText(report)
    msg['Subject'] = 'Customer Data Validation Report'
    msg['From'] = sender_email
    msg['To'] = receiver_email

    # Step 7: Send the email using Outlook SMTP server
    try:
        with smtplib.SMTP('smtp.office365.com', 587) as server:
            server.starttls()  # Upgrade the connection to a secure encrypted SSL/TLS connection
            server.login(sender_email, password)  # Log in to your email account
            server.sendmail(sender_email, receiver_email, msg.as_string())  # Send the email
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

def test_function():
    return "Test function works!"


if __name__ == "__main__":
    report = validate_customer_data('customers-100000.csv')
    
    # Step 8: Send the report via email
    send_email(report)
    print("Validation report sent via email.")


