import os
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as ge
from great_expectations.data_context import DataContext
from great_expectations.core.batch import RuntimeBatchRequest
import json
from fpdf import FPDF

connection_string = "mssql+pyodbc://localhost/your_database_name?driver=ODBC+Driver+17+for+SQL+Server"

# Create a connection to the SQL Server
engine = create_engine(connection_string)

try: 
    sales_df = pd.read_sql('SELECT * FROM [great-expectations].dbo.Sales', engine)
    print("Sales data loaded successfully!")
    print(sales_df.dtypes)
except Exception as e:
    print(f"Error loading data: {e}")


context = DataContext()

suite_name = "sales_expectation_suite"

# We are checking for existing suites because faced many errors around this  
existing_suites = context.list_expectation_suite_names()
if suite_name not in existing_suites:
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

# These are the Runtime batch requests
sales_batch_request = RuntimeBatchRequest(
    datasource_name="my_sqlalchemy_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="sales",
    runtime_parameters={"query": "SELECT * FROM [great-expectations].dbo.Sales"},
    batch_identifiers={"default_identifier_name": "default_identifier"}
)

sales_validator = context.get_validator(batch_request=sales_batch_request, expectation_suite_name=suite_name)

# Expectations for the task
sales_validator.expect_column_values_to_be_between("Total_Revenue", min_value=0)
sales_validator.expect_column_values_to_not_be_null("Total_Cost")
sales_validator.expect_column_values_to_be_between("Unit_Price", min_value=0, max_value=10000)
sales_validator.expect_column_values_to_not_be_null("Units_Sold")
sales_validator.expect_column_value_lengths_to_equal("Order_ID", 9)
sales_validator.expect_column_values_to_be_in_set("Order_Priority", ["H", "L", "C", "M"])
sales_validator.expect_column_values_to_be_in_set("Sales_Channel", ["offline", "online"])
sales_validator.expect_column_values_to_be_of_type("Country", "VARCHAR")
sales_validator.expect_column_values_to_be_of_type("Region", "VARCHAR")


context.save_expectation_suite(sales_validator.expectation_suite)

# Validate the data
sales_results = sales_validator.validate()

# Converting the validation results to a JSON-dictionary(Serializable)
sales_results_dict = sales_results.to_json_dict()

# Converting the dictionary to a JSON-formatted string
sales_results_json = json.dumps(sales_results_dict, indent=2)

# Creating a PDF using fpdf
class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Sales Data Validation Results', 0, 1, 'C')

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'L')
        self.ln(5)

    def chapter_body(self, body):
        self.set_font('Arial', '', 10)
        self.multi_cell(0, 10, body)
        self.ln()

# Initializing PDF
pdf = PDF()
pdf.set_left_margin(10)
pdf.set_right_margin(10)
pdf.add_page()


pdf.chapter_title('Validation Results:')


max_length = 3000  
if len(sales_results_json) > max_length:
    sales_results_json = sales_results_json[:max_length] + "... (truncated)"

pdf.chapter_body(sales_results_json)

# This was additional code added because of no such directory error
output_dir = 'C:\\Python\\assignments-1'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Saving the PDF file
pdf_output_path = os.path.join(output_dir, 'results.pdf')
pdf.output(pdf_output_path)

print(f"Validation results saved to {pdf_output_path}")
