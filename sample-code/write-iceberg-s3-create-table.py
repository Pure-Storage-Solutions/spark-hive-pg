import boto3
from botocore.client import Config
import os
from pyspark.sql import *

from delta import *
from pyspark.sql.types import StructType, StructField, DecimalType, IntegerType, StringType, DateType

from datetime import datetime 
import time
import argparse


# Set environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'PSFBSAZRALDCBILIJHMOBPMEMKAMNPPDNAFNMFLF'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'EC7AD50d3ffdc0a+aec7+B2D0F591a6963432HOMD'
os.environ['AWS_S3_ENDPOINT'] = '10.21.236.205'
os.environ['AWS_REGION'] = 'us-east-1'


# # # Set environment variables S500
# os.environ['AWS_ACCESS_KEY_ID'] = 'PSFBSAZQMNEGEMLPKOFBJEENIPKDDLPGJPDNHCKE'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'F19FD3F96fb33af8+ca51/A80339EBf78a9b58BMJK'
# os.environ['AWS_S3_ENDPOINT'] = '10.21.236.98'
# os.environ['AWS_REGION'] = 'us-east-1'


# Set up command-line argument parsing for flashblade_model and scale
parser = argparse.ArgumentParser(description="Run TPC-DS benchmark on Spark")
parser.add_argument("--flashblade_model", type=str, required=True, help="FlashBlade model for testing")
parser.add_argument("--scale", type=int, required=True, help="Scale value for the benchmark")
parser.add_argument("--nodes", type=int, required=False, help="Number of spark workers")
parser.add_argument("--table_format", type=str, required=True, help="Table format")
parser.add_argument("--file_format", type=str, required=False, help="File format")

args = parser.parse_args()

flashblade_model = args.flashblade_model  # Set FlashBlade model from command line
scale = args.scale  # Set scale value from command line
nodes = args.nodes
table_format = args.table_format
file_format = args.file_format
db_name = ""



# Define the schema for dbgen_version table
dbgen_version_schema = StructType([
    StructField("dv_version", StringType(), True),          # Version of the data generator
    StructField("dv_create_date", DateType(), True),        # Date when data was generated
    StructField("dv_create_time", StringType(), True),      # Time when data was generated
    StructField("dv_cmdline_args", StringType(), True)      # Command-line arguments used
])


# Define the schema for customer_address table
customer_address_schema = StructType([
    StructField("ca_address_sk", IntegerType(), True),       # Surrogate key
    StructField("ca_address_id", StringType(), True),        # Unique identifier for the address
    StructField("ca_street_number", StringType(), True),     # Street number
    StructField("ca_street_name", StringType(), True),       # Street name
    StructField("ca_street_type", StringType(), True),       # Type of street (Ave, Blvd, etc.)
    StructField("ca_suite_number", StringType(), True),      # Suite or apartment number
    StructField("ca_city", StringType(), True),              # City
    StructField("ca_county", StringType(), True),            # County
    StructField("ca_state", StringType(), True),             # State
    StructField("ca_zip", StringType(), True),               # Zip code
    StructField("ca_country", StringType(), True),           # Country
    StructField("ca_gmt_offset", DecimalType(5, 2), True),   # GMT offset (Decimal 5,2)
    StructField("ca_location_type", StringType(), True)      # Location type (residential, business)
])


# Define the schema for customer_demographics table
customer_demographics_schema = StructType([
    StructField("cd_demo_sk", IntegerType(), True),              # Surrogate key
    StructField("cd_gender", StringType(), True),                # Gender ('M', 'F', 'U')
    StructField("cd_marital_status", StringType(), True),        # Marital status ('S', 'M', 'D', 'U')
    StructField("cd_education_status", StringType(), True),      # Education status (e.g., 'Advanced Degree')
    StructField("cd_purchase_estimate", IntegerType(), True),    # Estimated annual purchases
    StructField("cd_credit_rating", StringType(), True),         # Credit rating (Low, Medium, High)
    StructField("cd_dep_count", IntegerType(), True),            # Number of dependents
    StructField("cd_dep_employed_count", IntegerType(), True),   # Number of employed dependents
    StructField("cd_dep_college_count", IntegerType(), True)     # Number of dependents in college
])


# Define the schema for date_dim table
date_dim_schema = StructType([
    StructField("d_date_sk", IntegerType(), True),               # Surrogate key
    StructField("d_date_id", StringType(), True),                # Unique identifier for the date
    StructField("d_date", DateType(), True),                     # Actual date
    StructField("d_month_seq", IntegerType(), True),             # Month sequence number
    StructField("d_week_seq", IntegerType(), True),              # Week sequence number
    StructField("d_quarter_seq", IntegerType(), True),           # Quarter sequence number
    StructField("d_year", IntegerType(), True),                  # Year of the date
    StructField("d_dow", IntegerType(), True),                   # Day of week (0-6)
    StructField("d_moy", IntegerType(), True),                   # Month of year (1-12)
    StructField("d_dom", IntegerType(), True),                   # Day of month
    StructField("d_qoy", IntegerType(), True),                   # Quarter of year (1-4)
    StructField("d_fy_year", IntegerType(), True),               # Fiscal year
    StructField("d_fy_quarter_seq", IntegerType(), True),        # Fiscal quarter sequence number
    StructField("d_fy_week_seq", IntegerType(), True),           # Fiscal week sequence number
    StructField("d_day_name", StringType(), True),               # Day name
    StructField("d_month_name", StringType(), True),             # Month name
    StructField("d_holiday", StringType(), True),                # Is it a holiday (Y/N)?
    StructField("d_weekend", StringType(), True),                # Is it a weekend (Y/N)?
    StructField("d_following_holiday", StringType(), True),      # Is the following day a holiday (Y/N)?
    StructField("d_first_dom", IntegerType(), True),             # First day of the month
    StructField("d_last_dom", IntegerType(), True),              # Last day of the month
    StructField("d_same_day_ly", DateType(), True),              # Same day last year
    StructField("d_same_day_lq", DateType(), True),              # Same day last quarter
    StructField("d_current_day", StringType(), True),            # Is it the current day (Y/N)?
    StructField("d_current_week", StringType(), True),           # Is it the current week (Y/N)?
    StructField("d_current_month", StringType(), True),          # Is it the current month (Y/N)?
    StructField("d_current_quarter", StringType(), True),        # Is it the current quarter (Y/N)?
    StructField("d_current_year", StringType(), True)            # Is it the current year (Y/N)?
])


# Define the schema for warehouse table
warehouse_schema = StructType([
    StructField("w_warehouse_sk", IntegerType(), True),         # Surrogate key
    StructField("w_warehouse_id", StringType(), True),          # Unique identifier for the warehouse
    StructField("w_warehouse_name", StringType(), True),        # Name of the warehouse
    StructField("w_warehouse_sq_ft", IntegerType(), True),      # Size of the warehouse in square feet
    StructField("w_street_number", StringType(), True),         # Street number
    StructField("w_street_name", StringType(), True),           # Street name
    StructField("w_street_type", StringType(), True),           # Street type (e.g., 'Ave', 'Blvd')
    StructField("w_suite_number", StringType(), True),          # Suite number
    StructField("w_city", StringType(), True),                  # City of the warehouse
    StructField("w_county", StringType(), True),                # County of the warehouse
    StructField("w_state", StringType(), True),                 # State of the warehouse
    StructField("w_zip", StringType(), True),                   # ZIP code
    StructField("w_country", StringType(), True),               # Country
    StructField("w_gmt_offset", DecimalType(5, 2), True)        # GMT offset of the warehouse
])

# Define the schema for ship_mode table
ship_mode_schema = StructType([
    StructField("sm_ship_mode_sk", IntegerType(), True),   # Surrogate key
    StructField("sm_ship_mode_id", StringType(), True),    # Unique identifier for the ship mode
    StructField("sm_type", StringType(), True),            # Type of ship mode (e.g., 'AIR', 'GROUND')
    StructField("sm_code", StringType(), True),            # Code for the ship mode
    StructField("sm_carrier", StringType(), True),         # Name of the carrier
    StructField("sm_contract", StringType(), True)         # Shipping contract details
])



# Define the schema for time_dim table
time_dim_schema = StructType([
    StructField("t_time_sk", IntegerType(), True),    # Surrogate key
    StructField("t_time_id", StringType(), True),     # Unique identifier for the time
    StructField("t_time", IntegerType(), True),       # Time in seconds after midnight
    StructField("t_hour", IntegerType(), True),       # Hour of the day (0-23)
    StructField("t_minute", IntegerType(), True),     # Minute within the hour (0-59)
    StructField("t_second", IntegerType(), True),     # Second within the minute (0-59)
    StructField("t_am_pm", StringType(), True),       # AM or PM indicator
    StructField("t_shift", StringType(), True),       # Work shift
    StructField("t_sub_shift", StringType(), True),   # Sub-shift
    StructField("t_meal_time", StringType(), True)    # Meal period (e.g., 'Lunch', 'Dinner')
])


# Define the schema for reason table
reason_schema = StructType([
    StructField("r_reason_sk", IntegerType(), True),   # Surrogate key
    StructField("r_reason_id", StringType(), True),    # Unique identifier for the reason
    StructField("r_reason_desc", StringType(), True)   # Description of the reason
])


# Define the schema for income_band table
income_band_schema = StructType([
    StructField("ib_income_band_sk", IntegerType(), True),  # Surrogate key
    StructField("ib_lower_bound", IntegerType(), True),     # Lower bound of the income range
    StructField("ib_upper_bound", IntegerType(), True)      # Upper bound of the income range
])

# Define the schema for item table
item_schema = StructType([
    StructField("i_item_sk", IntegerType(), False),                   # Primary key, not null
    StructField("i_item_id", StringType(), False),                   # Unique identifier for the item
    StructField("i_rec_start_date", DateType(), True),               # Record start date
    StructField("i_rec_end_date", DateType(), True),                 # Record end date
    StructField("i_item_desc", StringType(), True),                  # Item description
    StructField("i_current_price", DecimalType(7, 2), True),         # Current price
    StructField("i_wholesale_cost", DecimalType(7, 2), True),       # Wholesale cost
    StructField("i_brand_id", IntegerType(), True),                  # Brand ID
    StructField("i_brand", StringType(), True),                      # Brand name
    StructField("i_class_id", IntegerType(), True),                  # Class ID
    StructField("i_class", StringType(), True),                      # Class name
    StructField("i_category_id", IntegerType(), True),               # Category ID
    StructField("i_category", StringType(), True),                   # Category name
    StructField("i_manufact_id", IntegerType(), True),               # Manufacturer ID
    StructField("i_manufact", StringType(), True),                   # Manufacturer name
    StructField("i_size", StringType(), True),                       # Item size
    StructField("i_formulation", StringType(), True),                # Formulation
    StructField("i_color", StringType(), True),                      # Color
    StructField("i_units", StringType(), True),                      # Units
    StructField("i_container", StringType(), True),                  # Container type
    StructField("i_manager_id", IntegerType(), True),                # Manager ID
    StructField("i_product_name", StringType(), True)                # Product name
])

# Define the schema for store table
store_schema = StructType([
    StructField("s_store_sk", IntegerType(), False),                  # Primary key, not null
    StructField("s_store_id", StringType(), False),                  # Unique store identifier
    StructField("s_rec_start_date", DateType(), True),               # Record start date
    StructField("s_rec_end_date", DateType(), True),                 # Record end date
    StructField("s_closed_date_sk", IntegerType(), True),            # Closed date surrogate key
    StructField("s_store_name", StringType(), True),                 # Store name
    StructField("s_number_employees", IntegerType(), True),          # Number of employees
    StructField("s_floor_space", IntegerType(), True),               # Floor space in square feet
    StructField("s_hours", StringType(), True),                      # Store operating hours
    StructField("s_manager", StringType(), True),                    # Store manager's name
    StructField("s_market_id", IntegerType(), True),                 # Market ID
    StructField("s_geography_class", StringType(), True),            # Geography class
    StructField("s_market_desc", StringType(), True),                # Market description
    StructField("s_market_manager", StringType(), True),             # Market manager's name
    StructField("s_division_id", IntegerType(), True),               # Division ID
    StructField("s_division_name", StringType(), True),              # Division name
    StructField("s_company_id", IntegerType(), True),                # Company ID
    StructField("s_company_name", StringType(), True),               # Company name
    StructField("s_street_number", StringType(), True),              # Street number
    StructField("s_street_name", StringType(), True),                # Street name
    StructField("s_street_type", StringType(), True),                # Street type
    StructField("s_suite_number", StringType(), True),               # Suite number
    StructField("s_city", StringType(), True),                       # City
    StructField("s_county", StringType(), True),                     # County
    StructField("s_state", StringType(), True),                      # State abbreviation
    StructField("s_zip", StringType(), True),                        # ZIP code
    StructField("s_country", StringType(), True),                    # Country
    StructField("s_gmt_offset", DecimalType(5, 2), True),           # GMT offset
    StructField("s_tax_precentage", DecimalType(5, 2), True)        # Tax percentage
])


# Define the schema for call_center table
call_center_schema = StructType([
    StructField("cc_call_center_sk", IntegerType(), False),            # Primary key, not null
    StructField("cc_call_center_id", StringType(), False),            # Unique call center identifier
    StructField("cc_rec_start_date", DateType(), True),               # Record start date
    StructField("cc_rec_end_date", DateType(), True),                 # Record end date
    StructField("cc_closed_date_sk", IntegerType(), True),            # Closed date surrogate key
    StructField("cc_open_date_sk", IntegerType(), True),              # Open date surrogate key
    StructField("cc_name", StringType(), True),                       # Call center name
    StructField("cc_class", StringType(), True),                      # Call center class
    StructField("cc_employees", IntegerType(), True),                 # Number of employees
    StructField("cc_sq_ft", IntegerType(), True),                     # Square footage
    StructField("cc_hours", StringType(), True),                      # Call center operating hours
    StructField("cc_manager", StringType(), True),                    # Call center manager's name
    StructField("cc_mkt_id", IntegerType(), True),                   # Market ID
    StructField("cc_mkt_class", StringType(), True),                  # Market class
    StructField("cc_mkt_desc", StringType(), True),                   # Market description
    StructField("cc_market_manager", StringType(), True),             # Market manager's name
    StructField("cc_division", IntegerType(), True),                  # Division ID
    StructField("cc_division_name", StringType(), True),              # Division name
    StructField("cc_company", IntegerType(), True),                   # Company ID
    StructField("cc_company_name", StringType(), True),               # Company name
    StructField("cc_street_number", StringType(), True),              # Street number
    StructField("cc_street_name", StringType(), True),                # Street name
    StructField("cc_street_type", StringType(), True),                # Street type
    StructField("cc_suite_number", StringType(), True),               # Suite number
    StructField("cc_city", StringType(), True),                       # City
    StructField("cc_county", StringType(), True),                     # County
    StructField("cc_state", StringType(), True),                      # State abbreviation
    StructField("cc_zip", StringType(), True),                        # ZIP code
    StructField("cc_country", StringType(), True),                    # Country
    StructField("cc_gmt_offset", DecimalType(5, 2), True),           # GMT offset
    StructField("cc_tax_percentage", DecimalType(5, 2), True)        # Tax percentage
])


# Define the schema for customer table
customer_schema = StructType([
    StructField("c_customer_sk", IntegerType(), False),            # Primary key, not null
    StructField("c_customer_id", StringType(), False),            # Unique customer identifier
    StructField("c_current_cdemo_sk", IntegerType(), True),       # Current customer demographic surrogate key
    StructField("c_current_hdemo_sk", IntegerType(), True),       # Current household demographic surrogate key
    StructField("c_current_addr_sk", IntegerType(), True),        # Current address surrogate key
    StructField("c_first_shipto_date_sk", IntegerType(), True),   # First ship-to date surrogate key
    StructField("c_first_sales_date_sk", IntegerType(), True),    # First sales date surrogate key
    StructField("c_salutation", StringType(), True),               # Salutation
    StructField("c_first_name", StringType(), True),               # First name
    StructField("c_last_name", StringType(), True),                # Last name
    StructField("c_preferred_cust_flag", StringType(), True),      # Preferred customer flag
    StructField("c_birth_day", IntegerType(), True),               # Birth day
    StructField("c_birth_month", IntegerType(), True),             # Birth month
    StructField("c_birth_year", IntegerType(), True),              # Birth year
    StructField("c_birth_country", StringType(), True),            # Birth country
    StructField("c_login", StringType(), True),                    # Login
    StructField("c_email_address", StringType(), True),            # Email address
    StructField("c_last_review_date", StringType(), True)          # Last review date
])

# Define the schema for web_site table
web_site_schema = StructType([
    StructField("web_site_sk", IntegerType(), False),               # Primary key, not null
    StructField("web_site_id", StringType(), False),               # Unique website identifier
    StructField("web_rec_start_date", DateType(), True),          # Record start date
    StructField("web_rec_end_date", DateType(), True),            # Record end date
    StructField("web_name", StringType(), True),                   # Website name
    StructField("web_open_date_sk", IntegerType(), True),          # Open date surrogate key
    StructField("web_close_date_sk", IntegerType(), True),         # Close date surrogate key
    StructField("web_class", StringType(), True),                  # Website class
    StructField("web_manager", StringType(), True),                # Manager name
    StructField("web_mkt_id", IntegerType(), True),                # Marketing identifier
    StructField("web_mkt_class", StringType(), True),              # Marketing class
    StructField("web_mkt_desc", StringType(), True),               # Marketing description
    StructField("web_market_manager", StringType(), True),         # Market manager name
    StructField("web_company_id", IntegerType(), True),            # Company identifier
    StructField("web_company_name", StringType(), True),           # Company name
    StructField("web_street_number", StringType(), True),          # Street number
    StructField("web_street_name", StringType(), True),            # Street name
    StructField("web_street_type", StringType(), True),            # Street type
    StructField("web_suite_number", StringType(), True),           # Suite number
    StructField("web_city", StringType(), True),                   # City
    StructField("web_county", StringType(), True),                 # County
    StructField("web_state", StringType(), True),                  # State
    StructField("web_zip", StringType(), True),                    # ZIP code
    StructField("web_country", StringType(), True),                # Country
    StructField("web_gmt_offset", DecimalType(5, 2), True),       # GMT offset
    StructField("web_tax_percentage", DecimalType(5, 2), True)     # Tax percentage
])


# Define the schema for store_returns table
store_returns_schema = StructType([
    StructField("sr_returned_date_sk", IntegerType(), True),            # Returned date surrogate key
    StructField("sr_return_time_sk", IntegerType(), True),              # Return time surrogate key
    StructField("sr_item_sk", IntegerType(), False),                   # Item surrogate key, Primary key
    StructField("sr_customer_sk", IntegerType(), True),                # Customer surrogate key
    StructField("sr_cdemo_sk", IntegerType(), True),                   # Customer demographics surrogate key
    StructField("sr_hdemo_sk", IntegerType(), True),                   # Household demographics surrogate key
    StructField("sr_addr_sk", IntegerType(), True),                    # Address surrogate key
    StructField("sr_store_sk", IntegerType(), True),                   # Store surrogate key
    StructField("sr_reason_sk", IntegerType(), True),                  # Reason for return surrogate key
    StructField("sr_ticket_number", IntegerType(), False),             # Ticket number, Primary key
    StructField("sr_return_quantity", IntegerType(), True),            # Quantity returned
    StructField("sr_return_amt", DecimalType(7, 2), True),            # Return amount
    StructField("sr_return_tax", DecimalType(7, 2), True),            # Return tax
    StructField("sr_return_amt_inc_tax", DecimalType(7, 2), True),    # Return amount including tax
    StructField("sr_fee", DecimalType(7, 2), True),                    # Return fee
    StructField("sr_return_ship_cost", DecimalType(7, 2), True),      # Return shipping cost
    StructField("sr_refunded_cash", DecimalType(7, 2), True),         # Refunded cash amount
    StructField("sr_reversed_charge", DecimalType(7, 2), True),       # Reversed charge amount
    StructField("sr_store_credit", DecimalType(7, 2), True),          # Store credit amount
    StructField("sr_net_loss", DecimalType(7, 2), True)               # Net loss amount
])


# Define the schema for household_demographics table
household_demographics_schema = StructType([
    StructField("hd_demo_sk", IntegerType(), False),                   # Demographics surrogate key, Primary key
    StructField("hd_income_band_sk", IntegerType(), True),            # Income band surrogate key
    StructField("hd_buy_potential", StringType(), True),              # Buying potential
    StructField("hd_dep_count", IntegerType(), True),                 # Dependency count
    StructField("hd_vehicle_count", IntegerType(), True)              # Vehicle count
])


# Define the schema for web_page table
web_page_schema = StructType([
    StructField("wp_web_page_sk", IntegerType(), False),              # Web page surrogate key, Primary key
    StructField("wp_web_page_id", StringType(), False),              # Web page ID
    StructField("wp_rec_start_date", DateType(), True),              # Record start date
    StructField("wp_rec_end_date", DateType(), True),                # Record end date
    StructField("wp_creation_date_sk", IntegerType(), True),         # Creation date surrogate key
    StructField("wp_access_date_sk", IntegerType(), True),           # Access date surrogate key
    StructField("wp_autogen_flag", StringType(), True),              # Auto-generated flag
    StructField("wp_customer_sk", IntegerType(), True),              # Customer surrogate key
    StructField("wp_url", StringType(), True),                       # URL
    StructField("wp_type", StringType(), True),                      # Type
    StructField("wp_char_count", IntegerType(), True),               # Character count
    StructField("wp_link_count", IntegerType(), True),               # Link count
    StructField("wp_image_count", IntegerType(), True),              # Image count
    StructField("wp_max_ad_count", IntegerType(), True)              # Max ad count
])


# Define the schema for promotion table
promotion_schema = StructType([
    StructField("p_promo_sk", IntegerType(), False),                    # Promo surrogate key, Primary key
    StructField("p_promo_id", StringType(), False),                    # Promo ID
    StructField("p_start_date_sk", IntegerType(), True),               # Start date surrogate key
    StructField("p_end_date_sk", IntegerType(), True),                 # End date surrogate key
    StructField("p_item_sk", IntegerType(), True),                     # Item surrogate key
    StructField("p_cost", DecimalType(15, 2), True),                   # Cost
    StructField("p_response_target", IntegerType(), True),             # Response target
    StructField("p_promo_name", StringType(), True),                   # Promo name
    StructField("p_channel_dmail", StringType(), True),                # Direct mail channel
    StructField("p_channel_email", StringType(), True),                # Email channel
    StructField("p_channel_catalog", StringType(), True),              # Catalog channel
    StructField("p_channel_tv", StringType(), True),                   # TV channel
    StructField("p_channel_radio", StringType(), True),                # Radio channel
    StructField("p_channel_press", StringType(), True),                # Press channel
    StructField("p_channel_event", StringType(), True),                # Event channel
    StructField("p_channel_demo", StringType(), True),                 # Demo channel
    StructField("p_channel_details", StringType(), True),              # Channel details
    StructField("p_purpose", StringType(), True),                      # Promo purpose
    StructField("p_discount_active", StringType(), True)               # Discount active flag
])

# Define the schema for catalog_page table
catalog_page_schema = StructType([
    StructField("cp_catalog_page_sk", IntegerType(), False),            # Catalog page surrogate key, Primary key
    StructField("cp_catalog_page_id", StringType(), False),            # Catalog page ID
    StructField("cp_start_date_sk", IntegerType(), True),             # Start date surrogate key
    StructField("cp_end_date_sk", IntegerType(), True),               # End date surrogate key
    StructField("cp_department", StringType(), True),                  # Department name
    StructField("cp_catalog_number", IntegerType(), True),            # Catalog number
    StructField("cp_catalog_page_number", IntegerType(), True),       # Catalog page number
    StructField("cp_description", StringType(), True),                # Description
    StructField("cp_type", StringType(), True)                        # Type of catalog page
])



# Define the schema for inventory table
inventory_schema = StructType([
    StructField("inv_date_sk", IntegerType(), False),            # Inventory date surrogate key, Primary key
    StructField("inv_item_sk", IntegerType(), False),            # Inventory item surrogate key, Primary key
    StructField("inv_warehouse_sk", IntegerType(), False),       # Inventory warehouse surrogate key, Primary key
    StructField("inv_quantity_on_hand", IntegerType(), True)    # Quantity on hand
])


# Define the schema for catalog_returns table
catalog_returns_schema = StructType([
    StructField("cr_returned_date_sk", IntegerType(), True),           # Returned date surrogate key
    StructField("cr_returned_time_sk", IntegerType(), True),           # Returned time surrogate key
    StructField("cr_item_sk", IntegerType(), False),                   # Item surrogate key, Primary key
    StructField("cr_refunded_customer_sk", IntegerType(), True),        # Refunded customer surrogate key
    StructField("cr_refunded_cdemo_sk", IntegerType(), True),           # Refunded customer demographics key
    StructField("cr_refunded_hdemo_sk", IntegerType(), True),           # Refunded household demographics key
    StructField("cr_refunded_addr_sk", IntegerType(), True),            # Refunded address key
    StructField("cr_returning_customer_sk", IntegerType(), True),       # Returning customer surrogate key
    StructField("cr_returning_cdemo_sk", IntegerType(), True),          # Returning customer demographics key
    StructField("cr_returning_hdemo_sk", IntegerType(), True),          # Returning household demographics key
    StructField("cr_returning_addr_sk", IntegerType(), True),           # Returning address key
    StructField("cr_call_center_sk", IntegerType(), True),              # Call center surrogate key
    StructField("cr_catalog_page_sk", IntegerType(), True),             # Catalog page surrogate key
    StructField("cr_ship_mode_sk", IntegerType(), True),                # Ship mode surrogate key
    StructField("cr_warehouse_sk", IntegerType(), True),                # Warehouse surrogate key
    StructField("cr_reason_sk", IntegerType(), True),                   # Reason for return key
    StructField("cr_order_number", IntegerType(), False),               # Order number, Primary key
    StructField("cr_return_quantity", IntegerType(), True),             # Quantity returned
    StructField("cr_return_amount", DecimalType(7, 2), True),          # Return amount
    StructField("cr_return_tax", DecimalType(7, 2), True),             # Return tax
    StructField("cr_return_amt_inc_tax", DecimalType(7, 2), True),     # Return amount including tax
    StructField("cr_fee", DecimalType(7, 2), True),                     # Fee for return
    StructField("cr_return_ship_cost", DecimalType(7, 2), True),       # Shipping cost for return
    StructField("cr_refunded_cash", DecimalType(7, 2), True),          # Refunded cash amount
    StructField("cr_reversed_charge", DecimalType(7, 2), True),        # Reversed charge amount
    StructField("cr_store_credit", DecimalType(7, 2), True),           # Store credit amount
    StructField("cr_net_loss", DecimalType(7, 2), True)                # Net loss from return
])



# Define the schema for web_returns table
web_returns_schema = StructType([
    StructField("wr_returned_date_sk", IntegerType(), True),           # Returned date surrogate key
    StructField("wr_returned_time_sk", IntegerType(), True),           # Returned time surrogate key
    StructField("wr_item_sk", IntegerType(), False),                   # Item surrogate key, Primary key
    StructField("wr_refunded_customer_sk", IntegerType(), True),        # Refunded customer surrogate key
    StructField("wr_refunded_cdemo_sk", IntegerType(), True),           # Refunded customer demographics key
    StructField("wr_refunded_hdemo_sk", IntegerType(), True),           # Refunded household demographics key
    StructField("wr_refunded_addr_sk", IntegerType(), True),            # Refunded address key
    StructField("wr_returning_customer_sk", IntegerType(), True),       # Returning customer surrogate key
    StructField("wr_returning_cdemo_sk", IntegerType(), True),          # Returning customer demographics key
    StructField("wr_returning_hdemo_sk", IntegerType(), True),          # Returning household demographics key
    StructField("wr_returning_addr_sk", IntegerType(), True),           # Returning address key
    StructField("wr_web_page_sk", IntegerType(), True),                 # Web page surrogate key
    StructField("wr_reason_sk", IntegerType(), True),                   # Reason for return key
    StructField("wr_order_number", IntegerType(), False),               # Order number, Primary key
    StructField("wr_return_quantity", IntegerType(), True),             # Quantity returned
    StructField("wr_return_amt", DecimalType(7, 2), True),             # Return amount
    StructField("wr_return_tax", DecimalType(7, 2), True),             # Return tax
    StructField("wr_return_amt_inc_tax", DecimalType(7, 2), True),     # Return amount including tax
    StructField("wr_fee", DecimalType(7, 2), True),                     # Fee for return
    StructField("wr_return_ship_cost", DecimalType(7, 2), True),       # Shipping cost for return
    StructField("wr_refunded_cash", DecimalType(7, 2), True),          # Refunded cash amount
    StructField("wr_reversed_charge", DecimalType(7, 2), True),        # Reversed charge amount
    StructField("wr_account_credit", DecimalType(7, 2), True),         # Account credit amount
    StructField("wr_net_loss", DecimalType(7, 2), True)                # Net loss from return
])



# Define the schema for web_sales table
web_sales_schema = StructType([
    StructField("ws_sold_date_sk", IntegerType(), True),                 # Sold date surrogate key
    StructField("ws_sold_time_sk", IntegerType(), True),                 # Sold time surrogate key
    StructField("ws_ship_date_sk", IntegerType(), True),                 # Ship date surrogate key
    StructField("ws_item_sk", IntegerType(), False),                     # Item surrogate key, Primary key
    StructField("ws_bill_customer_sk", IntegerType(), True),             # Billing customer surrogate key
    StructField("ws_bill_cdemo_sk", IntegerType(), True),                # Billing customer demographics key
    StructField("ws_bill_hdemo_sk", IntegerType(), True),                # Billing household demographics key
    StructField("ws_bill_addr_sk", IntegerType(), True),                 # Billing address key
    StructField("ws_ship_customer_sk", IntegerType(), True),             # Shipping customer surrogate key
    StructField("ws_ship_cdemo_sk", IntegerType(), True),                # Shipping customer demographics key
    StructField("ws_ship_hdemo_sk", IntegerType(), True),                # Shipping household demographics key
    StructField("ws_ship_addr_sk", IntegerType(), True),                 # Shipping address key
    StructField("ws_web_page_sk", IntegerType(), True),                  # Web page surrogate key
    StructField("ws_web_site_sk", IntegerType(), True),                  # Web site surrogate key
    StructField("ws_ship_mode_sk", IntegerType(), True),                 # Shipping mode surrogate key
    StructField("ws_warehouse_sk", IntegerType(), True),                 # Warehouse surrogate key
    StructField("ws_promo_sk", IntegerType(), True),                     # Promotion surrogate key
    StructField("ws_order_number", IntegerType(), False),                # Order number, Primary key
    StructField("ws_quantity", IntegerType(), True),                     # Quantity sold
    StructField("ws_wholesale_cost", DecimalType(7, 2), True),          # Wholesale cost
    StructField("ws_list_price", DecimalType(7, 2), True),              # List price
    StructField("ws_sales_price", DecimalType(7, 2), True),             # Sales price
    StructField("ws_ext_discount_amt", DecimalType(7, 2), True),        # Extended discount amount
    StructField("ws_ext_sales_price", DecimalType(7, 2), True),         # Extended sales price
    StructField("ws_ext_wholesale_cost", DecimalType(7, 2), True),      # Extended wholesale cost
    StructField("ws_ext_list_price", DecimalType(7, 2), True),          # Extended list price
    StructField("ws_ext_tax", DecimalType(7, 2), True),                  # Extended tax
    StructField("ws_coupon_amt", DecimalType(7, 2), True),               # Coupon amount
    StructField("ws_ext_ship_cost", DecimalType(7, 2), True),           # Extended shipping cost
    StructField("ws_net_paid", DecimalType(7, 2), True),                 # Net paid amount
    StructField("ws_net_paid_inc_tax", DecimalType(7, 2), True),        # Net paid amount including tax
    StructField("ws_net_paid_inc_ship", DecimalType(7, 2), True),       # Net paid amount including shipping
    StructField("ws_net_paid_inc_ship_tax", DecimalType(7, 2), True),   # Net paid amount including shipping and tax
    StructField("ws_net_profit", DecimalType(7, 2), True)                # Net profit
])



# Define the schema for catalog_sales table
catalog_sales_schema = StructType([
    StructField("cs_sold_date_sk", IntegerType(), True),                  # Sold date surrogate key
    StructField("cs_sold_time_sk", IntegerType(), True),                  # Sold time surrogate key
    StructField("cs_ship_date_sk", IntegerType(), True),                  # Ship date surrogate key
    StructField("cs_bill_customer_sk", IntegerType(), True),              # Billing customer surrogate key
    StructField("cs_bill_cdemo_sk", IntegerType(), True),                 # Billing customer demographics key
    StructField("cs_bill_hdemo_sk", IntegerType(), True),                 # Billing household demographics key
    StructField("cs_bill_addr_sk", IntegerType(), True),                  # Billing address key
    StructField("cs_ship_customer_sk", IntegerType(), True),              # Shipping customer surrogate key
    StructField("cs_ship_cdemo_sk", IntegerType(), True),                 # Shipping customer demographics key
    StructField("cs_ship_hdemo_sk", IntegerType(), True),                 # Shipping household demographics key
    StructField("cs_ship_addr_sk", IntegerType(), True),                  # Shipping address key
    StructField("cs_call_center_sk", IntegerType(), True),                # Call center surrogate key
    StructField("cs_catalog_page_sk", IntegerType(), True),               # Catalog page surrogate key
    StructField("cs_ship_mode_sk", IntegerType(), True),                  # Shipping mode surrogate key
    StructField("cs_warehouse_sk", IntegerType(), True),                  # Warehouse surrogate key
    StructField("cs_item_sk", IntegerType(), False),                      # Item surrogate key, Primary key
    StructField("cs_promo_sk", IntegerType(), True),                      # Promotion surrogate key
    StructField("cs_order_number", IntegerType(), False),                 # Order number, Primary key
    StructField("cs_quantity", IntegerType(), True),                      # Quantity sold
    StructField("cs_wholesale_cost", DecimalType(7, 2), True),           # Wholesale cost
    StructField("cs_list_price", DecimalType(7, 2), True),               # List price
    StructField("cs_sales_price", DecimalType(7, 2), True),              # Sales price
    StructField("cs_ext_discount_amt", DecimalType(7, 2), True),         # Extended discount amount
    StructField("cs_ext_sales_price", DecimalType(7, 2), True),          # Extended sales price
    StructField("cs_ext_wholesale_cost", DecimalType(7, 2), True),       # Extended wholesale cost
    StructField("cs_ext_list_price", DecimalType(7, 2), True),           # Extended list price
    StructField("cs_ext_tax", DecimalType(7, 2), True),                   # Extended tax
    StructField("cs_coupon_amt", DecimalType(7, 2), True),                # Coupon amount
    StructField("cs_ext_ship_cost", DecimalType(7, 2), True),            # Extended shipping cost
    StructField("cs_net_paid", DecimalType(7, 2), True),                  # Net paid amount
    StructField("cs_net_paid_inc_tax", DecimalType(7, 2), True),         # Net paid amount including tax
    StructField("cs_net_paid_inc_ship", DecimalType(7, 2), True),        # Net paid amount including shipping
    StructField("cs_net_paid_inc_ship_tax", DecimalType(7, 2), True),    # Net paid amount including shipping and tax
    StructField("cs_net_profit", DecimalType(7, 2), True)                 # Net profit
])



# Define schema for store_sales table
store_sales_schema = StructType([
    StructField("ss_sold_date_sk", IntegerType(), True),
    StructField("ss_sold_time_sk", IntegerType(), True),
    StructField("ss_item_sk", IntegerType(), True),
    StructField("ss_customer_sk", IntegerType(), True),
    StructField("ss_cdemo_sk", IntegerType(), True),
    StructField("ss_hdemo_sk", IntegerType(), True),
    StructField("ss_addr_sk", IntegerType(), True),
    StructField("ss_store_sk", IntegerType(), True),
    StructField("ss_promo_sk", IntegerType(), True),
    StructField("ss_ticket_number", IntegerType(), True),
    StructField("ss_quantity", IntegerType(), True),
    StructField("ss_wholesale_cost", DecimalType(7, 2), True),  # decimal(7,2)
    StructField("ss_list_price", DecimalType(7, 2), True),      # another decimal field
    StructField("ss_sales_price", DecimalType(7, 2), True),
    StructField("ss_ext_discount_amt", DecimalType(7, 2), True),
    StructField("ss_ext_sales_price", DecimalType(7, 2), True),
    StructField("ss_ext_wholesale_cost", DecimalType(7, 2), True),
    StructField("ss_ext_list_price", DecimalType(7, 2), True),
    StructField("ss_ext_tax", DecimalType(7, 2), True),
    StructField("ss_coupon_amt", DecimalType(7, 2), True),
    StructField("ss_net_paid", DecimalType(7, 2), True),
    StructField("ss_net_paid_inc_tax", DecimalType(7, 2), True),
    StructField("ss_net_profit", DecimalType(7, 2), True)
])





#.master("spark://sn1-r6515-g04-32.puretec.purestorage.com:7077") \
# Hive catalog option
spark = SparkSession.builder \
    .appName("Iceberg data gen") \
    .master("spark://sn1-r6515-g04-32.puretec.purestorage.com:7077") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-hive-runtime:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-aws-bundle:1.6.0") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.my_catalog.type", "hive") \
    .config("spark.sql.catalog.my_catalog.uri", "thrift://127.0.0.1:9083") \
    .config("spark.sql.catalog.my_catalog.jdbc.user", "hiveuser") \
    .config("spark.sql.catalog.my_catalog.jdbc.password", "hivepassword") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://iceberg-demo/warehouse") \
    .config("spark.sql.catalog.my_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.my_catalog.s3.endpoint", "http://10.21.236.205") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.defaultCatalog", "my_catalog") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/iceicedata/spark-events") \
    .config("spark.history.fs.logDirectory", "/tmp/iceicedata/spark-events") \
    .config("spark.hadoop.fs.s3a.aws.region", "us-east-1") \
    .config("spark.executor.memory", "120G") \
    .config("spark.executor.cores", "36") \
    .config("spark.driver.memory", "22G") \
    .config("spark.memory.fraction", "0.75") \
    .config("spark.memory.storageFraction", "0.4") \
    .config("spark.driver.cores", "16") \
    .enableHiveSupport() \
    .getOrCreate()
    


# def list_contents_in_directories(bucket_name, directories, s3_client):
#     for directory in directories:
#         print(f"\nListing contents of directory: {directory}")
        
#         # Use the list_objects_v2 method to list files and folders under the directory
#         result = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        
#         if 'Contents' in result:
#             for obj in result['Contents']:
#                 print(f" - {obj['Key']}")
#         else:
#             print(f"No contents found in directory: {directory}")


def create_table(table_name,schema_name,db_name,file_format):
    print(f"Creating table --> {table_name}")

    # Define the path where TPC-DS data is stored
    if 's3k' in db_name:
        data_path = "/spark/tpcds-dataset-3TB/"
        scale = 's3k'
    elif 's10k' in db_name:
        data_path = "/spark/tpcds-dataset-10TB/"
        scale = 's10k'
    df = spark.read \
    .option("delimiter", "|") \
    .schema(schema_name) \
    .csv(f"file:///{data_path}/{table_name}.dat") 

    print(f"NumPartitions --> {df.rdd.getNumPartitions()}")
    # create a temp from
    df.createOrReplaceTempView("temp_view")

    # query = f"""
    # CREATE TABLE s200_iceberg_{scale}_{file_format}_new.{table_name}  USING iceberg OPTIONS (
    # 'write.object-storage.enabled'=true, 
    # 'write.data.path'='s3://tcpdsdata-s200-iceberg-{scale}-{file_format}-new',
    # 'write.format.default'='{file_format}')  AS SELECT * FROM temp_view """
    
    query = f"""
    CREATE TABLE s200_iceberg_{scale}_{file_format}_test.{table_name}  USING iceberg OPTIONS (
    'write.format.default'='{file_format}')  AS SELECT * FROM temp_view """
    #  'write.format.default'='avro' 
    spark.sql(query)

# s200_iceberg_s3k_parquet_test
# s500_iceberg_s3k_avro_new
# s500_iceberg_s3k_orc_new
# s500_iceberg_s10k_parquet_new
# s500_iceberg_s10k_avro_new
# s500_iceberg_s10k_orc_new
# tcpdsdata-s200-iceberg-s3k-parquet-new
# tcpdsdata-s200-iceberg-s3k-avro-new
# tcpdsdata-s200-iceberg-s3k-orc-new
# tcpdsdata-s200-iceberg-s10k-parquet-new
# tcpdsdata-s200-iceberg-s10k-avro-new
# tcpdsdata-s200-iceberg-s10k-orc-new




current_time = datetime.now().strftime("%Y_%B_%d_%H%M")
output_file_path = f"results/Iceberg-datagen-{nodes}-{flashblade_model}-scale_{scale}-{table_format}-{file_format}-{current_time}.log"
with open(output_file_path, 'w') as log_file:
    log_file.write("Iceberg Data Gen Execution Times Log\n")
    log_file.write("-" * 30 + "\n")
    start_time = time.time()

    try:

        # iceberg block
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 3000 and file_format == 'parquet':
            print(f"Using databse -> s200_s3k_iceberg_parquet")
            db_name = "s200-s3k-iceberg-parquet"
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 10000 and file_format == 'parquet':
            print(f"Using databse -> s200_s10k_iceberg_parquet")
            db_name = "s200-s10k-iceberg-parquet"
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 3000 and file_format == 'avro':
            print(f"Using databse -> s200_s3k_iceberg_avro")
            db_name = "s200-s3k-iceberg-avro"
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 10000 and file_format == 'avro':
            print(f"Using databse -> s200_s10k_iceberg_avro")
            db_name = "s200-s10k-iceberg-avro"
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 3000 and file_format == 'orc':
            print(f"Using databse -> s200_s3k_iceberg_orc")
            db_name = "s200-s3k-iceberg-orc"
        if table_format == 'iceberg' and flashblade_model == 'S200' and scale == 10000 and file_format == 'orc':
            print(f"Using databse -> s200_s10k_iceberg_orc")
            db_name = "s200-s10k-iceberg-orc"           


        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 3000 and file_format == 'parquet':
            print(f"Using databse -> s500_s3k_iceberg_parquet")
            db_name = "s500-s3k-iceberg-parquet"
        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 10000 and file_format == 'parquet':
            print(f"Using databse -> s500_s10k_iceberg_parquet")
            db_name = "s500-s10k-iceberg-parquet"
        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 3000 and file_format == 'avro':
            print(f"Using databse -> s500_s3k_iceberg_avro")
            db_name = "s500-s3k-iceberg-avro"
        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 10000 and file_format == 'avro':
            print(f"Using databse -> s500-s10k-iceberg-avro")
            db_name = "s500-s10k-iceberg-avro"
        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 3000 and file_format == 'orc':
            print(f"Using databse -> s500-s3k-iceberg-orc")
            db_name = "s500-s3k-iceberg-orc"
        if table_format == 'iceberg' and flashblade_model == 'S500' and scale == 10000 and file_format == 'orc':
            print(f"Using databse -> s500-s10k-iceberg-orc")
            db_name = "s500-s10k-iceberg-orc"            

        # convert the CSV to delta and create table - dbgen_version
        table_name = 'dbgen_version'
        create_table(table_name, dbgen_version_schema,db_name,file_format)


        # convert the CSV to delta and create table - customer_address
        table_name = 'customer_address'
        create_table(table_name, customer_address_schema,db_name,file_format)


        # convert the CSV to delta and create table - customer_demographics
        table_name = 'customer_demographics'
        create_table(table_name, customer_demographics_schema,db_name,file_format)


        # convert the CSV to delta and create table - date_dim
        table_name = 'date_dim'
        create_table(table_name, date_dim_schema,db_name,file_format)


        # convert the CSV to delta and create table - warehouse
        table_name = 'warehouse'
        create_table(table_name, warehouse_schema,db_name,file_format)


        # convert the CSV to delta and create table - ship_mode
        table_name = 'ship_mode'
        create_table(table_name, ship_mode_schema,db_name,file_format)


        # convert the CSV to delta and create table - time_dim
        table_name = 'time_dim'
        create_table(table_name, time_dim_schema,db_name,file_format)


        # convert the CSV to delta and create table - reason
        table_name = 'reason'
        create_table(table_name, reason_schema,db_name,file_format)


        # convert the CSV to delta and create table - income_band
        table_name = 'income_band'
        create_table(table_name, income_band_schema,db_name,file_format)


        # convert the CSV to delta and create table - item
        table_name = 'item'
        create_table(table_name, item_schema,db_name,file_format)


        # convert the CSV to delta and create table - store
        table_name = 'store'
        create_table(table_name, store_schema,db_name,file_format)


        # convert the CSV to delta and create table - call_center
        table_name = 'call_center'
        create_table(table_name, call_center_schema,db_name,file_format)


        # convert the CSV to delta and create table - customer
        table_name = 'customer'
        create_table(table_name, customer_schema,db_name,file_format)


        # convert the CSV to delta and create table - web_site
        table_name = 'web_site'
        create_table(table_name, web_site_schema,db_name,file_format)


        # convert the CSV to delta and create table - store_returns
        table_name = 'store_returns'
        create_table(table_name, store_returns_schema,db_name,file_format)


        # convert the CSV to delta and create table - household_demographics
        table_name = 'household_demographics'
        create_table(table_name, household_demographics_schema,db_name,file_format)


        # convert the CSV to delta and create table - web_page
        table_name = 'web_page'
        create_table(table_name, web_page_schema,db_name,file_format)


        # convert the CSV to delta and create table - promotion
        table_name = 'promotion'
        create_table(table_name, promotion_schema,db_name,file_format)



            # convert the CSV to delta and create table - catalog_page
        table_name = 'catalog_page'
        create_table(table_name, catalog_page_schema,db_name,file_format)


        # convert the CSV to delta and create table - inventory
        table_name = 'inventory'
        create_table(table_name, inventory_schema,db_name,file_format)


        # convert the CSV to delta and create table - catalog_returns
        table_name = 'catalog_returns'
        create_table(table_name, catalog_returns_schema,db_name,file_format)


        # convert the CSV to delta and create table - web_returns
        table_name = 'web_returns'
        create_table(table_name, web_returns_schema,db_name,file_format)


        # convert the CSV to delta and create table - web_sales
        table_name = 'web_sales'
        create_table(table_name, web_sales_schema,db_name,file_format)


        # convert the CSV to delta and create table - catalog_sales
        table_name = 'catalog_sales'
        create_table(table_name, catalog_sales_schema,db_name,file_format)


        # convert the CSV to delta and create table - store_sales
        table_name = 'store_sales'
        create_table(table_name, store_sales_schema,db_name,file_format)


        runtime = time.time() - start_time
        log_file.write(f"DataGen  completed in {runtime:.2f} seconds.\n")
    except Exception as e:
        log_file.write(f"DataGen failed with error: {e}\n")
