from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

# Example directory where Spark should write output
output_path = "../output/"

# Check directory permissions
if not os.access(output_path, os.W_OK):
    raise PermissionError(f"Output directory '{output_path}' is not writable.")

# Initialize Spark session with additional configurations
spark = SparkSession.builder \
    .appName('RealEstateETL') \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()

# Function to load CSV files
def load_csv(file_path):
    return spark.read.option("header", "true").csv(file_path)

# Load CSV files into DataFrames
tenants_df = load_csv("../input/2024-06-28_23-47_Tenants.csv")
identificationtypes_df = load_csv("../input/2024-06-28_23-47_IdentificationTypes.csv")
apartments_df = load_csv("../input/2024-06-28_23-47_Apartments.csv")
buildings_df = load_csv("../input/2024-06-28_23-47_Buildings.csv")
apartment_types_df = load_csv("../input/2024-06-28_23-47_ApartmentTypes.csv")
exits_df = load_csv("../input/2024-06-28_23-47_Exits.csv")
invoices_df = load_csv("../input/2024-06-28_23-47_Invoices.csv")
invoice_items_df = load_csv("../input/2024-06-28_23-47_InvoiceItems.csv")
receipts_df = load_csv("../input/2024-06-28_23-47_Receipts.csv")
tenancy_documents_df = load_csv("../input/2024-06-28_23-47_TenancyDocuments.csv")
utilitybills_df = load_csv("../input/2024-06-28_23-47_UtilityBills.csv")

# Rename columns to avoid ambiguity
tenants_df = tenants_df.withColumnRenamed("Id", "tenant_Id")
identificationtypes_df = identificationtypes_df.withColumnRenamed("Id", "identificationType_Id")
apartments_df = apartments_df.withColumnRenamed("Id", "apartment_Id")
buildings_df = buildings_df.withColumnRenamed("Id", "building_Id")
apartment_types_df = apartment_types_df.withColumnRenamed("Id", "apartmentType_Id")
exits_df = exits_df.withColumnRenamed("Id", "exit_Id")
invoices_df = invoices_df.withColumnRenamed("Id", "invoice_Id")
receipts_df = receipts_df.withColumnRenamed("Id", "receipt_Id")
tenancy_documents_df = tenancy_documents_df.withColumnRenamed("Id", "tenancyDocument_Id")

# Define function to write DataFrames to Redshift
def write_to_redshift(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://estate-cluster.cch6uxtp5ivv.us-east-1.redshift.amazonaws.com:5439/estate-db") \
        .option("dbtable", table_name) \
        .option("user", "johnk") \
        .option("password", "Optimus99.") \
        .option("tempdir", "s3://cprocessed-estate-data/temp/") \
        .option("aws_iam_role", "arn:aws:iam::533267245238:user/Johnk0") \
        .option("forward_spark_s3_credentials", "true") \
        .option("driver", "com.amazon.redshift.jdbc42.Driver") \
        .option("truncate", "true") \
        .mode("append") \
        .save()

# Transform and select columns for dimension tables
dim_tenants = tenants_df.join(identificationtypes_df, tenants_df.IdentificationTypeId == identificationtypes_df.identificationType_Id, "left") \
    .select(
        col("tenant_Id").alias("id"),  # Change alias to match Redshift schema
        col("FirstName").alias("first_name"),
        col("OtherNames").alias("other_name"),  # Adjust column name here if necessary
        col("Gender").alias("gender"),
        col("PhoneNumber").alias("phone_number"),
        col("EmailAddress").alias("email_address"),
        col("IdentificationTypeId").alias("identification_type_id"),
        col("IdentificationNumber").alias("identification_number"),
        col("Employer").alias("employer"),
        col("JobDescription").alias("job_description"),
        col("NextOfKinName").alias("next_of_kin_name"),
        col("NextOfKinPhone").alias("next_of_kin_phone"),
        col("NextOfKinEmail").alias("next_of_kin_email"),
        col("ApartmentId").alias("apartment_id"),
        to_date(col("TenancyFrom")).alias("tenancy_from"),
        to_date(col("TenancyTo")).alias("tenancy_to"),
        col("Status").alias("status"),
        col("TenancyNo").alias("tenancy_no")
    )

dim_apartments = apartments_df.join(buildings_df, apartments_df.BuildingId == buildings_df.building_Id, "left") \
    .join(apartment_types_df, apartments_df.ApartmentTypeId == apartment_types_df.apartmentType_Id, "left") \
    .select(
        col("apartment_Id").alias("id"),  # Change alias to match Redshift schema
        col("building_Id").alias("building_id"),
        col("Floor").alias("floor"),
        col("ApartmentNumber").alias("apartment_number"),
        col("apartmentType_Id").alias("apartment_type_id"),
        col("MonthlyRent").alias("monthly_rent"),
        col("Deposit").alias("deposit"),
        col("ElectricityMeterNo").alias("electricity_meter_no"),
        col("WaterMeterNo").alias("water_meter_no")
    )

dim_buildings = buildings_df.select(
    col("building_Id").alias("id"),  # Change alias to match Redshift schema
    col("BuildingName").alias("building_name"),
    col("BuildingNumber").alias("building_number"),
    col("PhysicalAddress").alias("physical_address"),
    col("PayBillNumber").alias("paybill_number")
)

dim_apartmentTypes = apartment_types_df.select(
    col("apartmentType_Id").alias("id"),  # Change alias to match Redshift schema
    col("Description").alias("description")
)

dim_identificationTypes = identificationtypes_df.select(
    col("identificationType_Id").alias("id"),  # Change alias to match Redshift schema
    col("Description").alias("description")
)

dim_tenancydocuments = tenancy_documents_df.select(
    col("tenancyDocument_Id").alias("id"),  # Change alias to match Redshift schema
    col("tenantId").alias("tenant_id"),
    col("documentData").alias("document_data"),
    col("Filename").alias("filename"),
    col("FileType").alias("filetype")
)

# Transform and select columns for fact tables
fact_exits = exits_df.select(
    col("exit_Id").alias("id"),  # Change alias to match Redshift schema
    col("ExitNo").alias("exit_no"),
    col("ExitDate").alias("exit_date"),
    col("TenancyId").alias("tenancy_id"),
    col("EffectiveDate").alias("effective_date"),
    col("DepositPayable").alias("deposit_payable"),
    col("Status").alias("status"),
    col("Charges").alias("charges"),
    col("DatePaid").alias("date_paid"),
    col("NetRefundable").alias("net_refundable")
)

fact_invoices = invoices_df.select(
    col("invoice_Id").alias("id"),  # Change alias to match Redshift schema
    col("InvoiceNo").alias("invoice_no"),
    col("InvoiceDate").alias("invoice_date"),
    col("TenancyId").alias("tenancy_id"),
    col("CancelledBy").alias("cancelled_by"),
    col("CreatedBy").alias("created_by"),
    col("Status").alias("status"),
    col("DueDate").alias("due_date"),
    col("ApartmentNo").alias("apartment_no"),
    col("BuildingNo").alias("building_no"),
    col("IdNumber").alias("id_number"),
    col("ArrearAmount").alias("arrear_amount")
)

fact_receipts = receipts_df.select(
    col("receipt_Id").alias("id"),  # Change alias to match Redshift schema
    col("ReceiptNo").alias("receipt_no"),
    col("TransactionDate").alias("transaction_date"),
    col("TenancyId").alias("tenancy_id"),
    col("ReferenceNo").alias("reference_no"),
    col("PaymentMode").alias("payment_mode"),
    col("ReceiptAmount").alias("receipt_amount"),
    col("CapturedBy").alias("captured_by"),
    col("Narration").alias("narration"),
    col("Status").alias("status"),
    col("IdNumber").alias("id_number"),
    col("PayBillNo").alias("paybill_no")
)

fact_utilitybills = utilitybills_df.select(
    col("Id").alias("id"),  # Change alias to match Redshift schema
    col("TenantId").alias("tenant_id"),
    col("Water").alias("water"),
    col("Electricity").alias("electricity"),
    col("Garbage").alias("garbage"),
    col("Others").alias("others"),
    col("BuildingName").alias("building_name"),
    col("ApartmentNo").alias("apartment_no"),
    col("IdNumber").alias("id_number"),
    col("Status").alias("status")
)

fact_invoiceItems = invoice_items_df.select(
    col("Id").alias("id"),  # Change alias to match Redshift schema
    col("InvoiceId").alias("invoice_id"),
    col("Item").alias("item"),
    col("Amount").alias("amount"),
    col("Narration").alias("narration")
)

# Write dimension and fact tables to Redshift
write_to_redshift(dim_tenants, "dim_tenants")
write_to_redshift(dim_apartments, "dim_apartments")
write_to_redshift(dim_buildings, "dim_buildings")
write_to_redshift(dim_apartmentTypes, "dim_apartmentTypes")
write_to_redshift(dim_identificationTypes, "dim_identificationTypes")
# write_to_redshift(dim_tenancydocuments, "dim_tenancydocuments")
write_to_redshift(fact_exits, "fact_exits")
write_to_redshift(fact_invoices, "fact_invoices")
write_to_redshift(fact_receipts, "fact_receipts")
write_to_redshift(fact_utilitybills, "fact_utilitybills")
write_to_redshift(fact_invoiceItems, "fact_invoiceItems")

spark.stop()
