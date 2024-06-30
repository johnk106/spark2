from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName('RealEstateETL') \
    .config("spark.jars.packages", "com.amazonaws:aws-java-sdk:1.12.238,com.amazonaws:aws-java-sdk-s3:1.12.238,org.apache.hadoop:hadoop-aws:3.3.1,io.github.spark-redshift-community:spark-redshift_2.12:3.0.0-preview1") \
    .getOrCreate()

# Define S3 input path and Redshift connection details
s3_input_path = "s3a://your-input-bucket/realestate/"
redshift_jdbc_url = "jdbc:redshift://your-redshift-cluster-url:5439/your-database"
redshift_user = "your-username"
redshift_password = "your-password"

# Function to load CSV files from S3
def load_csv(file_path):
    return spark.read.option("header", "true").csv(file_path)

# Load data from CSV files
tenants_df = load_csv(f"{s3_input_path}Tenants.csv")
identificationtypes_df = load_csv(f"{s3_input_path}IdentificationTypes.csv")
apartments_df = load_csv(f"{s3_input_path}Apartments.csv")
exits_df = load_csv(f"{s3_input_path}Exits.csv")
invoices_df = load_csv(f"{s3_input_path}Invoices.csv")
receipts_df = load_csv(f"{s3_input_path}Receipts.csv")
utilitybills_df = load_csv(f"{s3_input_path}UtilityBills.csv")
invoice_items_df = load_csv(f"{s3_input_path}InvoiceItems.csv")
apartment_types_df = load_csv(f"{s3_input_path}ApartmentTypes.csv")
buildings_df = load_csv(f"{s3_input_path}Buildings.csv")
mpesa_statements_df = load_csv(f"{s3_input_path}MpesaStatements.csv")
tenancy_documents_df = load_csv(f"{s3_input_path}TenancyDocuments.csv")

# Function to write DataFrame to Redshift
def write_to_redshift(df, table_name):
    df.write \
      .format("io.github.spark_redshift_community.spark.redshift") \
      .option("url", redshift_jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", redshift_user) \
      .option("password", redshift_password) \
      .option("tempdir", "s3a://your-temp-bucket/temp/") \
      .mode("overwrite") \
      .save()

# Transform and select columns for dimension tables
dim_tenants = tenants_df.join(identificationtypes_df, tenants_df.IdentificationTypeId == identificationtypes_df.Id, "left") \
    .select(
        col("Id").alias("tenant_id"),
        col("Firstname"),
        col("Othername"),
        col("Gender"),
        col("Phonenumber"),
        col("Emailaddress"),
        col("Identificationtypeid"),
        col("Identificationnumber"),
        col("Employer"),
        col("Jobdescription"),
        col("Nextofkinname"),
        col("Nextofkinphone"),
        col("Nextofkinemail"),
        col("Apartmentid"),
        to_date(col("Tenancyfrom")).alias("tenancy_from"),
        to_date(col("Tenancyto")).alias("tenancy_to"),
        col("Status"),
        col("tenancyNo")
    )

dim_apartments = apartments_df.join(buildings_df, apartments_df.Buildingid == buildings_df.Id, "left") \
    .join(apartment_types_df, apartments_df.Apartmenttype == apartment_types_df.Id, "left") \
    .select(
        col("Id").alias("apartment_id"),
        col("Buildingid").alias("building_id"),
        col("Floor"),
        col("apartmentNumber"),
        col("Apartmenttype").alias("apartmenttype_id"),
        col("Monthlyrent"),
        col("Deposit"),
        col("Electricitymeterno"),
        col("Watermeterno")
    )

dim_buildings = buildings_df.select(
    col("Id").alias("building_id"),
    col("Buildingname"),
    col("Buildingnumber"),
    col("Physicaladdress"),
    col("Paybillnumber")
)

dim_apartmentTypes = apartment_types_df.select(
    col("Id").alias("apartmenttype_id"),
    col("Description")
)

dim_identificationTypes = identificationtypes_df.select(
    col("Id").alias("identification_type_id"),
    col("Description")
)

dim_mpesaStatements = mpesa_statements_df.select(
    col("MpesaId"),
    col("TransactionDate"),
    col("amount"),
    col("transactionType"),
    col("tenantid")
)

dim_tenancydocuments = tenancy_documents_df.select(
    col("Id").alias("document_id"),
    col("tenantId"),
    col("documentData"),
    col("Filename"),
    col("Filetype")
)

# Transform and select columns for fact tables
fact_invoices = invoices_df.join(receipts_df, invoices_df.Id == receipts_df.InvoiceId, "left_outer") \
    .select(
        col("Id").alias("invoice_id"),
        col("Tenencyid"),
        col("Apartmentid"),
        col("Invoicedate"),
        col("Status"),
        col("dueDate"),
        col("Arrearamount"),
        col("Recieptamount").alias("PaidAmount")
    )

fact_receipts = receipts_df.select(
    col("Id").alias("receipt_id"),
    col("InvoiceId"),
    col("Transactiondate"),
    col("Paymentmode"),
    col("Recieptamount")
)

fact_utilityBills = utilitybills_df.select(
    col("Id").alias("utility_bill_id"),
    col("Tenantid"),
    col("Water"),
    col("Electricity"),
    col("Garbage"),
    col("Others")
)

fact_exits = exits_df.select(
    col("Id").alias("exit_id"),
    col("Tenancyid"),
    col("Exitdate"),
    col("Effectivedate"),
    col("Depositpayable"),
    col("Status"),
    col("Charges"),
    col("Datepaid"),
    col("Netrefundable")
)

# Write dimension tables to Redshift
write_to_redshift(dim_tenants, "dim_tenants")
write_to_redshift(dim_apartments, "dim_apartments")
write_to_redshift(dim_buildings, "dim_buildings")
write_to_redshift(dim_apartmentTypes, "dim_apartmenttypes")
write_to_redshift(dim_identificationTypes, "dim_identificationtypes")
write_to_redshift(dim_mpesaStatements, "dim_mpesastatements")
write_to_redshift(dim_tenancydocuments, "dim_tenancydocuments")

# Write fact tables to Redshift
write_to_redshift(fact_invoices, "fact_invoices")
write_to_redshift(fact_receipts, "fact_receipts")
write_to_redshift(fact_utilityBills, "fact_utilitybills")
write_to_redshift(fact_exits, "fact_exits")

# Stop Spark session
spark.stop()
