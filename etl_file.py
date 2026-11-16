
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, when
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType


# Main ETL class
class TransactionETL:

    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

        self.spark = (
            SparkSession.builder
            .appName("Transactions_ETL")
            .getOrCreate()
        )

    # 1. EXTRACT: Read CSV data from HDFS
    def extract(self):

        schema = StructType([
            StructField("transaction_id", LongType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("payment_type", StringType(), True)
        ])

        df = (
            self.spark.read
            .option("header", "false")
            .schema(schema)
            .csv(self.input_path)
        )

        print("Extracted records:", df.count())

        return df

    # 2. TRANSFORM: Clean and transform the data
    def transform(self, df):

        # Remove unwanted spaces from text columns
        text_columns = [
            "category", "sub_category",
            "city", "state", "payment_type"
        ]

        for column_name in text_columns:
            df = df.withColumn(
                column_name,
                trim(col(column_name))
            )

        # Convert date string into a date
        df = df.withColumn(
            "transaction_date",
            to_date(col("transaction_date"), "MM-dd-yyyy")
        )

        # Replace missing category with UNKNOWN
        df = df.withColumn(
            "category",
            when(
                col("category").isNull() | (col("category") == ""),
                "UNKNOWN"
            ).otherwise(col("category"))
        )

        # Remove invalid records and negative amounts
        df = df.filter(
            col("transaction_id").isNotNull()
            & col("product_id").isNotNull()
            & col("transaction_date").isNotNull()
            & col("amount").isNotNull()
            & (col("amount") >= 0)
        )

        # Remove duplicate transaction IDs
        df = df.dropDuplicates(["transaction_id"])

        print("Cleaned records:", df.count())

        return df

    # 3. LOAD: Write the cleaned data to HDFS
    def load(self, df):

        (
            df.write
            .mode("overwrite")
            .parquet(self.output_path)
        )

        print("Data loaded successfully!")
        print("Output path:", self.output_path)

    # MAIN: Execute ETL steps in sequence
    def run(self):

        try:
            df = self.extract()

            cleaned_df = self.transform(df)

            self.load(cleaned_df)

            print("ETL job completed successfully!")

        finally:
            self.spark.stop()


# Program entry point
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(
            "Usage: spark-submit wc_parameterized.py "
            "<input_path> <output_path>"
        )
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    etl = TransactionETL(input_path, output_path)

    etl.run()
