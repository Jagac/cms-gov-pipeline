import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class Preprocess:
    def __init__(
        self,
        spark: SparkSession,
        prescriber_drug_path: str | os.PathLike,
        prescribers_path: str | os.PathLike,
        geo_drug_path: str | os.PathLike,
    ) -> None:
        """Basic preprocessing class

        Args:
            spark (SparkSession): spark session
            prescriber_drug_path (str | os.PathLike): raw path
            prescribers_path (str | os.PathLike): raw path
            geo_drug_path (str | os.PathLike): raw path
        """

        self.prescriber_drug_path = prescriber_drug_path
        self.prescribers_path = prescribers_path
        self.geo_drug_path = geo_drug_path
        self.spark = spark

    @staticmethod
    def lower_headers(df: DataFrame) -> DataFrame:
        """Makes all the headers lowercase

        Args:
            df (DataFrame): pyspark dataframe

        Returns:
            DataFrame: pyspark dataframe with all headers lowered
        """
        df = df.select([F.col(x).alias(x.lower()) for x in df.columns])
        return df

    def prepare_prescriber_drug(
        self, save_prescriber_drug_pth: str | os.PathLike
    ) -> None:
        """Only selects needed columns, renames them, casts, and creates a new col
        presc_fullname based on concatination of prscrbr_first_name and prscrbr_last_org_name

        Args:
            save_prescriber_drug_pth (str | os.PathLike): path to save parquet file
        """
        presc_drug_df = self.spark.read.csv(self.prescriber_drug_path, header=True)
        presc_drug_df = self.lower_headers(presc_drug_df)

        presc_drug_df = (
            presc_drug_df.select(
                F.col("prscrbr_npi").alias("presc_id").cast("integer"),
                F.col("prscrbr_last_org_name").alias("presc_lname"),
                F.col("prscrbr_first_name").alias("presc_fname"),
                F.col("prscrbr_state_abrvtn").alias("presc_state_code"),
                F.col("prscrbr_type").alias("presc_specialty"),
                F.col("brnd_name").alias("drug_brand_name"),
                F.col("gnrc_name").alias("drug"),
                F.col("tot_clms").alias("total_claims").cast("float"),
                F.col("tot_drug_cst").alias("total_drug_cost").cast("float"),
            )
            .withColumn(
                "presc_fullname",
                F.concat_ws("_", F.col("presc_fname"), F.col("presc_lname")),
            )
            .drop("presc_lname", "presc_fname")
        )

        presc_drug_df.write.format("parquet").mode("overwrite").save(
            f"{save_prescriber_drug_pth}"
        )

    def prepare_prescriber(self, save_prescriber_pth: str | os.PathLike) -> None:
        """Only selects needed columns, renames them, casts, and creates a new col
        presc_type_desc based on wether prscrbr_ent_cd = I (individual) or not

        Args:
            save_prescriber_drug_pth (str | os.PathLike): path to save parquet file
        """

        prescriber_df = self.spark.read.csv(self.prescribers_path, header=True)
        prescriber_df = self.lower_headers(prescriber_df)
        prescriber_df = (
            prescriber_df.select(
                F.col("prscrbr_npi").alias("presc_id").cast("integer"),
                F.col("prscrbr_ent_cd").alias("presc_type"),
                F.col("prscrbr_city").alias("presc_city"),
                F.col("prscrbr_state_abrvtn").alias("prscrbr_state_code"),
            )
            .withColumn(
                "presc_type_desc",
                F.when(F.col("presc_type") == "I", "Individual").otherwise(
                    "Corporation"
                ),
            )
            .drop(F.col("presc_type"))
        )

        prescriber_df.write.format("parquet").mode("overwrite").save(
            f"{save_prescriber_pth}"
        )

    def prepare_geo_drug(self, save_geo_drug_pth: str | os.PathLike) -> None:
        """Only selects needed columns, renames them, casts, and creates a new col
        is_antibiotic_check based on wether antbtc_drug_flag = Y (Antibiotic) or not

        Args:
            save_prescriber_drug_pth (str | os.PathLike): path to save parquet file
        """
        drug_df = self.spark.read.csv(self.geo_drug_path, header=True)
        drug_df = self.lower_headers(drug_df)
        drug_df = (
            drug_df.select(
                F.col("brnd_name").alias("drug_brand_name"),
                F.col("gnrc_name").alias("drug"),
                F.col("antbtc_drug_flag").alias("is_antibiotic"),
            ).withColumn(
                "is_antibiotic_check",
                F.when(F.col("is_antibiotic") == "Y", "Antibiotic").otherwise(
                    "Not Antibiotic"
                ),
            )
        ).drop(F.col("is_antibiotic"))

        drug_df.write.format("parquet").mode("overwrite").save(f"{save_geo_drug_pth}")
