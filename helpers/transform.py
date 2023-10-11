import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class Finalize:
    def __init__(
        self,
        spark: SparkSession,
        save_prescriber_drug_path: str | os.PathLike,
        save_prescribers_path: str | os.PathLike,
        save_geo_drug_path: str | os.PathLike,
    ) -> None:
        self.save_prescriber_drug_path = save_prescriber_drug_path
        self.save_prescribers_path = save_prescribers_path
        self.save_geo_drug_path = save_geo_drug_path
        self.spark = spark

    def create_final_dataset(self):
        drug = self.spark.read.parquet(self.save_geo_drug_path)
        presc_drug = self.spark.read.parquet(self.save_prescriber_drug_path)
        presc = self.spark.read.parquet(self.save_prescribers_path)

        drug = drug.alias("drug_d")
        presc = presc.alias("presc_d")
        presc_drug = presc_drug.alias("presc_drug_d")
        
        
        
        presc_final = (presc_drug.join(presc, presc_drug["presc_id"] == presc["presc_id"])
                         .select("presc_drug_d.*", "presc_d.presc_type_desc")
                )
        
        grouped = presc_drug.groupBy("drug_brand_name", "drug").agg(
                F.sum("total_claims").alias("total_claims"),
                F.sum("total_drug_cost").alias("total_cost"))

        grouped = grouped.alias("g")
        
        drug_final = (grouped.join(drug, 
                (presc_drug["drug_brand_name"] == drug["drug_brand_name"])
                & (presc_drug["drug"] == drug["drug"]))
                .select("g.*", "drug_d.is_antibiotic_check"))
        

        presc_final.show()
        drug_final.show()