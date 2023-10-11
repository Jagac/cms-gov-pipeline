import os

os.environ["SPARK_VERSION"] = "3.3"  # needed to work with spark 3

import pydeequ
from pyspark.sql import SparkSession

from helpers.preprocess import Preprocess
from helpers.tests import QualityTests
from helpers.transform import Finalize


def run_pipeline():
    spark = (
        SparkSession.builder.config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    test = QualityTests(spark=spark)

    prescriber_drug_pth = "/home/jagac/perscriber_drug.csv"
    prescriber_pth = "/home/jagac/providers.csv"
    geo_drug_pth = "/home/jagac/geo_drug.csv"

    # Tests below cause
    # File "<string>", line 1, in <module>
    # NameError: name 'ConstrainableDataTypes' is not defined
    # test.run_test(prescriber_drug_pth)
    # test.run_test(prescriber_pth)
    # test.run_test(geo_drug_pth)

    save_prescriber_drug_pth = (
        "/home/jagac/spark-pipeline/preprocessed/perscriber_drug.parquet"
    )
    save_prescriber_pth = "/home/jagac/spark-pipeline/preprocessed/providers.parquet"
    save_geo_drug_pth = "/home/jagac/spark-pipeline/preprocessed/geo_drug.parquet"

    preprocess_data = Preprocess(
        spark=spark,
        prescriber_drug_path=prescriber_drug_pth,
        prescribers_path=prescriber_pth,
        geo_drug_path=geo_drug_pth,
    )

    preprocess_data.prepare_prescriber_drug(
        save_prescriber_drug_pth=save_prescriber_drug_pth
    )
    preprocess_data.prepare_prescriber(save_prescriber_pth=save_prescriber_pth)
    preprocess_data.prepare_geo_drug(save_geo_drug_pth=save_geo_drug_pth)

    test.run_test(save_prescriber_drug_pth)
    test.run_test(save_prescriber_pth)
    test.run_test(save_geo_drug_pth)

    final_transformation = Finalize(
        spark=spark,
        save_prescriber_drug_path=save_prescriber_drug_pth,
        save_prescribers_path=save_prescriber_pth,
        save_geo_drug_path=save_geo_drug_pth,
    )
    final_transformation.create_final_dataset()

    final_loc_1 = "/home/jagac/spark-pipeline/final/presc.parquet"
    final_loc_2 = "/home/jagac/spark-pipeline/final/drug.parquet"

    test.run_test(final_loc_1)
    test.run_test(final_loc_2)

    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
