import os

from pydeequ.checks import Check, CheckLevel
from pydeequ.suggestions import (
    CompleteIfCompleteRule,
    ConstraintSuggestionRunner,
    NonNegativeNumbersRule,
    RetainCompletenessRule,
    RetainTypeRule,
    UniqueIfApproximatelyUniqueRule,
)
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class QualityTests:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run_test(self, data_path: str | os.PathLike) -> str:
        if data_path.split(".")[1] == "parquet":
            df = self.spark.read.format("parquet").load(f"{data_path}")

        if data_path.split(".")[1] == "csv":
            df = (
                self.spark.read.format("csv")
                .option("header", "true")
                .load(f"{data_path}")
            )

        # get suggestions
        suggestionResult = (
            ConstraintSuggestionRunner(spark_session=self.spark)
            .onData(df=df)
            .addConstraintRule(CompleteIfCompleteRule())
            .addConstraintRule(NonNegativeNumbersRule())
            .addConstraintRule(RetainCompletenessRule())
            .addConstraintRule(RetainTypeRule())
            .addConstraintRule(UniqueIfApproximatelyUniqueRule())
            .run()
        )

        # extract suggested code for the constraints
        suggestedValidationSuite = ""
        for suggestion in suggestionResult["constraint_suggestions"]:
            suggestedValidationSuite = (
                suggestedValidationSuite + suggestion["code_for_constraint"]
            )

        suggestedValidationSuite = "check" + suggestedValidationSuite
        print(suggestedValidationSuite)
        check = Check(
            spark_session=self.spark, level=CheckLevel.Error, description="DQ Tests"
        )

        # apply suggestions
        checkConstraints = (
            VerificationSuite(spark_session=self.spark)
            .onData(df=df)
            .addCheck(eval(suggestedValidationSuite))
            .run()
        )

        # convert to df
        checkedConstraintsDf = VerificationResult.checkResultsAsDataFrame(
            spark_session=self.spark, verificationResult=checkConstraints
        )
        # filter out failed ones
        failedTests = checkedConstraintsDf.filter(col("constraint_status") == "Failure")
        if failedTests.count() > 0:
            failedTests.show()
