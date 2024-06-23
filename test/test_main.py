import unittest
from pyspark.sql import SparkSession
from main import organize_df

class OrganizeDFTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("OrganizeDFTests") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_organize_df(self):
        data = [
            ('{"timestamp": "2021-01-01T00:00:29", "event_id": "6d7d0c85-94a2-4bf0-a570-72774e4fbc0d", "domain": "transaction", "event_type": "creation", "data": {"id": 186295}}{"timestamp": "2021-01-01T00:01:01", "event_id": "bfa28493-b97c-4ee6-9612-f6422d185229", "domain": "transaction", "event_type": "creation", "data": {"id": 384341}}',),
        ]
        schema = ["value"]
        df = self.spark.createDataFrame(data, schema)

        result_df = organize_df(self.spark, df)

        expected_data = [
            (186295, "2021-01-01", "2021-01-01T00:00:29", "transaction", "creation"),
            (384341, "2021-01-01", "2021-01-01T00:01:01", "transaction", "creation"),
        ]
        expected_schema = ["id", "date_time", "timestamp", "domain", "event_type"]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()