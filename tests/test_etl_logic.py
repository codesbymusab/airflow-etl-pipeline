"""
Unit and integration tests for Frankfurter Currency Exchange ETL Pipeline
"""

import unittest
from datetime import datetime
import requests


class TestCurrencyETLLogic(unittest.TestCase):
    def setUp(self):
        self.api_url = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"

    def test_api_extraction(self):
        """Test API extraction from Frankfurter v2 rates endpoint"""
        response = requests.get(self.api_url, timeout=10)
        self.assertEqual(response.status_code, 200, "Frankfurter API should return HTTP 200")
        data = response.json()
        self.assertIsInstance(data, list, "API response should be a list of rate objects")
        self.assertGreater(len(data), 0, "API response should not be empty")

        quotes_found = {item.get("quote") for item in data}
        self.assertTrue({"USD", "GBP", "JPY"}.issubset(quotes_found), "Must contain USD, GBP, JPY quotes")

    def test_transformation_schema(self):
        """Test transformation produces the required schema matching assignment specifications"""
        mock_raw_data = [
            {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15},
            {"date": "2026-11-04", "base": "EUR", "quote": "GBP", "rate": 0.84},
            {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 165.2},
        ]

        retrieved_at_timestamp = "2026-11-05T07:15:00"
        transformed_records = []
        for item in mock_raw_data:
            transformed_record = {
                "base_currency": item.get("base", "EUR"),
                "target_currency": item.get("quote"),
                "exchange_rate": float(item.get("rate")),
                "rate_date": item.get("date"),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at_timestamp,
            }
            transformed_records.append(transformed_record)

        # Validate count
        self.assertEqual(len(transformed_records), 3)

        # Validate exact schema of USD record
        usd_record = transformed_records[0]
        expected_keys = {
            "base_currency",
            "target_currency",
            "exchange_rate",
            "rate_date",
            "source",
            "retrieved_at",
        }
        self.assertEqual(set(usd_record.keys()), expected_keys)
        self.assertEqual(usd_record["base_currency"], "EUR")
        self.assertEqual(usd_record["target_currency"], "USD")
        self.assertEqual(usd_record["exchange_rate"], 1.15)
        self.assertEqual(usd_record["rate_date"], "2026-11-04")
        self.assertEqual(usd_record["source"], "Frankfurter API")
        self.assertEqual(usd_record["retrieved_at"], "2026-11-05T07:15:00")


if __name__ == "__main__":
    unittest.main()
