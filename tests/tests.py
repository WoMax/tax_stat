import os
import sqlite3
import sys
import unittest
import unittest.mock as mock

import tax_stat

import pandas as pd
import pandas.testing as pd_testing
import pandera


class TestBase(unittest.TestCase):
    fixture_file_name = None
    source_file_name = None

    @property
    def fixture_file_path(self):
        return self.get_file_path("fixtures", self.fixture_file_name)

    @property
    def source_file_path(self):
        return self.get_file_path("sources", self.source_file_name)

    def setUp(self):
        self.addTypeEqualityFunc(pd.DataFrame, self.assertDataFrameEqual)

    def assertDataFrameEqual(self, a, b, msg):
        try:
            pd_testing.assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def get_file_path(self, dir_name, file_name):
        return os.path.join(
            os.path.join(os.path.dirname(__file__)),
            dir_name,
            file_name
        )


class TestCSVBase(TestBase):
    source_file_name = "source_data.csv"

    def setUp(self):
        super().setUp()
        self.source_data = pd.read_csv(self.source_file_path)
        self.loader = tax_stat.CSVLoader(self.source_file_path)


class TestSQLLiteBase(TestBase):
    source_file_name = "source_data.db"
    table_name = "Tax data"

    def setUp(self):
        super().setUp()
        self.source_data = self.get_source_data()

        self.loader = tax_stat.SQLLiteLoader(
            self.source_file_path, self.table_name
        )

    def get_source_data(self):
        conn = sqlite3.connect(self.source_file_path)
        data = pd.read_sql_query(f"SELECT * FROM '{self.table_name}'", conn)
        conn.close()
        return data


class TestCSVLoader(TestCSVBase):
    def test_load_data(self):
        self.assertEqual(self.source_data, self.loader.load_data())


class TestSQLLiteLoader(TestSQLLiteBase):
    def test_load_data(self):
        self.assertEqual(self.source_data, self.loader.load_data())


class TestDataFormatter(TestCSVBase):
    fixture_file_name = "formatted_data.csv"

    def test_get_formatted_data(self):
        data = tax_stat.DataFormatter().get_formatted_data(self.source_data)
        self.assertEqual(pd.read_csv(self.fixture_file_path), data)


class TestDataFormatterKeyError(TestCSVBase):
    source_file_name = "wrong_data_key_error.csv"

    def test_key_error(self):
        with self.assertRaises(KeyError) as context:
            tax_stat.DataFormatter().get_formatted_data(self.source_data)

        self.assertEqual(
            context.exception.args[0],
            (
                "Data does not contain all mandatory columns. "
                "KeyError: ['county'] not in index"
            )
        )


class TestDataFormatterSchemaError(TestCSVBase):
    source_file_name = "wrong_data_schema_error.csv"

    def test_schema_error(self):
        with self.assertRaises(pandera.errors.SchemaError):
            tax_stat.DataFormatter().get_formatted_data(self.source_data)


class TestDataCalculator(TestCSVBase):
    def setUp(self):
        super().setUp()
        formatter = tax_stat.DataFormatter()
        self.data = formatter.get_formatted_data(self.source_data)
        self.calculator = tax_stat.DataCalculator()

    def test_get_amount_taxes_per_stat(self):
        self.fixture_file_name = "amount_taxes_per_state.csv"
        result = self.calculator.get_amount_taxes_per_state(self.data)
        expected_result = pd.read_csv(self.fixture_file_path, index_col=0)
        self.assertEqual(result, expected_result)

    def test_get_average_taxes_per_state(self):
        self.fixture_file_name = "average_taxes_per_state.csv"
        result = self.calculator.get_average_taxes_per_state(self.data)
        expected_result = pd.read_csv(self.fixture_file_path, index_col=0)
        self.assertEqual(result, expected_result)

    def test_average_tax_rate_per_state(self):
        self.fixture_file_name = "average_tax_rate_per_state.csv"
        result = self.calculator.get_average_tax_rate_per_state(self.data)
        expected_result = pd.read_csv(self.fixture_file_path, index_col=0)
        self.assertEqual(result, expected_result)

    def test_average_country_tax_rate(self):
        result = self.calculator.get_average_country_tax_rate(self.data)
        self.assertEqual(result, 11.68)

    def test_country_tax_amount(self):
        result = self.calculator.get_country_tax_amount(self.data)
        self.assertEqual(result, 491000.0)


class TestController(unittest.TestCase):

    @unittest.mock.patch("configparser.ConfigParser.get")
    @unittest.mock.patch("configparser.ConfigParser.read")
    def test_get_source_type(self, m_read, m_get):
        tax_stat.Controller().get_source_type()
        self.assertEqual(m_get.call_count, 1)
        self.assertEqual(m_get.call_args.args, ('SETTINGS', 'source_type'))
        self.assertEqual(m_read.call_count, 1)
        self.assertEqual(m_read.call_args.args, ("config.ini",))

    def test_get_source_loader(self):
        args = mock.Mock()
        args.source_path = None
        result = tax_stat.Controller().get_source_loader("csv", args)
        self.assertIsInstance(result, tax_stat.CSVLoader)

        args.table_name = None
        result = tax_stat.Controller().get_source_loader("sqllite", args)
        self.assertIsInstance(result, tax_stat.SQLLiteLoader)

    def test_parse_args_csv(self):
        args = tax_stat.Controller().parse_args(
            "csv",
            [
                "--source_path",
                "tests/sources/source_data.db",
                "--get_amount_taxes_per_state",
                "--get_average_taxes_per_state",
                "--get_average_tax_rate_per_state",
                "--get_average_country_tax_rate",
                "--get_country_tax_amount"
            ]
        )

        self.assertEqual(args.source_path, "tests/sources/source_data.db")
        self.assertTrue(args.get_amount_taxes_per_state)
        self.assertTrue(args.get_average_taxes_per_state)
        self.assertTrue(args.get_average_tax_rate_per_state)
        self.assertTrue(args.get_average_country_tax_rate)
        self.assertTrue(args.get_country_tax_amount)

    def test_parse_args_sqllite(self):
        args = tax_stat.Controller().parse_args(
            "sqllite",
            [
                "--source_path",
                "tests/sources/source_data.db",
                "--table_name",
                "Tax stat",
                "--get_amount_taxes_per_state",
            ]
        )

        self.assertEqual(args.source_path, "tests/sources/source_data.db")
        self.assertEqual(args.table_name, "Tax stat")
        self.assertTrue(args.get_amount_taxes_per_state)
        self.assertIsNone(args.get_average_taxes_per_state)
        self.assertIsNone(args.get_average_tax_rate_per_state)
        self.assertIsNone(args.get_average_country_tax_rate)
        self.assertIsNone(args.get_country_tax_amount)

    @unittest.mock.patch("tax_stat.Controller.get_country_tax_amount")
    @unittest.mock.patch("tax_stat.Controller.get_average_country_tax_rate")
    @unittest.mock.patch("tax_stat.Controller.get_average_tax_rate_per_state")
    @unittest.mock.patch("tax_stat.Controller.get_average_taxes_per_state")
    @unittest.mock.patch("tax_stat.Controller.get_amount_taxes_per_state")
    @unittest.mock.patch("tax_stat.Controller.get_formatted_data")
    @unittest.mock.patch("tax_stat.Controller.get_source_loader")
    @unittest.mock.patch("tax_stat.Controller.get_source_type")
    def test_run(
        self,
        m_get_source_type,
        m_get_source_loader,
        m_get_formatted_data,
        m_get_amount_taxes_per_state,
        m_get_average_taxes_per_state,
        m_get_average_tax_rate_per_state,
        m_get_average_country_tax_rate,
        m_get_country_tax_amount
    ):
        m_get_source_type.return_value = "csv"

        test_argv = [
            "tax_stat.py",
            "--source_path",
            "tests/sources/source_data.csv",
            "--get_amount_taxes_per_state",
            "--get_average_taxes_per_state",
            "--get_average_tax_rate_per_state",
        ]

        with unittest.mock.patch.object(sys, 'argv', test_argv):
            tax_stat.Controller().run()
            self.assertEqual(m_get_amount_taxes_per_state.call_count, 1)
            self.assertEqual(m_get_average_taxes_per_state.call_count, 1)
            self.assertEqual(m_get_average_tax_rate_per_state.call_count, 1)
            self.assertEqual(m_get_average_country_tax_rate.call_count, 0)
            self.assertEqual(m_get_country_tax_amount.call_count, 0)
