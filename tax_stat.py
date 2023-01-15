import abc
import argparse
import configparser
import sqlite3
import sys

import pandas as pd
import pandera


class LoaderBase(abc.ABC):

    @abc.abstractmethod
    def load_data(self):  # pragma: no cover
        pass


class CSVLoader(LoaderBase):
    def __init__(self, source_path):
        self.source_path = source_path

    def load_data(self):
        return pd.read_csv(self.source_path)


class SQLLiteLoader(LoaderBase):
    def __init__(self, source_path, table_name):
        self.source_path = source_path
        self.table_name = table_name

    def load_data(self):
        conn = sqlite3.connect(self.source_path)
        data = pd.read_sql_query(f"SELECT * FROM '{self.table_name}'", conn)
        conn.close()
        return data


class DataFormatter:
    is_non_negative = pandera.Check(lambda value: value >= 0)

    schema = pandera.DataFrameSchema({
        "county": pandera.Column(str, nullable=False),
        "state": pandera.Column(str, nullable=False),
        "tax rate": pandera.Column(
            float, checks=is_non_negative, nullable=False, coerce=True
        ),
        "tax amount": pandera.Column(
            float, checks=is_non_negative, nullable=False, coerce=True
        )
    })

    def get_formatted_data(self, data):
        data.columns = data.columns.str.lower()

        try:
            data = data[["county", "state", "tax rate", "tax amount"]]
        except KeyError as e:
            msg = "Data does not contain all mandatory columns. KeyError: %s" \
                  % str(e).strip("\"")
            raise KeyError(msg)

        return self.schema(data)


class DataCalculator:
    def get_amount_taxes_per_state(self, data):
        return data[["state", "tax amount"]].groupby(["state"]).sum()

    def get_average_taxes_per_state(self, data):
        return data[["state", "tax amount"]].groupby(["state"]).mean()

    def get_average_tax_rate_per_state(self, data):
        return data[["state", "tax rate"]].groupby(["state"]).mean()

    def get_average_country_tax_rate(self, data):
        return data[["tax rate"]].mean()["tax rate"]

    def get_country_tax_amount(self, data):
        return data[["tax amount"]].sum()["tax amount"]


class Controller(DataFormatter, DataCalculator):
    def get_source_type(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        return config.get("SETTINGS", "source_type")

    def get_source_loader(self, source_type, parsed_args):
        if source_type == "csv":
            return CSVLoader(parsed_args.source_path)
        elif source_type == "sqllite":
            return SQLLiteLoader(
                parsed_args.source_path, parsed_args.table_name
            )

    def parse_args(self, config_source, args):
        parser = argparse.ArgumentParser(description="Tax statistic")
        parser.add_argument("--source_path", required=True)

        if config_source == "sqllite":
            parser.add_argument("--table_name", required=True)

        for arg in [
            "--get_amount_taxes_per_state",
            "--get_average_taxes_per_state",
            "--get_average_tax_rate_per_state",
            "--get_average_country_tax_rate",
            "--get_country_tax_amount"
        ]:
            parser.add_argument(arg, action=argparse.BooleanOptionalAction)

        return parser.parse_args(args)

    def run(self):
        source_type = self.get_source_type()
        parsed_args = self.parse_args(source_type, sys.argv[1:])
        data = self.get_source_loader(source_type, parsed_args).load_data()
        data = self.get_formatted_data(data)

        for msg, arg in (
            ("Amount taxes per state:", "get_amount_taxes_per_state"),
            ("Average taxes per state:", "get_average_taxes_per_state"),
            ("Average tax rate per state:", "get_average_tax_rate_per_state"),
            ("Average country tax rate:", "get_average_country_tax_rate"),
            ("Country tax amount:", "get_country_tax_amount")
        ):
            if getattr(parsed_args, arg, None):
                print(f"{msg}\n{getattr(self, arg)(data)}\n")


if __name__ == "__main__":  # pragma: no cover
    Controller().run()
