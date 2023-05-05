from dataclasses import dataclass, field
from itertools import chain
import re
from typing import Union, List
import yaml
import awswrangler as wr
import sqlglot

from fs_tool.dsl.operations.operations import BaseOp, CompoundBaseOp


def create_feature_name(operation: Union[BaseOp, CompoundBaseOp], cfg_name: str) -> str:
    """Add information about the window and partition_by columns to the feature name defined in the config file."""

    if isinstance(operation, BaseOp):
        return "_".join([cfg_name, operation.window] + operation.partition_by)
    else:
        return "_".join([cfg_name] + operation.windows + operation.partition_by)


@dataclass
class Feature:
    """Keep information about how to calculate and how to define a feature in a Feature Group.

    Args:
        cfg_name:
            Name of the feature as defined in the config file.
        description:
            Description of the feature.
        operation:
            Operation object obtained from the feature's operation expression.

    Attributes:
        name:
            Final name of the feature, after adding a sufix with the windows and partition_by columns.
        query:
            The SQL expression that defines the calculation of the feature.
    """

    cfg_name: str = field(repr=False)
    name: str = field(init=False)
    description: str
    operation: Union[BaseOp, CompoundBaseOp]
    query: str = field(init=False, repr=False)

    def __post_init__(self):
        self.name = create_feature_name(self.operation, self.cfg_name)
        self.query = self.operation.operation_query + f" AS {self.name}"


def validate_condition(condition: str, dataset_columns: List[str]) -> None:
    """Check if the condition is a valid SQL logical expression and if the identifiers."""

    try:
        _ = sqlglot.transpile(condition)
    except Exception as e:
        raise ValueError(f'The condition "{condition}" is not a valid SQL logical expression.') from e

    for identifier in (
        ident.name for ident in sqlglot.parse_one(condition).find_all(sqlglot.exp.Identifier)  # type: ignore
    ):
        if identifier not in dataset_columns:
            raise ValueError(
                f'The identifier "{identifier}" of the condition "{condition}" is not a column of the dataset.'
            )


class Parser:
    """Parse and validate the Feature Group and feature definitions from a config file.

    Args:
        fg_config_file:
            Path to the config file (YAML).

    Raises:
        ValueError:
            When some element in the config file is not valid.
    """

    def __init__(self, fg_config_path: str) -> None:
        self.fg_config = yaml.load(open(fg_config_path), Loader=yaml.FullLoader)
        self.features: List[Feature] = []

    def parse_dataset_uri(self) -> None:
        """Retrieve information about the dataset that will be utilized to calculate the features."""

        try:
            self.ds_bucket = str(self.fg_config["dataset"]["bucket"]).lower()
            self.ds_prefix = str(self.fg_config["dataset"]["prefix"]).lower()
            self.ds_name = str(self.fg_config["dataset"]["name"]).lower()
        except Exception as e:
            raise ValueError("Error: the config file must provide the bucket, prefix and name of the dataset.") from e
        self.ds_uri = f"s3://{self.ds_bucket}/{self.ds_prefix}/{self.ds_name}.csv"

    def validate_dataset(self) -> None:
        """Check if the dataset URI is valid."""

        try:
            ds = wr.s3.read_csv(self.ds_uri, chunksize=1)
            self._ds_head = next(ds)
        except Exception as e:
            raise ValueError(f"Error: could not read the dataset {self.ds_uri}. Make sure it exists.") from e

    def parse_fg_info(self) -> None:
        """Retrieve information about the Feature Group and feature windows."""

        try:
            self.fg_name = str(self.fg_config["feature_group"]["name"]).lower()
            self.fg_description = str(self.fg_config["feature_group"]["description"])
            self.fg_time_column = str(self.fg_config["feature_group"]["time_column"]).lower()
            self.fg_entity_columns = [
                str(col).lower() for col in list(self.fg_config["feature_group"]["entity_columns"])
            ]
            self.fg_windows = list(self.fg_config["feature_group"]["windows"])
            self.fg_cpd_windows = list(self.fg_config["feature_group"]["cpd_windows"])
            self.fg_features = list(self.fg_config["feature_group"]["features"])
        except Exception as e:
            raise ValueError(
                "Error: the config file must provide the name, description, time_column, entity_columns, windows, "
                "cpd_windows and features of the feature group. The last 4 must be lists."
            ) from e

    def validate_fg_columns(self) -> None:
        """Check if time and entity columns are present in the dataset."""

        for col in [self.fg_time_column] + self.fg_entity_columns:
            if col not in self._ds_head.columns:
                raise ValueError(f"Error: the column {col} is not in the dataset.")

    def validate_windows(self) -> None:
        """Check if the given windows are valid."""

        for window in chain.from_iterable([self.fg_windows] + self.fg_cpd_windows):
            if re.fullmatch("([1-9]|1[0-9]|2[0-3])h|([1-9]|[1-9][0-9]|1[0-7][0-9]|180)d", str(window).lower()) is None:
                raise ValueError(f"Error: The window {window} is not valid. It must be 1-23h or 1-180d.")

    def parse_features(self) -> None:
        """Transform the feature definitions into Feature objects."""

        for feat in self.fg_features:
            try:
                name = str(feat["name"]).lower()
                description = str(feat["description"])
                operation_exp = str(feat["operation"])
                condition = str(feat["condition"]) if feat["condition"] else feat["condition"]
            except Exception as e:
                raise ValueError(
                    "Error: the feature definition must provide the name, description, operation and condition "
                    "of the feature."
                ) from e

            if condition:
                validate_condition(condition, self._ds_head.columns.to_list())

            operation_class = None
            for op_cls in BaseOp.subclasses + CompoundBaseOp.subclasses:
                if op_cls.reg_exp().fullmatch(operation_exp):
                    operation_class = op_cls
            if not operation_class:
                raise ValueError(f"Error: The operation expression {operation_exp} is not valid.")

            windows = self.fg_windows if issubclass(operation_class, BaseOp) else self.fg_cpd_windows
            for window in windows:
                operation = operation_class(
                    operation_exp, self.fg_time_column, self.fg_entity_columns, window, condition
                )  # type: ignore
                self.features.append(Feature(name, description, operation))

    def validate_feature_names(self) -> None:
        """Check if there are duplicated features."""

        feature_names = [feature.name for feature in self.features]
        for feature_name in feature_names:
            if feature_names.count(feature_name) > 1:
                raise ValueError(f"Error: the feature {feature_name} is duplicated.")

    def validate_operation_columns(self) -> None:
        """Check if the columns utilized by operation are present in the dataset."""

        for feature in self.features:
            op_columns = []
            if isinstance(feature.operation, BaseOp):
                op_columns.append(feature.operation.op_column)
            else:
                op_columns.extend([op.op_column for op in feature.operation.single_ops])
            for op_column in op_columns:
                if op_column is not None and op_column not in self._ds_head.columns:
                    raise ValueError(f"Error: the operation column {op_column} is not in the dataset.")

    def parse(self) -> None:
        """Parse and validate the config file so the Feature Group can be created and the features calculated
        as exprected."""

        self.parse_dataset_uri()
        self.validate_dataset()
        self.parse_fg_info()
        self.validate_fg_columns()
        self.validate_windows()
        self.parse_features()
        self.validate_feature_names()
        self.validate_operation_columns()
