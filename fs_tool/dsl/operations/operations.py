import re
from typing import List, Optional

from fs_tool.dsl.operations.base import BaseOp, CompoundBaseOp


class SumOp(BaseOp):
    """Sum operation. Sum the values of a column within a time period.

    Args:
        exp:
            DSL expression that defines the operation.
        time_column:
            Column utilized for the time period.
        partition_by:
            Columns by which the window function is partitioned.
        window:
            DSL expression that defines the interval or window.
        condition:
            SQL logical expression to filter the rows before calculating the feature.
    """

    def __init__(
        self, exp: str, time_column: str, partition_by: List[str], window: str, condition: Optional[str] = None
    ) -> None:
        self.exp = exp
        self.time_column = time_column
        self.partition_by = partition_by
        self.window = window
        self.condition = condition

    @staticmethod
    def reg_exp() -> re.Pattern:
        return re.compile(r"sum *\( *([\w_-]+) *\)", flags=re.IGNORECASE)

    @property
    def op_column(self) -> Optional[str]:
        return self.reg_exp().search(self.exp).group(1).lower()  # type: ignore

    @property
    def sql_op_exp(self) -> str:
        return f"SUM({self.op_column})" if not self.condition else f"SUM(IF({self.condition}, {self.op_column}, 0))"


class CountOp(BaseOp):
    """Count operation. Count the number of rows within a time period.

    Args:
        exp:
            DSL expression that defines the operation.
        time_column:
            Column utilized for the time period.
        partition_by:
            Columns by which the window function is partitioned.
        window:
            DSL expression that defines the interval or window.
        condition:
            SQL logical expression to filter the rows before calculating the feature.
    """

    def __init__(
        self, exp: str, time_column: str, partition_by: List[str], window: str, condition: Optional[str] = None
    ) -> None:
        self.exp = exp
        self.time_column = time_column
        self.partition_by = partition_by
        self.window = window
        self.condition = condition

    @staticmethod
    def reg_exp() -> re.Pattern:
        return re.compile(r"count *\( *\)", flags=re.IGNORECASE)

    @property
    def op_column(self) -> Optional[str]:
        return None

    @property
    def sql_op_exp(self) -> str:
        return "COUNT(1)" if not self.condition else f"COUNT(IF({self.condition}, 1, 0))"


class RatioOp(CompoundBaseOp):
    """Ratio operation. Divide one single operation by another.

    Args:
        exp:
            DSL expression that defines the operation.
        time_column:
            Column utilized for the time period.
        partition_by:
            Columns by which the window function is partitioned.
        windows:
            DSL expressions that define the interval or window for both numerator and denominator.
        condition:
            SQL logical expression to filter the rows before calculating the feature.
    """

    def __init__(
        self, exp: str, time_column: str, partition_by: List[str], windows: List[str], condition: Optional[str] = None
    ) -> None:
        self.exp = exp
        self.time_column = time_column
        self.partition_by = partition_by
        self.windows = windows
        self.condition = condition

    @staticmethod
    def reg_exp() -> re.Pattern:
        single_op_exps = [cls.reg_exp().pattern for cls in BaseOp.subclasses]
        return re.compile(
            rf"ratio *\( *({'|'.join(single_op_exps)}) *, *({'|'.join(single_op_exps)}) *\)", flags=re.IGNORECASE
        )

    @property
    def operation_query(self) -> str:
        return f"(({self.single_ops[0].operation_query})/({self.single_ops[1].operation_query}))"
