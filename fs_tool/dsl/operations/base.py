from abc import ABC, abstractmethod
import re
from typing import List, Optional, Type

from fs_tool.dsl.operations.utils import get_window_expression, get_operation_query


class BaseOp(ABC):
    """Base class for single operations."""

    exp: str
    time_column: str
    partition_by: List[str]
    window: str
    condition: Optional[str]

    subclasses: List[Type["BaseOp"]] = []

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)

    @staticmethod
    @abstractmethod
    def reg_exp() -> re.Pattern:
        """Regular expression that identifies the operation from the DSL expression."""

        pass

    @property
    @abstractmethod
    def op_column(self) -> Optional[str]:
        """Column on which the operation is applied."""

        pass

    @property
    @abstractmethod
    def sql_op_exp(self) -> str:
        """SQL expression corresponding to the operation."""

        pass

    @property
    def sql_window_exp(self) -> str:
        """DSL window expression converted to a SQL interval expression."""

        return get_window_expression(self.window)

    @property
    def operation_query(self) -> str:
        """SQL window function expression that applies the operation."""

        return get_operation_query(self)


class CompoundBaseOp:
    """Base class for compound operations."""

    exp: str
    time_column: str
    partition_by: List[str]
    windows: List[str]
    condition: Optional[str]

    subclasses: List[Type["CompoundBaseOp"]] = []

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)

    @staticmethod
    @abstractmethod
    def reg_exp() -> re.Pattern:
        """Regular expression that identifies the operation from the DSL expression."""

        pass

    @property
    def single_ops(self) -> List[BaseOp]:
        """List of single operations that make up the compound operation, following the order they appear in the
        DSL expression."""

        groups = self.reg_exp().match(self.exp).groups()  # type: ignore
        ops = []
        window_idx = 0
        for gp in groups:
            for op_cls in BaseOp.subclasses:
                if gp is not None and op_cls.reg_exp().fullmatch(gp):
                    ops.append(
                        op_cls(
                            gp, self.time_column, self.partition_by, self.windows[window_idx], self.condition
                        )  # type: ignore
                    )
                    window_idx += 1
        return ops

    @property
    @abstractmethod
    def operation_query(self) -> str:
        """SQL window function expression that applies the operation."""

        pass
