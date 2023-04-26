import re


def get_window_expression(window: str) -> str:
    """Transform a window element into a SQL expression."""

    numeric_part = re.search("\d+", window).group(0)  # type: ignore
    alpha_part = re.search("\D", window).group(0).lower()  # type: ignore
    alpha_to_unity = {"d": "DAY", "h": "HOUR"}
    return f"'{numeric_part}' {alpha_to_unity[alpha_part]}"


def get_operation_query(operation) -> str:
    """Create the window function expression to the operation."""

    return (
        f"{operation.sql_op_exp} OVER (PARTITION BY {', '.join(operation.partition_by)} "
        f"ORDER BY CAST({operation.time_column} AS TIMESTAMP) "
        f"RANGE BETWEEN INTERVAL {operation.sql_window_exp} PRECEDING AND CURRENT ROW)"
    )
