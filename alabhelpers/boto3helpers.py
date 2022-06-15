import boto3
import os


def get_table(env_var: str, default_value: str = None):
    """
    Return a dynamodb table object named as the value of the environment variable env_var.

    Args:
        env_var (str): Name of environment variable containing table name
        default_value (str, optional): Default value if env_var is not in environment. Defaul   ts to None.

    Raises:
        ValueError: Raised if there is not env_var and no default_value.

    Returns:
        _type_: A boto3 dynamo table
    """
    table_name = os.environ.get(env_var, default_value)
    if not table_name:
        raise ValueError(f"No environment variable '{env_var}'.")
    return boto3.resource('dynamodb').Table(table_name)


def query_table(table, *, keycondition, processor, logger, **kwargs):
    """
    Queries a boto3 dynamo table using the provided keycondition.
    The returned items are passed to the processor function (as a list)
    for processing with the **kwargs as additional parameters.

    Args:
        table: Boto3 dynamo table to query.
        keycondition (_type_): KeyConditionExpression-parameter. See boto3 documentation.
        processor (function): Function invoked with list of fetched items. It may be invoked multiple times.
        logger (_type_): Logger instance for logging.
    """
    logger.debug("Fetching data")
    ddb_response = table.query(KeyConditionExpression=keycondition)
    record_count = len(ddb_response['Items'])
    logger.debug(f"Fetched {record_count} records.")
    processor(
        items=ddb_response["Items"],
        **kwargs)
    lastkey = ddb_response.get("LastEvaluatedKey")

    while lastkey is not None:
        ddb_response = table.query(
            KeyConditionExpression=keycondition,
            ExclusiveStartKey=lastkey
        )
        count = len(ddb_response['Items'])
        record_count += count
        logger.debug(f"Fetched {count} records (total {record_count}).")
        lastkey = ddb_response.get("LastEvaluatedKey")
        processor(
            items=ddb_response["Items"],
            **kwargs)
    logger.debug(f"Fetched {record_count} records.")
