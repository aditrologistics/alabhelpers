import boto3
import os


def get_table(env_var: str, default_value: str = None):
    table_name = os.environ.get(env_var, default_value)
    if not table_name:
        raise ValueError(f"No environment variable '{env_var}'.")
    return boto3.resource('dynamodb').Table(table_name)


def query_table(table, *, keycondition, processor, logger, **kwargs):
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
