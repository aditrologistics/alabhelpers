import os
import boto3
from typing import List
import datetime
try:
    from .loghelpers import setup_logger
except ImportError:
    from loghelpers import setup_logger

logger = setup_logger('rdshelpers')


def _verify_params(secret_arn, cluster_arn, database, rds):
    secret_arn = secret_arn or os.environ.get("AURORA_SECRET_ARN")
    cluster_arn = cluster_arn or os.environ.get("AURORA_CLUSTER_ARN")
    database = database or os.environ.get("DATABASE")
    rds = rds or boto3.client('rds-data')

    if secret_arn is None:
        raise ValueError("'secret_arn' not set and not defined in AURORA_SECRET_ARN.")
    if cluster_arn is None:
        raise ValueError("'cluster_arn' not set and not defined in AURORA_CLUSTER_ARN.")
    if database is None:
        raise ValueError("'database' not set and not defined in DATABASE.")

    return secret_arn, cluster_arn, database, rds


def execute_sql(
        sql: str, 
        *,
        params: list = None,
        rds = None,
        cluster_arn: str = None,
        secret_arn: str = None,
        database: str = None) -> dict:
    """
    Execute sql-statement and return response

    Args:
        sql (str): sql-statement to execute
        rds ([type]): boto3-client for rds-data. If not set, a new client is created on each invocation.
        params (list, optional): Parameters for the sql-statement. Defaults to None.
        cluster_arn (str, optional): ARN to the cluster. If not set, use the value of the environment variable AURORA_CLUSTER_ARN.
        secret_arn (str, optional): ARN to the cluster. If not set, use the value of the environment variable AURORA_SECRET_ARN.
        database (str, optional): Name of database. If not set, use the value of the environment variable DATABASE.

    Returns:
        a dictionary response from the boto3 invocation.
    """
    secret_arn, cluster_arn, database, rds = _verify_params(secret_arn, cluster_arn, database, rds)
    # print(f"Executing: {sql}\nParams: {params}")

    response = rds.execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            parameters=params or [],
            sql=sql)
    # print(response)
    return response


def execute_sql_batch(
        sql: str,
        *,
        params: list = None,
        rds = None,
        cluster_arn: str = None,
        secret_arn: str = None,
        database: str = None) -> dict:
    """
    Execute sql-statement as batch and return response.

    Args:
        sql (str): sql-statement to execute
        rds ([type]): boto3-client for rds-data. If not set, a new client is created on each invocation.
        params (list, optional): Parameters for the sql-statement. Defaults to None.
        cluster_arn (str, optional): ARN to the cluster. If not set, use the value of the environment variable AURORA_CLUSTER_ARN.
        secret_arn (str, optional): ARN to the cluster. If not set, use the value of the environment variable AURORA_SECRET_ARN.
        database (str, optional): Name of database. If not set, use the value of the environment variable DATABASE.

    Returns:
        a dictionary response from the boto3 invocation.
    """
    logger.debug(f"Executing {len(params)}: {sql}")
    secret_arn, cluster_arn, database, rds = _verify_params(secret_arn, cluster_arn, database, rds)
    if not params:
        logger.debug("No records - leaving")
        return
    response = rds.batch_execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            sql=sql,
            parameterSets=params)
    # print(response)
    return response


def get_data(query_response: dict, columns: List[str]) -> list:
    """
    Extract data from a query response and return as a list of maps (field name -> value (str -> str)).
    It is important that the column list is ordered in the same way as the response.

    Args:
        query_response (dict): The response from execute_sql.
        columns (List): List of column names.

    Returns:
        list: List of rows, where each row is a map of (name -> value (str -> str))
    """
    def extract_value(v):
        if "isNull" in v:
            return None
        return list(v.values())[0]
    res = []
    for row in query_response.get("records", []):
        res.append({columns[i]: extract_value(v) for i, v in enumerate(row)})
    return res


def fetch_from_sql(
        sql: str,
        fields: List[str],
        params: list = None,
        rds = None,
        cluster_arn: str = None,
        secret_arn: str = None,
        database: str = None) -> list:
    """
    Execute the sql-statement and convert the response to a list of dictionaries.

    This function bundles execute_sql and get_data.

    Args:
        sql (str): sql-statement to execute.
        fields (List[str]): List of fields *in the order they are returned by the query,
        params (list, optional): Parameters. Defaults to None.
        rds ([type], optional): rds-client. Defaults to None.
        cluster_arn (str, optional): arn to cluster. Defaults to None.
        secret_arn (str, optional): arn to secret. Defaults to None.
        database (str, optional): Name of database. Defaults to None.

    Returns:
        list: List of dictionaries with column names -> values (as strings)
    """
    resp = execute_sql(
        sql,
        params=params,
        rds=rds,
        cluster_arn=cluster_arn,
        secret_arn=secret_arn,
        database=database)
    return get_data(resp, fields)


def make_param(fieldname: str, value, fieldtype: str) -> dict:
    """
    Create a parameter-dictionary based on the field-name, value and field type.

    Args:
        fieldname (str): Name of field/parameter.
        value ([type]): Value of parameter
        fieldtype (str): Type of parameter.

    Returns:
        dict: A parameter-dictionary for use with execute_sql.
    """
    def maptype(t: str) -> str:
        if t in ["string", "date"]:
            return "stringValue"
        if t in ["bool"]:
            return "booleanValue"
        return "longValue"

    def map_string(value, t):
        isnull = value is None
        if t == "date":
            if not value:
                return value, isnull
            if type(value) == datetime.datetime:
                return value.strftime("%Y-%m-%d"), isnull
            return value[:10], isnull
        if t == "int" and value == '':
            return 0, isnull
        if t in ["bool"]:
            return bool(value), isnull
        # if t == "string" and value is None:
        #     return ''
        return value, isnull

    mappedValue, isNull = map_string(value, fieldtype)
    parameter = {
        "name": fieldname,
        "value": { maptype(fieldtype): mappedValue }
    }
    if fieldtype == "date":
        parameter["typeHint"] = 'DATE'
    if value is None or isNull:
        parameter["value"] = { "isNull": True}

    return parameter
