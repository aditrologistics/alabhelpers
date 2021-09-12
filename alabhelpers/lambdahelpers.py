import json

DEFAULT_HEADERS = {
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}


def response(content, *, status: int = 200) -> dict:
    result = {
        'statusCode': status,
        "headers": {'Content-Type': 'application/json', **DEFAULT_HEADERS},
        'body': json.dumps(content)
    }
    return result
