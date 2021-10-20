import json
import urllib
from decimal import Decimal

DEFAULT_HEADERS = {
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
        s = str(obj)
        if "." in s:
            return float(obj)
        return int(obj)
    return json.JSONEncoder.default(self, obj)


def response(content, *, status: int = 200) -> dict:
    result = {
        'statusCode': status,
        "headers": {'Content-Type': 'application/json', **DEFAULT_HEADERS},
        'body': json.dumps(content, default=str, cls=DecimalEncoder)
    }
    return result

def route_and_execute(event, routes) -> dict:
    """
    Matches the incoming event to a route and executes the route.

    Mapped functions should be defined as
       def my_func(mapped_param1, mapped_param2, ... [body], **kwargs):
           pass

    where mapped_paramN is the named path-parameter in the mapping string, e.g.
        mapper.connect(f"/contact/:company/:id", controller=get_contact, conditions={"method": ["GET"]})

    will work well with the following function
       def get_contact(company, id, **kwargs):
           ...
           return response({"message": "ok", "data": contact_info})

    If there is no need to peek into body, simply omit that parameter. It will still be found in kwargs.

    Failing to find a match will return a 400 response.

    Args:
        event: The incoming event-parameter to the lambda.main.
        routes: Application defined routes.

    Returns:
        dict: a response from the executed function or the error response from this function.
    """
    verb = event.get("httpMethod", "")
    match = routes.match(urllib.parse.unquote(event["path"]), environ={"REQUEST_METHOD": verb})
    print(f"{urllib.parse.unquote(event['path'])}[{verb}] -> {match}")
    if match:
        body = event.get("body") or "{}"  # Ensure body: None -> "{}"

        if type(body) is not dict:
            body = json.loads(body)
        return match["controller"](body=body, **match)

    print(f"{urllib.parse.unquote(event['path'])} is invalid")

    return response(f"{urllib.parse.unquote(event['path'])} is invalid", status=400)
