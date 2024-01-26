import azure.functions as func
import logging

def get_param(req, param_name):
    param_value = req.params.get(param_name)
    if not param_value:
        try:
            req_body = req.get_json()
            param_value = req_body.get(param_name)
        except ValueError:
            pass

    if not param_value:
        logging.info(f"No {param_name} provided")
        return None, func.HttpResponse(f"No {param_name} provided", status_code=400)

    return param_value, None