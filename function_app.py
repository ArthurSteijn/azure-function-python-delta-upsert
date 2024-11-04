import azure.functions as func
import logging
import os
from functions.custom_utils import get_param
from deltalake import DeltaTable, write_deltalake          
import polars as pl 
import fsspec

ADLS_CONNECTION_STRING = os.environ['ADLS_CONNECTION_STRING']
ADLS_ACCOUNT_KEY = os.environ['ADLS_ACCOUNT_KEY']

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
     
@app.function_name(name="to_delta_http_trigger")
@app.route(route="to_delta_http_trigger")
def todelta_http_trigger(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

    # define the parameters
    sourcepath, response = get_param(req, 'sourcepath')
    if response:
        return response

    targetpath, response = get_param(req, 'targetpath')
    if response:
        return response

    primarykeys, response = get_param(req, 'primarykeys')
    if response:
        return response

    # set predicate for the merge, based on primairy keys
    predicate = ' AND '.join([f's.{key}= t.{key}' for key in primarykeys])

    # read the source df 
    try:
        fs = fsspec.filesystem('abfs', connection_string=ADLS_CONNECTION_STRING)
        with fs.open(sourcepath, 'rb') as f:
            source_df = pl.read_csv(f, has_header=True, separator="|", quote_char='"')
        source_pa = source_df.to_arrow()
        logging.info(f"INFO: df read successfully")
    except Exception as e:
        msg = f"INFO: df not read successfully: {e}"
        logging.info(msg)
        return func.HttpResponse(msg, status_code=400)           

    # check if target exists and create a new one if not
    try:
        dt = DeltaTable(targetpath, storage_options={"account_key": ADLS_ACCOUNT_KEY}) 
        logging.info(f"INFO: delta table exists")
    except:
        write_deltalake(targetpath, source_pa, mode="error", storage_options={"account_key": ADLS_ACCOUNT_KEY})      
        msg = "INFO: new delta table created successfully"
        logging.info(msg)
        return  func.HttpResponse(msg, status_code=200)
    else:
        try:      
            result = (
                dt.merge(
                    source=source_pa,
                    predicate=predicate,
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            msg = f"INFO: df merged successfully: \n{result}"
            logging.info(msg)
            return  func.HttpResponse(msg, status_code=200)
        except Exception as e:
            msg = f"INFO: df not merged successfully: {e}"
            logging.info(msg)
            return func.HttpResponse(msg, status_code=400)