# Azure Function to Merge Delta Tables

This GitHub repo contains the source code for this article written on [www.sidequests.blog](https://sidequests.blog/2024/01/29/merge-delta-lake-tables-with-azure-functions-delta-rs-polars/)

The `function_app.py` script is an Azure Function designed to merge Delta Tables. Here's a high-level overview of its operation:

1. **Imports**: The script begins by importing necessary libraries. These include `polars` for data manipulation, `logging` for logging events, `azure.functions` for Azure Function utilities, and `deltalake` for working with Delta Tables.

2. **Reading the Source DataFrame**: The script reads a source DataFrame from a CSV file located at a specified path in Azure Data Lake Storage. This is accomplished using the `polars.read_csv` function with the provided Azure Data Lake Storage connection string.

3. **Checking and Creating the Target Delta Table**: The script checks if a Delta Table exists at the specified target path. If it doesn't exist, a new Delta Table is created using the `write_deltalake` function with the source DataFrame.

4. **Merging the Source DataFrame into the Target Delta Table**: If the target Delta Table exists, the script merges the source DataFrame into the target Delta Table based on a provided predicate. This is done using the `merge` method of the `DeltaTable` class. The `when_matched_update_all` and `when_not_matched_insert_all` methods are used to specify that all matching records should be updated and all non-matching records should be inserted.

5. **Error Handling**: If there's an error at any point during the execution of the script, it logs the error and returns an HTTP response with a status code of 400.

6. **Parameters**: The script takes the following parameters: `sourcepath` (the path to the source CSV file), `targetpath` (the path to the target Delta Table), and `primarykeys` (the primary keys for merging the Delta Tables). These parameters can be provided either as URL parameters or in the request body.

This script is designed to be deployed as an Azure Function, which means it can be triggered by various events and run on demand. It provides a serverless way to merge Delta Tables in Azure Data Lake Storage.

## Getting started
To get started with this project follow this [Quickstart Guide for Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-python?pivots=python-mode-decorators)

## Example of Usage

The Azure Function takes the following parameters:

- `sourcepath`: The path to the source CSV file in Azure Data Lake Storage.
- `targetpath`: The path to the target Delta Table in Azure Data Lake Storage.
- `primarykeys`: A list of primary keys for merging the Delta Tables.

You can provide these parameters either as URL parameters or in the request body.

### URL Params examples
```json
{
    "sourcepath":  "abfs://<container>@<storageaccountname>.dfs.core.windows.net/<path_to_file>",
    "targetpath": "abfs://<container>@<storageaccountname>.dfs.core.windows.net/<path_to_file>",
    "primarykeys": ["CustomerID"]
}
```

## Notes to self (and you?) 
- Storage options for polars.read_csv works with full connection_string, but write_deltalake works with account_key. Would be nice if the second one would work with the connection string as well? (needs to be implemented in the Delta-RS rust lib?)
- Deploying this function from a yaml pipeline with GitHub Actions is bugged. Currently deploying from VSCode directly. Also see [Github issue 1262](https://github.com/Azure/azure-functions-python-worker/issues/1262) and [Github issue 1338](https://github.com/Azure/azure-functions-python-worker/issues/1338)




