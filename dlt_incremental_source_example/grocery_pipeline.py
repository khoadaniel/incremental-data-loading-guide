import dlt
import requests


@dlt.source(
    name="grocery_source",
    # Let the schema freely evolve
    schema_contract={"tables": "evolve",
                     "columns": "evolve",
                     "data_type": "evolve"},
    # No unnesting of tables
    max_table_nesting=0,
)
def grocery_source(
    # We read the secrets and config from the .dlt/*.toml files
    my_api_secret_key=dlt.secrets.value,
    initial_value_created_after=dlt.config.value,
    account_numbers=dlt.config.value,
):
    # Each resource can be seen as a table in the schema, getting data from an API endpoint
    @dlt.resource(
        write_disposition="merge",
        table_name="sales",
        primary_key="id",
    )
    def sales_resource(
        updated_after=dlt.sources.incremental(
            # This is where we declare the field in the API response that we will use as a cursor
            cursor_path="updated_at",
            initial_value=initial_value_created_after),
    ):

        for account_number in account_numbers:
            url = f"""http://localhost:9191/v1/account/{
                account_number}/list_sales"""

            # In this example, the credentials are passed as a header
            headers = {"Authorization": f"""Bearer {my_api_secret_key}"""}

            data_field = "data"
            limit = 1
            skip = 0

            record_count = 0

            while True:
                params = {
                    "limit": limit,
                    "skip": skip,
                    # We retrieve the updated_after value from state (last sync's value)
                    "updated_after": updated_after.start_value,
                }

                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                response_to_yield = response.json()[data_field]
                record_count += len(response_to_yield)
                yield from response_to_yield

                # Check whether we have reached the end of the data
                if len(response_to_yield) < limit:
                    break
                skip += limit

            print(f"Loaded {record_count} " +
                  f"records for account {account_number}.")

    return (
        sales_resource,
        # Add more resources here if needed
    )


if __name__ == "__main__":
    # Initialize the source
    source = grocery_source()
    resource_names = ["sales_resource"]

    # We iterate over the resources (~TABLE) within the source (~SCHEMA)
    for resource_name in resource_names:
        # Create the pipeline that load data from the API and write it to a Postgres table
        pipeline = dlt.pipeline(
            pipeline_name=f"grocery_source_{resource_name}",
            destination="postgres",  # destination type
            dataset_name="grocery_data",  # destination schema
        )

        # We can access the resource by name
        resource = source.with_resources(
            resource_name)

        # Run and print the load information
        load_info = pipeline.run(resource)
        print(load_info)
