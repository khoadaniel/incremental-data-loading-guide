from fastapi import FastAPI, HTTPException, Path, Query
import pandas as pd
from datetime import date, datetime

app = FastAPI()


@app.get("/v1/account/{account_number}/list_sales")
def list_sales(account_number: int = Path(..., description="Account number, either 1 or 2"),
               updated_after: date = Query(
                   None, description="Filter records by the `updated_after` date (greater than the provided date)"),
               limit: int = Query(
                   10, description="Limit the number of records to return"),
               skip: int = Query(0, description="Skip the first n records")
               ):
    """
    This endpoint reads from a local CSV file based on the account number and filters records by the "updated_after" date.
    """
    # Define the filename based on the account number
    filename = f"simulated_db_{account_number}.csv"

    # Attempt to load the specified CSV file
    try:
        data = pd.read_csv(filename)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404, detail="Data for the specified account number not found")

    # Filter data by "updated_after" date if provided
    if updated_after:
        data = data[data["updated_at"] > updated_after.isoformat()]

    # Paginate the data based on the limit and skip values
    data = data.iloc[skip:skip + limit]

    response = {
        # Return the filtered data as JSON
        # Read more here: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
        "data": data.to_dict(orient="records"),
        "metadata": {
            "total_records": len(data),
            "request_timestamp": datetime.utcnow().isoformat() + 'Z'
        }
    }

    return response


# For running the application with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("get_endpoint:app", host="127.0.0.1", port=9191, reload=True)
