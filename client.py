import json
import pyarrow as pa
from pyarrow import flight
import pandas as pd

# Connection and query settings
FLIGHT_URL = "grpc://localhost:8815"
CATALOG_ARN = "arn:aws:s3tables:<REGION>:867098943567:bucket/XXX"
SQL_QUERY = "SELECT * FROM s3_tables_db.s3tablescatalog.daily_sales LIMIT 10;"

def run_query(query, catalog_arn, client_id="test-client"):
    """Run a query against the DuckDB Flight server and return results as a DataFrame."""
    # Connect to the Flight server
    print(f"Connecting to Flight server at {FLIGHT_URL}")
    client = flight.connect(FLIGHT_URL)

    try:
        # Prepare query information
        query_info = {
            "query": query,
            "catalog_arn": catalog_arn,
            "client_id": client_id
        }

        # Create flight descriptor with the query command
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(query_info).encode('utf-8')
        )

        # Get flight info and ticket
        print(f"Submitting query: {query}")
        flight_info = client.get_flight_info(descriptor)

        if not flight_info.endpoints:
            raise ValueError("No endpoints returned from server")

        ticket = flight_info.endpoints[0].ticket

        # Execute the query and get results
        print("Fetching results...")
        reader = client.do_get(ticket)
        table = reader.read_all()

        # Convert to pandas DataFrame
        df = table.to_pandas()
        print(f"Query complete - received {len(df)} rows")
        return df

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        # Close the connection
        print("Closing connection")
        client.close()

if __name__ == "__main__":
    try:
        # Run the query
        df = run_query(SQL_QUERY, CATALOG_ARN)

        # Display results
        print("\nQuery Results:")
        print("==============")
        print(df)
        print(f"\nTotal rows: {len(df)}")

    except Exception as e:
        print(f"Query failed: {str(e)}")
