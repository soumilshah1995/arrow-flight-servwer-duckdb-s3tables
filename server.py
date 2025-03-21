import json
import duckdb
import os
import logging
import pyarrow as pa
from pyarrow import flight
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("duckdb-flight-server")

class DuckDBFlightServer(flight.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8815):
        self.location = f"grpc://{host}:{port}"
        super(DuckDBFlightServer, self).__init__(location=self.location)
        self.db_connections = {}
        self.lock = threading.Lock()
        logger.info(f"Server initialized at {self.location}")

    def _get_connection(self, client_id):
        """Get or create a DuckDB connection for a client."""
        with self.lock:
            if client_id not in self.db_connections:
                # Create a new connection
                conn = duckdb.connect(':memory:')

                # Install and load required extensions
                extensions = ['aws', 'httpfs', 'iceberg', 'parquet']
                for ext in extensions:
                    conn.execute(f"INSTALL {ext};")
                    conn.execute(f"LOAD {ext};")

                # Set up AWS credentials
                conn.execute("CALL load_aws_credentials();")
                conn.execute("""
                CREATE SECRET (
                    TYPE s3,
                    PROVIDER credential_chain
                );
                """)

                self.db_connections[client_id] = conn
                logger.info(f"Created new DuckDB connection for client {client_id}")

            return self.db_connections[client_id]

    def list_flights(self, context, criteria):
        """List available flights."""
        return []  # This server uses tickets for queries, no predefined flights

    def get_flight_info(self, context, descriptor):
        """Get flight info for a query."""
        try:
            query_info = json.loads(descriptor.command.decode('utf-8'))

            # Basic validation
            if not query_info.get('query') or not query_info.get('catalog_arn'):
                raise ValueError("Missing required parameters: query and catalog_arn")

            # Create a schema that matches what we'll return
            schema = pa.schema([
                pa.field('query_id', pa.string()),
                pa.field('status', pa.string())
            ])

            # Create an endpoint with ticket containing the query info
            endpoints = [flight.FlightEndpoint(
                ticket=flight.Ticket(descriptor.command),
                locations=[flight.Location(self.location)]
            )]

            return flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=endpoints,
                total_records=-1,
                total_bytes=-1
            )

        except Exception as e:
            logger.error(f"Error in get_flight_info: {str(e)}")
            raise flight.FlightServerError(f"Failed to process query request: {str(e)}")

    def do_get(self, context, ticket):
        """Execute a query and return results."""
        try:
            # Extract query details from ticket
            query_info = json.loads(ticket.ticket.decode('utf-8'))
            query = query_info.get('query')
            catalog_arn = query_info.get('catalog_arn')
            client_id = query_info.get('client_id', 'default')

            if not query or not catalog_arn:
                raise ValueError("Missing required parameters: query and catalog_arn")

            # Get connection for this client
            conn = self._get_connection(client_id)

            # Attach the catalog if not already attached
            try:
                # Attach catalog
                logger.info(f"Attaching catalog: {catalog_arn}")
                conn.execute(f"""
                ATTACH '{catalog_arn}' 
                AS s3_tables_db (
                    TYPE iceberg,
                    ENDPOINT_TYPE s3_tables
                );
                """)
                logger.info("Catalog attached successfully")
            except Exception as e:
                # If it fails because it's already attached, that's fine
                if "already exists" not in str(e):
                    logger.warning(f"Note about catalog attachment: {str(e)}")

            # Execute the query and convert results to Arrow table
            logger.info(f"Executing query: {query}")
            result = conn.execute(query).fetch_arrow_table()
            logger.info(f"Query executed successfully, returning {result.num_rows} rows")

            # Return results as a stream
            return flight.RecordBatchStream(result)

        except Exception as e:
            error_msg = f"Query execution error: {str(e)}"
            logger.error(error_msg)

            # Create an error table to return
            error_table = pa.Table.from_pydict({
                "error": [str(e)],
                "details": ["Query execution failed"]
            })

            return flight.RecordBatchStream(error_table)

    def do_action(self, context, action):
        """Handle client actions like closing connections."""
        if action.type == "close_connection":
            try:
                action_data = json.loads(action.body.to_pybytes().decode('utf-8'))
                client_id = action_data.get('client_id', 'default')

                with self.lock:
                    if client_id in self.db_connections:
                        self.db_connections[client_id].close()
                        del self.db_connections[client_id]
                        logger.info(f"Closed connection for client {client_id}")

                return [flight.Result(b"Connection closed")]
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
                return [flight.Result(f"Error: {str(e)}".encode('utf-8'))]

        return [flight.Result(b"Unknown action")]

def serve():
    """Start the Flight server."""
    host = "0.0.0.0"  # Listen on all interfaces
    port = 8815

    # Set environment variables if running locally
    if not os.environ.get('AWS_ACCESS_KEY_ID'):
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv("AWS_ACCESS_KEY_ID", "")
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        os.environ['AWS_DEFAULT_REGION'] = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    try:
        server = DuckDBFlightServer(host=host, port=port)
        logger.info(f"Starting DuckDB Flight server on {host}:{port}")
        logger.info(f"DuckDB version: {duckdb.__version__}")
        server.serve()
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")

if __name__ == "__main__":
    serve()
