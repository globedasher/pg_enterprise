import os, sys, getopt, psycopg2, getpass, random, logging, yaml, json, argparse
from datetime import datetime as date
import boto3
import base64
from botocore.exceptions import ClientError


def get_secret():
    logging.info("get_secret() called")
    secret_name = input_stuff(
            "Please enter the AWS secret name to be used:", None
            )
    if not secret_name:
        logging.info("Please rerun and add a secret name.")
        sys.exit(2)
    region_name = input_stuff(
            "Enter the region name where the secret is held:","us-west-2"
            )

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            retSecret = json.loads(secret)
            retSecret['dbname'] = 'postgres'
            logging.info("returning secret")
            return retSecret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            retSecret = json.loads(decoded_binary_secret)
            logging.info("returning decoded_binary_secret")
            return retSecret

def get_args():
    """
    Get argument data passed from the command line and return a dictionary of
    the arguments.
    """
    parser = argparse.ArgumentParser()

    help_text = """The control selector can be set to either 'aws' or cli'. 'aws' will use secrets manager to get the connection string. 'cli' will request the credentials interactively."""
    parser.add_argument("-s"
                        , "--selector"
                        , dest="selector"
                        , default=""
                        , help=help_text
                        )

    help_text = """Set the log level to debug or not."""
    parser.add_argument("-l"
                        , "--loglevel"
                        , dest="log_level"
                        , default=""
                        , help=help_text
                        )

    return parser.parse_args()

def log_config(log_filename):
    formatter = '%(asctime)s: %(levelname)s: %(message)s'
    logging.basicConfig(format=formatter,
            filename=log_filename,
            filemode='w',
            level=logging.NOTSET)
    logging.info("Logging initialized.")
    # This will cause the log lines to print to stdout as well
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def get_connection_info():
    # Get host DB information
    connection_info = {}

    connection_info['host'] = input_stuff(
            "Target database host address (default is localhost):"
            ,"localhost")
    logging.info("Target host name %s" % connection_info['host'])

    connection_info['dbname'] = input_stuff(
            "Database name (Name of database on host):"
            ,"postgres")
    logging.info("Database name %s" % connection_info['dbname'])

    connection_info['username'] = input_stuff(
            "Database role (username - leave blank if same as your current user):"
            ,getpass.getuser())
    logging.info("Username %s" % connection_info['username'])

    #Obviously, don't log the role password in the log
    connection_info['password'] = getpass.getpass("Target role password:")

    return connection_info

def create_connection(connection_info):
    # Connect to the target DB and create a cursor.
    logging.info("create_connection() called")
    try:
        conn = psycopg2.connect(
                dbname=connection_info['dbname'],
                user=connection_info['username'],
                password=connection_info['password'],
                host=connection_info['host'])
        target_cur = conn.cursor()
    except (psycopg2.OperationalError) as e:
        logging.error(str(e))
        sys.exit(2)
    except:
        logging.error(str(sys.exc_info()))
        sys.exit(2)
    return conn

def input_stuff(message, default):
    # Used to make data input and defaults generic
    data = input(message)
    if len(data) == 0:
        data = default
        print(data)
    return data

# Open YAML file and convert it to Python variables
def open_yaml_file(filename):
    #print(filename)
    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader

    with open(filename) as f:
        data = yaml.load(f, Loader=Loader)
    #print(f.closed)
    return data

def main():
    args = get_args()
    if not args.selector:
        print("No args?")
        sys.exit(2)

    log_filename = "log/pg_enterprise_" + str(date.now()) + ".log"
    log_config(log_filename)
    print("Welcome to pg_enterprise. Did you checkout the readme?")

    #all_passes()
    if args.selector == "aws":
        # Get secret information from aws
        connection_info = get_secret()
    elif args.selector == "cli":
        # Get all the connection information needed to access the DB.
        connection_info = get_connection_info()


    # Get a list of databases on the target database
    try:
        # Create the DB connections and cursors
        target_conn = create_connection(connection_info)
        target_cur = target_conn.cursor()
        SQL = "select datname from pg_catalog.pg_database;"
        target_cur.execute(SQL)
        databases = target_cur.fetchall()
        target_cur.close()
        target_conn.close()
    except (psycopg2.ProgrammingError) as e:
        logging.error(str(e))
    except (psycopg2.InternalError) as e:
        logging.error(str(e))
    except:
        logging.error(sys.exc_info())
        sys.exit(2)

    # Get the exclusions file and log the DBs to be excluded
    exclusions = open_yaml_file("./exclusions.yaml")
    for exclude in exclusions:
        #print("Excluding " + exclude)
        logging.info("Exclude: %s" % exclude)

    #Start processing the DBs and log the start of the process
    with open("./sql/table.sql") as sql_query:
        sql = sql_query.read()
        #print(sql)
        for database in databases:
            if database[0] not in exclusions:
                #print(database[0])
                logging.info("Database processed: %s" % database[0])
                connection_info['dbname'] = database[0]
                db_conn = create_connection(connection_info)
                try:
                    db_curr = db_conn.cursor()
                    db_curr.execute(sql)
                    if "SELECT" in sql:
                        results = db_curr.fetchall()
                        for result in results:
                            logging.info(result)
                except (psycopg2.ProgrammingError) as e:
                    logging.error(str(e))
                    db_conn.rollback()
                    db_curr.close()
                    db_conn.close()
                    sys.exit(2)
                except (psycopg2.InternalError) as e:
                    logging.error(str(e))
                    db_conn.rollback()
                    db_curr.close()
                    db_conn.close()
                    sys.exit(2)
                except:
                    logging.error(sys.exc_info())
                    db_conn.rollback()
                    sys.exit(2)
                # Commit the transaction
                db_conn.commit()
                # Close the cursor and connection
                db_curr.close()
                db_conn.close()

    print("Your logfile is at " + log_filename)

# Initiate the main function. The folloing calls main() by default if the
# script is being called directly.
if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception(str(sys.exc_info()))
        logging.exception(str(sys._getframe()))
        sys.exit(2)
