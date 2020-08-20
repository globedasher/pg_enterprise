import os, sys, getopt, psycopg2, getpass, random, logging, yaml

def log_config():
    formatter = '%(asctime)s: %(levelname)s: %(message)s'
    logging.basicConfig(format=formatter,
            filename='debug.log',
            filemode='w',
            level=logging.INFO)
    logging.info("Logging initialized.")

def create_connection(dbname, user, password, host):
    # Connect to the source DB and create a cursor.
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        source_cur = conn.cursor()
    except (psycopg2.OperationalError) as e:
        logging.log(30, "Error: " + str(e))
        sys.exit(2)
    except:
        logging.log(30, "Unhandled exception:\n" + str(sys.exc_info()))
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

def get_connection_info():
    # Get host DB information
    connection_info = {}

    connection_info['source_host'] = input_stuff(
            "Source database host address (default is localhost):"
            ,"localhost")
    logging.log(20, "Source host name %s" % connection_info['source_host'])

    connection_info['dbname'] = input_stuff(
            "Database name (Name of database on host):"
            ,"postgres")
    logging.log(20, "Database name %s" % connection_info['dbname'])

    connection_info['user'] = input_stuff(
            "Database role (username - leave blank if same as your current user):"
            ,getpass.getuser())
    logging.log(20, "Username %s" % connection_info['user'])

    #Obviously, don't log the role password in the log
    connection_info['source_password'] = getpass.getpass("Source role password:")

    return connection_info



def main():
    log_config()
    welcome = "Initiating pg_enterprise: run scripts across your postgres cluster! Welcome to pg_enterprise.py. After running, review the debug.log file for run info."
    print(welcome)

    # Get all the connection information needed to access the DB.
    connection_info = get_connection_info()

    # Create the DB connections and cursors
    source_conn = create_connection(connection_info['dbname'], connection_info['user'], connection_info['source_password'], connection_info['source_host'])
    source_cur = source_conn.cursor()

    exclusions = open_yaml_file("./exclusions.yaml")
    for exclude in exclusions:
        logging.log(20, "Exclude: %s" % exclude)

    try:
        # Get a list of databases on the source database
        SQL = "select datname from pg_catalog.pg_database;"
        source_cur.execute(SQL)
        databases = source_cur.fetchall()
    except (psycopg2.ProgrammingError) as e:
        logging.log(30, "Error: " + str(e))
    except (psycopg2.InternalError) as e:
        logging.log(30, "Error: " + str(e))
    except:
        logging.log(30, "Unhandled exception\n%s" % sys.exc_info())
        sys.exit(2)

    for database in databases:
        if database[0] not in exclusions:
            logging.log(20, "Database processed: %s" % database[0])



    source_cur.close()
    source_conn.close()

# Initiate the main function. The folloing calls main() by default if the
# script is being called directly.
if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception(str(sys.exc_info()))
        logging.exception(str(sys._getframe()))
        sys.exit(2)
