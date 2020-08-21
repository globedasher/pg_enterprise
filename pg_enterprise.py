import os, sys, getopt, psycopg2, getpass, random, logging, yaml

def log_config():
    formatter = '%(asctime)s: %(levelname)s: %(message)s'
    logging.basicConfig(format=formatter,
            filename='debug.log',
            filemode='w',
            level=logging.INFO)
    logging.info("Logging initialized.")
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def get_connection_info():
    # Get host DB information
    connection_info = {}

    connection_info['target_host'] = input_stuff(
            "Target database host address (default is localhost):"
            ,"localhost")
    logging.log(20, "Target host name %s" % connection_info['target_host'])

    connection_info['dbname'] = input_stuff(
            "Database name (Name of database on host):"
            ,"postgres")
    logging.log(20, "Database name %s" % connection_info['dbname'])

    connection_info['user'] = input_stuff(
            "Database role (username - leave blank if same as your current user):"
            ,getpass.getuser())
    logging.log(20, "Username %s" % connection_info['user'])

    #Obviously, don't log the role password in the log
    connection_info['target_password'] = getpass.getpass("Target role password:")

    return connection_info

def create_connection(connection_info):
    # Connect to the target DB and create a cursor.
    try:
        conn = psycopg2.connect(
                dbname=connection_info['dbname'],
                user=connection_info['user'],
                password=connection_info['target_password'],
                host=connection_info['target_host'])
        target_cur = conn.cursor()
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


def main():
    log_config()
    logging.log(20, "Welcome to pg_enterprise. Did you checkout the readme?")

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
        logging.log(30, "Error: " + str(e))
    except (psycopg2.InternalError) as e:
        logging.log(30, "Error: " + str(e))
    except:
        logging.log(30, "Unhandled exception\n%s" % sys.exc_info())
        sys.exit(2)

    # Get the exclusions file and log the DBs to be excluded
    exclusions = open_yaml_file("./exclusions.yaml")
    for exclude in exclusions:
        #print("Excluding " + exclude)
        logging.log(20, "Exclude: %s" % exclude)

    #Start processing the DBs and log the start of the process
    with open("./sql/table.sql") as sql_query:
        sql = sql_query.read()
        #print(sql)
        for database in databases:
            if database[0] not in exclusions:
                #print(database[0])
                logging.log(20, "Database processed: %s" % database[0])
                connection_info['dbname'] = database[0]
                db_conn = create_connection(connection_info)
                try:
                    db_curr = db_conn.cursor()
                    db_curr.execute(sql)
                    results = db_curr.fetchall()
                    for result in results:
                        #print(result)
                        logging.log(20, result)
                    # Only use the commit if there are updates to the DB.
                    # Use this carefully!!!
                except (psycopg2.ProgrammingError) as e:
                    logging.log(30, "Error: " + str(e))
                    db_conn.rollback()
                    db_curr.close()
                    db_conn.close()
                    sys.exit(2)
                except (psycopg2.InternalError) as e:
                    logging.log(30, "Error: " + str(e))
                    db_conn.rollback()
                    db_curr.close()
                    db_conn.close()
                    sys.exit(2)
                except:
                    logging.log(30, "Unhandled exception\n%s" % sys.exc_info())
                    db_conn.rollback()
                    sys.exit(2)
                db_conn.commit()
                db_curr.close()
                db_conn.close()



# Initiate the main function. The folloing calls main() by default if the
# script is being called directly.
if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception(str(sys.exc_info()))
        logging.exception(str(sys._getframe()))
        sys.exit(2)
