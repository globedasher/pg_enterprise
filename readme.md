### pg_enterprise.py run a script against several PostgreSQL DBs at once.
___

Run the same query on several postgres DBs in the same instance!

___


1. `git clone https://github.com/globedasher/pg_enterprise`
1. Install a virtual environment (on linux install `virtualenv`) 
1. Run `virtualenv <env>` in the pg_enterprise root

   NOTE: I use `env` for my `<env>` environment folder name as it makes things smooth when working with Python. Not using a virtual environment can cause conflicts between modules installed and OS used modules.

1. Run `source <env>/bin/activate` to activate the virtual environment.
1. Run `pip install -r requirements.txt` to install module dependencies.
1. Run `python py_enterprise.py` and proide information at the prompts.

Use the -s flag and aws to use AWS Secrets Manager or cli to manually input
endpoint credentials.

Intended to run a single SQL script  on postgresql clusters with matching
schemas across the DBs. Currently limited to one query per SQL script. Add
exclusions to the exclusions.yaml for unique DBs upon which you cannot run your
scripts. After running the script, review the log for further details of what
was completed or not.
