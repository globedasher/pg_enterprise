### pg_enterprise.py run a script agains several PostgreSQL DBs at once.
___

Currently, the script will copy roles from one PostgreSQL host to a destination PostgreSQL host.

___


1. `git clone https://github.com/globedasher/pg_enterprise`
1. Install a virtual environment (on linux install `virtualenv`) 
1. Run `virtualenv <env>` in the pg_enterprise root

   NOTE: I use `env` for my `<env>` environment folder name as it makes things smooth when working with Python. Not using a virtual environment can cause conflicts between modules installed and OS used modules.

1. Run `source <env>/bin/activate` to activate the virtual environment.
1. Run `pip install -r requirements.txt` to install module dependencies.
1. Run `python py_enterprise.py` and proide information at the prompts.
