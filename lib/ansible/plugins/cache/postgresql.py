# (c) 2016, Alexander Sosna
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import sys
import time
import json

# Set default encoding
reload(sys)
sys.setdefaultencoding("utf-8")

from ansible import constants as C
from ansible.errors import AnsibleError
from ansible.plugins.cache.base import BaseCacheModule

try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    raise AnsibleError("The 'psycopg2' python module is required for the PostgreSQL fact cache, 'pip install psycopg2'")

class CacheModule(BaseCacheModule):
    """
    A caching module backed by PostgreSQL >= 9.5.

    Facts are stored in a table called "ansible_fact_cache".
    """
    def __init__(self, *args, **kwargs):
        if C.CACHE_PLUGIN_CONNECTION:
            # Get the connection string from the user
            connection = C.CACHE_PLUGIN_CONNECTION
        else:
            # Set default parameters instead
            connection = "dbname='ansible' user='ansible' hostaddr='127.0.0.1'"


        connection += " application_name='ansible_fact_cache'"
	minconnections = 1
        maxconnections = 2

        try:
            self._pool = ThreadedConnectionPool(minconnections, maxconnections, connection)
        except:
            raise AnsibleError("Unable to connect to PostgreSQL database!"
                               "Set connection string or use .pgpass file. Example conifguration line:"
                               "fact_caching_connection = \"hostaddr='127.0.0.1' dbname='ansible' user='ansible' password='mypassword'\"")

        # Set table name hard
        self._table = "ansible_fact_cache"
        self._timeout = float(C.CACHE_PLUGIN_TIMEOUT)

        # check if the table is present
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT TRUE FROM information_schema.tables WHERE table_name=%s;", (self._table,))
        if not bool(cur.rowcount):
            raise AnsibleError("Table \""+self._table+"\" does not exists. You can create it with:"
                               "CREATE TABLE \""+self._table+"\" ("
                               "host VARCHAR PRIMARY KEY,"
                               "changed  TIMESTAMP NOT NULL DEFAULT NOW(),"
                               "timeout BIGINT DEFAULT 0,"
                               "facts JSONB NOT NULL);")

        # remove all keys that expired at the beginning
        # keys are not removed if they expire during long running task
        try:
            cur.execute("DELETE FROM ansible_fact_cache "
                        "WHERE timeout != 0 "
                        "AND (changed + interval '1 second' * timeout) < NOW();")
        except:
            raise AnsibleError("Unable to delete old facts from PostgreSQL database")

        cur.close()
        self._pool.putconn(conn)


    def get(self, key):
        query = "SELECT facts FROM \""+self._table+"\" WHERE host = %s;"
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(query, (key,))
        value = cur.fetchone()
        cur.close()
        self._pool.putconn(conn)
        if value[0] is None:
            raise KeyError
        return value[0]

    def set(self, key, value):
        jvalue = json.dumps(value)
        # using UPSERT, PostgreSQL >= 9.5
        query = ("INSERT INTO \""+self._table+"\" "
                 "    (host, changed, timeout, facts) "
                 "VALUES (%s, NOW(), %s, %s) "
                 "ON CONFLICT (host) DO UPDATE SET facts = %s, changed = NOW(), timeout = %s;")
        params = (key, self._timeout, jvalue, jvalue, self._timeout)
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(query, params)
        cur.close()
        self._pool.putconn(conn)
        return True

    def keys(self):
        keys = dict()
        query = "SELECT host FROM \""+self._table+"\";"
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(query, (key,))
        for row in cur:
            keys += row
        cur.close()
        self._pool.putconn(conn)
        return keys

    def contains(self, key):
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT TRUE FROM \""+self._table+"\" WHERE host = %s;", (key,))
        value = cur.fetchone()
        cur.close()
        self._pool.putconn(conn)
        if value is None:
            return False 
        return True 

    def delete(self, key):
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute("DELETE FROM \""+self._table+"\" WHERE host = %s;", (key,))
        rowcount = cur.rowcount;
        cur.close()
        self._pool.putconn(conn)
        if rowcount <= 0:
            return False
        return True

    def flush(self):
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        # delete all rows
        cur.execute("DELETE FROM \""+self._table+"\";")
        cur.close()
        self._pool.putconn(conn)
        return True

    def copy(self):
        clone = dict()
        query = "SELECT host, facts FROM \""+self._table+"\";"
        conn = self._pool.getconn()
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        cur.execute(query, (key,))
        for row in cur:
            clone += row
        cur.close()
        self._pool.putconn(conn)
        return clone
