import sqlite3
from buff_utils import trunc_to_hour, trunc_to_min

class DB:
  def __init__(self,dbname):
      self.conn = sqlite3.connect(dbname)
      c = self.conn.cursor()
      c.execute("""CREATE TABLE IF NOT EXISTS minutes
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch INTEGER,
                    value REAL)""")
      c.execute("""CREATE TABLE IF NOT EXISTS hours
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch INTEGER,
                    value REAL)""")
      self.conn.commit()
      tables = self.conn.execute("""SELECT name
                                    FROM sqlite_master
                                    WHERE type = 'table'""").fetchall()
      self.table_names = set([e[0] for e in tables])


  def insert(self,table=None,data=None):
    """Insert data into table.

    :param:  table  <default> None
    :param:  data  <default> None

    data parameter is a tuple like that:
      data = (epoch,value)

    where epoch is a unix timestamp and value
    is a floating point number.

    """

    if table in self.table_names:
      if table == 'minutes':
        self._insert_minutes(table, data)
        self._insert_hours('hours', data)
      elif table == 'hours':
        self._insert_hours(table, data)


  def _insert_hours(self,table,data):
    if data is not None:
      arg_epoch, arg_value = data
      arg_epoch = trunc_to_hour(arg_epoch)
      m = self.fetch_from_epoch('hours', trunc_to_hour(arg_epoch))
      if m is None:
        sql = """INSERT INTO {}(epoch,value) VALUES (?,?)""".format(table)
        self.conn.execute(sql, (arg_epoch,arg_value) )
      else:
        idx, epoch, value = m
        if arg_value < value:
          sql = """UPDATE hours SET epoch = ?,value=? WHERE id=?"""
          self.conn.execute(sql, (arg_epoch,arg_value,idx))
      self.conn.commit()
    else:
      return None


  def _insert_minutes(self,table,data):

    arg_epoch, arg_value = data
    arg_epoch = trunc_to_min(arg_epoch)
    sql = """INSERT INTO {}(epoch,value) VALUES (?,?)""".format(table)
    self.conn.execute(sql, (arg_epoch,arg_value))
    self.conn.commit()
    return


  def reset(self,table_name):
    """Reset database tables.

    :param:  table_name

    table_name table will be removed, and it's primary key will
    be restored to the initial value."""

    if table_name in self.table_names:
      self.conn.execute("DELETE FROM {}".format(table_name))
      sql = """DELETE FROM SQLITE_SEQUENCE
               WHERE NAME = '{}'""".format(table_name)
      self.conn.execute(sql)
      return self.conn.commit()
    return []


  def fetch_all(self,table_name,start=None,end=None):
    """Query table_name database table.

    :param:  table_name
    :param:  start
    :param:  end

    table_name: database table queried
    start - end: time window to consider

    Data times in future date will be ingored."""

    if table_name in self.table_names:
      if start is None or end is None:
        sql = """SELECT epoch,value
                 FROM {}""".format(table_name)
      else:
        sql = """SELECT epoch,value
                 FROM {}
                 WHERE epoch <= {} and epoch >= {}""".format(table_name,
                                                             start,end)
      return self.conn.execute(sql).fetchall()
    return []


  # TODO
  def fetch_from_epoch(self,table,epoch):
    """fetch epoch from table_name database table.

    :param:  table
    :param:  epoch

    table_name: database table queried
    start - end: time window to consider

    """

    if table in self.table_names:
      sql = """SELECT * FROM {} WHERE epoch = '{}'""".format(table,epoch)
      return self.conn.execute(sql).fetchone()



  # def evaluate(self,table,start=None,end=None):
  #   if table in self.table_names:
  #     if start is not None and end is not None:
  #       sql = """SELECT epoch, MIN(value), MAX(value), AVG(value)
  #                FROM {}
  #                WHERE epoch IN (SELECT epoch
  #        FROM {}
  #        WHERE epoch <= {} and epoch >= {})
  #        GROUP BY epoch % 3600 = 0""".format(table,table,
  #                                                    start,end)

  #       return self.conn.execute(sql).fetchall()







