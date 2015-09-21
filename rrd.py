import time
import sys
from db_utils import DB
from buff_utils import trunc_to_min, wnd_1, wnd_24

class RRD:
  def __init__(self,db_name=None):
    if db_name is None:
      db_name = 'rrd.db'

    self.db = DB(db_name)
    self.last_day = [None]*24
    self.last_hour = [None]*60

    t = time.time()
    self.start_h, self.end_h = wnd_1(t)
    self.start_d, self.end_d = wnd_24(t)

  def update_time(func):
    def inner(self,*args, **kwargs):
      t = time.time()
      self.start_h, self.end_h = wnd_1(t)
      self.start_d, self.end_d = wnd_24(t)
      return func(self,*args, **kwargs)
    return inner

  @update_time
  def save(self,epoch,value):
    """Save new data received to database.

    :param: epoch   timestamp of data to be saved
    :param: value   float value to be saved

    timestamp older than 24 hours or in future date
    will be discarded.
    """
    m = self.db.fetch_from_epoch('minutes',trunc_to_min(epoch))
    if m is None and ( self._last_hour(epoch) or self._last_day(epoch)) :
      self.db.insert('minutes',data=(trunc_to_min(epoch), value))


  @update_time
  def query(self,*args):
    """Query informations stored on the database.

    Valid params are 'hours' or 'minutes'.
    Parameter 'hours' return values store in the
    last 24 hours, together with min value stored,
    max value stored and average of the values for the
    last 24 hours.
    'minutes' parameter will return values store in the
    last 60 minutes, together with min value stored,
    max value stored and average of the values for the
    last 60 minutes.
    """

    if args[0] == 'minutes':
      data = self.db.fetch_all('minutes',self.start_h,self.end_h)
    elif args[0] == 'hours':
      data = self.db.fetch_all('hours',self.start_d,self.end_d)

    self.print_time_epoch(args[0], data)

    data_float = [e[1] for e in data]
    if len(data_float) > 0:
      print "min:{}  max:{}  avg:{} ".format(min(data_float),max(data_float),sum(data_float)/len(data_float))
    else:
      print "Not enough informations to evaluate min, max, avg. (Empty sequence)"

  def _last_hour(self,epoch):
    epoch = trunc_to_min(epoch)
    return int(self.end_h) <= int(epoch) <= int(self.start_h)


  def _last_day(self,epoch):
    epoch = trunc_to_min(epoch)
    return int(self.end_d) <= int(epoch) <= int(self.start_d)


  def print_time_epoch(self,table,data):
    print "".format('-'*5,str(table.upper()),'-'*5)
    for epoch,value in data:
      print '({}): <{}>  ==> {}'.format(epoch, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch)),value)



def print_usage():
  """Print help message for command line usage."""
  help = """          OPTIONS
  ======================================================

  save [epoch][value] ............. save value with epoch
  query [hours] ................... query last 24 hours data saved
  query [minutes] ................... query last 60 minutes data saved
  """

  print help


if __name__ == "__main__":
  usage_error = False
  try:
    cmd = sys.argv[1]
    if cmd == 'save':
      epoch = sys.argv[2]
      value = sys.argv[3]
    elif cmd == 'query':
      param = sys.argv[2]
    else:
      print_usage()
      usage_error = True
  except IndexError:
    usage_error = True
    print_usage()

  if not usage_error:
    r = RRD()
    if cmd == 'save':
      r.save(float(epoch),float(value))
    elif cmd == 'query':
      r.query(param)




