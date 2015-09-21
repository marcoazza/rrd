import unittest
from rrd import RRD
from buff_utils import trunc_to_min, wnd_1,trunc_to_hour, wnd_24
import time

HOUR = 3600
MIN = 60


class TestCircularBuffer(unittest.TestCase):

  def setUp(self):
    self.start_h = time.time()
    self.end_h = self.start_h - HOUR
    self.start_d = self.start_h
    self.end_d = self.start_d - (HOUR * 24)
    self.rrd = RRD(db_name='test.db')
    self.rrd.db.reset('minutes')
    self.rrd.db.reset('hours')

  def test__last_hour(self):
    t = time.time()
    self.assertTrue(self.rrd._last_hour(t))
    self.assertTrue(self.rrd._last_hour(self.end_h))
    self.assertTrue(self.rrd._last_hour(self.start_h))
    self.assertFalse(self.rrd._last_hour(t + HOUR + MIN ))
    self.assertFalse(self.rrd._last_hour(t - HOUR - MIN ))
    self.assertTrue(self.rrd._last_hour(t - HOUR + MIN))
    self.assertTrue(self.rrd._last_hour(t - HOUR))
    self.assertTrue(self.rrd._last_hour(t - MIN))

  def test__last_day(self):
    t = time.time()
    self.assertTrue(self.rrd._last_day(t))
    self.assertTrue(self.rrd._last_day(self.end_h))
    self.assertTrue(self.rrd._last_day(self.start_h))
    self.assertFalse(self.rrd._last_day(t + 24*HOUR + MIN ))
    self.assertTrue(self.rrd._last_day(t - 24*HOUR + MIN))
    self.assertTrue(self.rrd._last_day(t - 24*HOUR))
    self.assertFalse(self.rrd._last_day(t - 24*HOUR - MIN))
    self.assertFalse(self.rrd._last_day(t + 24*HOUR))
    self.assertFalse(self.rrd._last_day(t + 24*HOUR - MIN))




  def test_save_min(self):
    epoch = trunc_to_min(time.time())
    expired_data = [ (epoch + HOUR -x*MIN,x-2) for x in xrange(0,10)]

    for e in expired_data:
      self.rrd.save(e[0],e[1])

    retrive_data = self.rrd.db.fetch_all('minutes',wnd_1(epoch))
    self.assertEqual([], retrive_data)

    valid_data = [ (epoch -x*MIN,x+1) for x in xrange(0,10)]
    valid_data.reverse()

    for e in valid_data:
      self.rrd.save(e[0],e[1])

    self.assertEqual(valid_data, self.rrd.db.fetch_all('minutes',wnd_1(epoch)))


  def test_save_hour(self):
    epoch = trunc_to_min(time.time())
    expired_data = [ (epoch + HOUR -x*MIN,x-2) for x in xrange(0,10)]
    for e in expired_data:
      self.rrd.save(e[0],e[1])
    retrive_data = self.rrd.db.fetch_all('hours',wnd_1(epoch))
    self.assertEqual([], retrive_data)

    valid_data = [(epoch -x*MIN,float(x+1)) for x in xrange(0,10)]
    valid_data.reverse()

    for e in valid_data:
      self.rrd.save(e[0],e[1])

    min_el = valid_data.pop()
    el = (trunc_to_hour(min_el[0]),min_el[1])
    self.assertEqual([el], self.rrd.db.fetch_all('hours',wnd_24(epoch)))


class TestDB(unittest.TestCase):
  def setUp(self):
    self.rrd = RRD(db_name='test.db')
    self.rrd.db.reset('minutes')
    self.rrd.db.reset('hours')


  def test_insert(self):
    epoch = trunc_to_min(time.time())
    value = 10.0
    self.rrd.db.insert('minutes',(epoch,value))
    self.assertEqual(epoch,self.rrd.db.fetch_from_epoch('minutes', epoch)[1])





if __name__ == '__main__':
    unittest.main()
