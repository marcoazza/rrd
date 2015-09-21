def trunc_to_min(epoch):
  epoch = epoch - (epoch % 60)
  return epoch

def trunc_to_hour(epoch):
  epoch = epoch - (epoch % 3600)
  return epoch


def wnd_1(epoch):
  start_h = epoch - (epoch % 60)
  end_h = start_h - 3600
  return start_h, end_h

def wnd_24(epoch):
  start_d = epoch - (epoch % 60)
  end_d = start_d - (3600 * 24)
  return start_d, end_d
