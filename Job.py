import time
import datetime as dt 
class Job(object):
	"""docstring for Job"""
	@staticmethod
	def start_at(ts, fmt = '%Y-%m-%d %H:%M:%S'):
		start_time = dt.datetime.strptime(ts, fmt)
		unix_start_time = time.mktime(start_time.timetuple())
		diff = unix_start_time - time.time()
		hrs = int(diff/3600)
		m = (diff - hrs*3600)
		mins = int(m/60)
		sec = int(m%60)
		print("Job starts at {}".format(ts))
		print("{}:{}:{} left".format(hrs,mins,sec))
		while True:
			if time.time()>unix_start_time:
				break

	@staticmethod
	def wait_about(hr):
		t = time.time()+hr*3600
		print("Job starts at {}".format(dt.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')))
		time.sleep(hr*3600)

	@staticmethod
	def pause_between(start_hr, end_hr):
		hrs = list(range(0,24))
		h =[]
		if start_hr <end_hr:
			h = hrs[start_hr:end_hr]
		if start_hr>end_hr:
			h = hrs[start_hr:]+hrs[:end_hr]

		t = time.time()
		curr_hr = dt.datetime.fromtimestamp(t).hour

		while True:
			t = time.time()
			curr_hr = dt.datetime.fromtimestamp(t).hour
			if curr_hr not in h:
				break
			time.sleep(10)