	
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta
class Date:
	"""docstring for Date"""
	
	def _to_dt(dt):
		if type(dt) is int:
			dt = str(dt)
		return date(int(dt[:4]),int(dt[4:6]),int(dt[6:8]))

	@staticmethod
	def add(date,days):
		date = Date._to_dt(date)
		return (date + timedelta(days = days)).strftime("%Y%m%d")
	@staticmethod
	def subtract(date,days):
		date = Date._to_dt(date)
		return (date - timedelta(days = days)).strftime("%Y%m%d")

	@staticmethod
	def month_diff(yyyymm,offset):
		if type(yyyymm) is int:
			yyyymm = str(yyyymm)
		year = yyyymm[:4]
		month = yyyymm[4:6]
		if offset >0:
			for o in range(1,offset+1):
				ym  = date(int(year), int(month), 1)
				next_month = ym.replace(day=28) + timedelta(days=4)
				year = next_month.year
				month =next_month.month
			return next_month.strftime("%Y%m")
		elif offset ==0:
			return yyyymm
		else:
			for o in range(1,abs(offset)+1):
				ym  = date(int(year), int(month), 1)
				prev_month = ym - timedelta(days = 1)
				year = prev_month.year
				month =prev_month.month
			return prev_month.strftime("%Y%m")



	@staticmethod
	def last_day(yyyymm):
		if type(yyyymm) is int:
			yyyymm = str(yyyymm)
		year = yyyymm[:4]
		month = yyyymm[4:6]
		ym  = date(int(year), int(month), 1)

		next_month = ym.replace(day=28) + timedelta(days=4)
		return (next_month - timedelta(days=next_month.day)).strftime("%Y%m%d")
	@staticmethod
	def first_day(yyyymm):
		if type(yyyymm) is int:
			yyyymm = str(yyyymm)
		year = yyyymm[:4]
		month = yyyymm[4:6]
		ym  = date(int(year), int(month), 1)
		return ym.strftime("%Y%m%d")		

	@staticmethod
	def between(start_date,end_date):
		start_dt = Date._to_dt(start_date)
		end_dt = Date._to_dt(end_date)
		date_range =[]
		for n in range(int ((end_dt - start_dt).days)+1):		
			date_range.append((start_dt + timedelta(n)).strftime("%Y%m%d"))
		return date_range
	@staticmethod
	def is_weekend(dt):
		dt = Date._to_dt(dt)
		#Return the day of the week as an integer, where Monday is 0 and Sunday is 6.
		if dt.weekday() >=5:
			return 'Y'
		return 'N'
	@staticmethod
	def get_dow(dt):
		dt = Date._to_dt(dt)
		weekday = ['월','화','수','목','금','토','일']
		return weekday[dt.weekday()]		