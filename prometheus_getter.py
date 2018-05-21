import time, sys
import requests
from pprint import pprint

url = ""
worker_queries = [
	{'var': "jqueuer_worker_count", 				'query_str': "sum(jqueuer_worker_count)+by+(service_name)" },
	]

queries = [
	{'var': "jqueuer_task_added_count", 				'query_str': "count(jqueuer_task_added)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_running_count", 				'query_str': "sum(jqueuer_task_running)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_started_count", 				'query_str': "sum(jqueuer_task_started)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_accomplished_count", 			'query_str': "count(jqueuer_task_accomplished)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_accomplished_duration", 		'query_str': "avg(jqueuer_task_accomplished_duration)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_accomplished_duration_count", 	'query_str': "sum(jqueuer_task_accomplished_duration_count)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_task_accomplished_duration_sum", 	'query_str': "avg(jqueuer_task_accomplished_duration_sum)+by+(experiment_id,service_name)" },

	{'var': "jqueuer_job_added_count", 					'query_str': "count(jqueuer_job_added)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_running_count", 				'query_str': "sum(jqueuer_job_running)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_started_count", 				'query_str': "sum(jqueuer_job_started)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_accomplished_count", 			'query_str': "count(jqueuer_job_accomplished)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_accomplished_duration", 		'query_str': "avg(jqueuer_job_accomplished_duration)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_accomplished_duration_count", 	'query_str': "sum(jqueuer_job_accomplished_duration_count)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_accomplished_duration_sum", 	'query_str': "avg(jqueuer_job_accomplished_duration_sum)+by+(experiment_id,service_name)" },

	{'var': "jqueuer_job_failed_count", 				'query_str': "count(jqueuer_job_failed)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_failed_duration", 				'query_str': "avg(jqueuer_job_failed_duration)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_failed_duration_count", 		'query_str': "sum(jqueuer_job_failed_duration_count)+by+(experiment_id,service_name)" },
	{'var': "jqueuer_job_failed_duration_sum", 			'query_str': "avg(jqueuer_job_failed_duration_sum)+by+(experiment_id,service_name)" },
	]

def get(query):
	local_url =  url + "/api/v1/query?query=" + query
	try:
		return requests.get(local_url).json()
	except Exception as e:
		return {"status": "failed", "message":e}
	
def start(prometheus_protocol, prometheus_ip, prometheus_port, experiments):
	global url
	url = prometheus_protocol + "://" + prometheus_ip + ":" + str(prometheus_port)
	print("URL = " + url)
	while True:
		for query in worker_queries:
			try:
				resposne = get(query['query_str'])
				if ('data' not in resposne):
					continue

				for result in resposne['data']['result']:
					try:
						service_name = result['metric']['service_name']
						for experiment_id in experiments:
							try:
								experiments[experiment_id]['experiment'].update(query['var'], result)
							except Exception as e:
								print("A problem happened while updating %s worker in %s" % (query['var'] , str(experiment_id)))
								raise e
					except Exception as e:
						pprint(result)
			except Exception as ex:
				print("Error in " + str(query))
				raise ex

		for query in queries:
			try:
				#pprint(result)
				resposne = get(query['query_str'])
				experiment_id = None
				for result in resposne['data']['result']:
					try:
						experiment_id = result['metric']['experiment_id']
						experiments[experiment_id]['experiment'].update(query['var'], result)
					except Exception as e:
						print("A problem happened while updating %s jobs/tasks in %s with result %s" % (query['var'] , experiment_id, str(result)))
						#pprint(result)
						raise e
			except Exception as e:
				pass
				#print("Error in " + str(query))


		time.sleep(10)
#start("http", "178.22.69.24", 9090, None)