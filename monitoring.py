from prometheus_client import Counter, Gauge, Histogram
import time, sys
from parameters import statsd


JQUEUER_JOB_ADDED 			= 'jqueuer_job_added'
JQUEUER_JOB_ADDED_TIMESTAMP = 'jqueuer_job_added_timestamp'
def add_job(experiment_id ,service_name, job_id):
	statsd.gauge(JQUEUER_JOB_ADDED_TIMESTAMP,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
		]
	)
	statsd.gauge(JQUEUER_JOB_ADDED,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
		]
	)

JQUEUER_TASK_ADDED 			= 'jqueuer_task_added'
JQUEUER_JOB_ADDED_TIMESTAMP = 'jqueuer_task_added_timestamp'
def add_task(experiment_id ,service_name, job_id, task_id):
	statsd.gauge(JQUEUER_JOB_ADDED_TIMESTAMP,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
			'task_id: %s' % task_id,
		]
	)
	statsd.gauge(JQUEUER_TASK_ADDED,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
			'task_id: %s' % task_id,
		]
	)


JQUEUER_SERVICE_REPLICAS_RUNNING = 'jqueuer_service_replicas_running'
def service_replicas_running(experiment_id ,service_name, service_replicas_running):
	statsd.gauge(JQUEUER_SERVICE_REPLICAS_RUNNING,
		service_replicas_running,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_SERVICE_REPLICAS_NEEDED = 'jqueuer_service_replicas_needed'
def service_replicas_needed(experiment_id ,service_name, service_replicas_needed):
	statsd.gauge(JQUEUER_SERVICE_REPLICAS_NEEDED,
		service_replicas_needed,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_SERVICE_REPLICAS_MIN = 'jqueuer_service_replicas_min'
def service_replicas_min(experiment_id ,service_name, service_replicas_min):
	statsd.gauge(JQUEUER_SERVICE_REPLICAS_MIN,
		service_replicas_min,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_SERVICE_REPLICAS_MAX = 'jqueuer_service_replicas_max'
def service_replicas_max(experiment_id ,service_name, service_replicas_max):
	statsd.gauge(JQUEUER_SERVICE_REPLICAS_MAX,
		service_replicas_max,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_SINGLE_TASK_DURATION = 'jqueuer_single_task_duration'
def single_task_duration(experiment_id ,service_name, single_task_duration):
	statsd.gauge(JQUEUER_SINGLE_TASK_DURATION,
		single_task_duration,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_EXPERIMENT_ACTUAL_START_TIMESTAMP = 'jqueuer_experiment_actual_start_timestamp'
def experiment_actual_start_timestamp(experiment_id ,service_name, experiment_actual_start_timestamp):
	statsd.gauge(JQUEUER_EXPERIMENT_ACTUAL_START_TIMESTAMP,
		experiment_actual_start_timestamp,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_EXPERIMENT_DEADLINE_TIMESTAMP = 'jqueuer_experiment_deadline_timestamp'
def experiment_deadline_timestamp(experiment_id ,service_name, experiment_deadline_timestamp):
	statsd.gauge(JQUEUER_EXPERIMENT_DEADLINE_TIMESTAMP,
		experiment_deadline_timestamp,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_EXPERIMENT_ACTUAL_END_TIMESTAMP = 'jqueuer_experiment_actual_end_timestamp'
def experiment_actual_end_timestamp(experiment_id ,service_name, experiment_actual_end_timestamp):
	statsd.gauge(JQUEUER_EXPERIMENT_ACTUAL_END_TIMESTAMP,
		experiment_actual_end_timestamp,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)

JQUEUER_EXPERIMENT_RUNNING_TIMESTAMP = 'jqueuer_experiment_running_timestamp'
def experiment_running_timestamp(experiment_id ,service_name, experiment_running_timestamp):
	statsd.gauge(JQUEUER_EXPERIMENT_RUNNING_TIMESTAMP,
		experiment_running_timestamp,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)
