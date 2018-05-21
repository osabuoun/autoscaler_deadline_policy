import redis
JOB_QUEUE_PREFIX 	= 	'jqueue_service_'
broker_protocol		= 	'pyamqp'
#broker_username 	= 	'guest'
broker_username 	= 	'admin'
#broker_password 	= 	''
broker_password 	= 	'mypass'
#broker_server		=	'192.168.253.1'
broker_server		=	'rabbit'
broker_port 		= 	5672

def broker():
	broker 	= 	broker_protocol + '://' + broker_username 
	if (broker_password != ''):
		broker 	= broker + ':' + broker_password
	broker 	= 	broker + '@' + broker_server + ':' + str(broker_port) + '//'
	return broker

backend_protocol 			= 	'redis'
#backend_server	 			=	'192.168.253.1'
backend_server	 			=	'redis'
backend_port     			=	6379
backend_db		 			=	0
backend_experiment_db_id	=	10

backend_experiment_db = redis.StrictRedis(
	host=backend_server, 
	port=backend_port, 
	db=backend_experiment_db_id, 
	charset="utf-8", 
	decode_responses=True
	)
 	
def backend(db):
	backend = backend_protocol + '://' + backend_server + ':' + str(backend_port) + '/' + str(db)
	return backend


#from datadog import DogStatsd

#STASTD_SERVER 	= '192.168.253.1'
STATSD_SERVER 	= 'statsd'
STATSD_PORT		= 9125


#statsd = DogStatsd(host=STATSD_SERVER, port=STATSD_PORT)

STATSD_OPTIONS = {
    'api_key':'jqueuer_api_key',
    'app_key':'jqueuer_app_key',
    'statsd_host': STATSD_SERVER,
    'statsd_port': STATSD_PORT
}

from datadog import initialize

initialize(**STATSD_OPTIONS)

from datadog import statsd
