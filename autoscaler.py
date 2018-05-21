from threading import Thread

import job_manager
#import prometheus_getter
import experiment_receiver

from experiment import Experiment 

http_server_port	= 8081
#prometheus_protocol = 'http'
#prometheus_ip       = 'prometheus'
#prometheus_port     = 9090

experiments = {}

if __name__ == '__main__':

	prometheus_getter_thread = Thread(target = prometheus_getter.start, 
		args = (prometheus_protocol, prometheus_ip, prometheus_port, experiments,)
		)
	prometheus_getter_thread.start()
	
	experiment_receiver_thread = Thread(target = experiment_receiver.start, args = (experiments,http_server_port,))
	experiment_receiver_thread.start()