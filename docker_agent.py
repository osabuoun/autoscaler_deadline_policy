from pprint import pprint
import shlex, subprocess, json

def create(image_url, service_name, replicas, stop_grace_period, reserve_memory, reserve_cpu):
	output = ""
	try:
		output = subprocess.check_output(
			[
			'docker','service', 'create', '-t', 
			'--name', service_name,
			'--replicas', str(replicas),
			'--stop-grace-period', stop_grace_period,
			'--reserve-memory', reserve_memory,
			'--reserve-cpu', reserve_cpu,
			'--log-driver', 'syslog',
			'--log-opt', 'syslog-address=udp://127.0.0.1:7514',
			'--rollback-parallelism', "1",
			'--update-parallelism', "1", 
			image_url
			]
		)

	except Exception as e:
		output = "Error happened : " + str(e)
	print(output)
	return output

def remove(service_name):
	output = ""
	try:
		output = subprocess.check_output(['docker','service', 'rm', service_name])
	except Exception as e:
		output = "Error happened : " + str(e)
	print(output)
	return output

def scale(service_name, replicas):
	output = ""
	try:
		output = subprocess.check_output(['docker','service', 'scale', service_name + '=' + str(replicas)])
	except Exception as e:
		output = "Error happened : " + str(e)
	print(output)
	return output

def replicas():
	output = ""
	try:
		output = subprocess.check_output(['docker','service', 'ls', '-q'])
		replicas_list = {}
		services = str(output,'utf-8').splitlines()
		for service in services:
			output = subprocess.check_output(['docker','service', 'inspect', service])
			data_json = json.loads(str(output,'utf-8'))
			service_id = data_json[0]['ID']
			service_name = data_json[0]['Spec']['Name']
			replicas = data_json[0]['Spec']['Mode']['Replicated']['Replicas']
			replicas_list[service_name] = {'id': service_id, 'replicas':replicas}
			
	except Exception as e:
		pass
	#print(output)
	return replicas_list

def replicas(service_name):
	output = ""
	result = 0
	try:
		output = subprocess.check_output(['docker','service', 'inspect', service_name])
		data_json = json.loads(str(output,'utf-8'))
		service_id = data_json[0]['ID']
		service_name = data_json[0]['Spec']['Name']
		replicas = data_json[0]['Spec']['Mode']['Replicated']['Replicas']
		result = replicas		
	except Exception as e:
		pass
	return result

#create('osabuoun/python_echo_app', 'test_name' , 3)
#print(scale('test_name',6))
#print(replicas('test_name1'))
