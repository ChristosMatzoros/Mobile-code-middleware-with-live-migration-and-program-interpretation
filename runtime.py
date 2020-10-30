from Color import Color
import socket
import struct
import pickle
import threading
import time
import sys
import os
import copy

MULTICAST_ADDRESS = "224.51.105.104"
MULTICAST_PORT = 3000
message_list = []
message_list_mutex = threading.Lock()
members_list = []
members_list_mutex = threading.Lock()
list_of_mailboxes = []
list_of_mailboxes_mutex = threading.Lock()
send_list = []
send_list_mutex = threading.Lock()
execution_states = []
execution_states_mutex = threading.Lock()
packet_list = []
packet_list_mutex = threading.Lock()
current_thread = 0
send_counter = 0
gw = os.popen("ip -4 route show default").read().split()
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect((gw[2], 0))
machine_ip = s.getsockname()[0]
my_unique_id = [machine_ip,sys.argv[1]]
# Create UDP socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp_socket.bind(('', MULTICAST_PORT))
mreq = struct.pack("=4sl", socket.inet_aton(MULTICAST_ADDRESS), socket.INADDR_ANY)
udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
team_id = [machine_ip,sys.argv[1],0]
end_flag = 0
terminate_flag = 0
terminate_mutex = threading.Lock()

def serialize(data):
	result = pickle.dumps(data)
	return result
def deserialize(data):
	result = pickle.loads(data)
	return result

def parse_id(input_id):
	sp = input_id[1:-1]
	comma = sp.split(",")
	l1 = comma[0][1:-1]
	l2 = comma[1][1:-1]
	l3 = int(comma[2])
	lista = []
	lista.append(l1)
	lista.append(l2)
	lista.append(l3)
	return(copy.deepcopy(lista))


def find_migs(msg_list):
	global my_unique_id
	weights_list = []
	for msg in msg_list:
		weights_list.append(msg[1])
	print("weights =",weights_list)
	avg = sum(weights_list)/len(weights_list)
	print("avg:",avg)
	differences_list = [(weight-avg) for weight in weights_list]
	print("differences_list:",differences_list)
	print("")

	total = 0
	for diff in differences_list:
		if(diff>0):
			total += diff

	for i in range(0,len(differences_list)):
		if(differences_list[i]>0):
			if(msg_list[i][0] == my_unique_id):
				print("The runtime",i,"will send:")
				for j in range(0,len(differences_list)):
					if(differences_list[j]<0):

						ip_addr = msg_list[j][0][0]
						port = msg_list[j][0][1]
						print("to runtime:",msg_list[j][0],":",end="")
						migration_number = -round(differences_list[j]/total*differences_list[i])
						print(" ",migration_number,"threads")

						execution_states_mutex.acquire()
						for i in range(0,migration_number):
							if(len(execution_states)>0):
								state = execution_states.pop(0)
								grp_id = state[0][0]
								thread_id = state[0][1]

								print(Color.F_Magenta,"grp_id:",grp_id,",thread_id:",thread_id,",ip_addr:",ip_addr,",port:",port,Color.F_Default)

								send_list_mutex.acquire()
								send_list.append(["MIGRATION",state,ip_addr,port])
								send_list_mutex.release()
						execution_states_mutex.release()
			print("")


#udp_listener listens to UDP multicast for messages
def multicast_listener():
	global udp_socket
	while True:
		msg,(client_ip, client_port) = udp_socket.recvfrom(1024)
		msg = deserialize(msg)
		app_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
		app_send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
		if(msg[1] == "TERMINATE"):
			print("TERMINATE MESSAGE")
			members_list_mutex.acquire()
			if(msg[0] in members_list):
				members_list.remove(msg[0])
			members_list_mutex.release()
			app_send_sock.sendto(serialize(["TERMINATE_ACK",my_unique_id]),(client_ip,client_port))

		elif(msg[1] == "DISCOVER"):
			print("DISCOVER MESSAGE")
			members_list_mutex.acquire()
			if(msg[0] not in members_list):
				members_list.append(msg[0])
			print(members_list)
			members_list_mutex.release()
			app_send_sock.sendto(serialize(["DISCOVER_ACK",my_unique_id]),(client_ip,client_port))
		elif((not isinstance(msg[1],int)) and len(msg[1])>1 and msg[1][0] == "MAILBOX"):
			print("MAILBOX MESSAGE")
			if(msg[1][3] == my_unique_id):
				list_of_mailboxes_mutex.acquire()
				for entry in msg[1][1]:
					list_of_mailboxes.append(copy.deepcopy(entry))
				list_of_mailboxes_mutex.release()
			execution_states_mutex.acquire()
			for state in execution_states:
				if(state[9] == msg[1][2]):
					state[9] = copy.deepcopy(msg[1][3])
			execution_states_mutex.release()
			app_send_sock.sendto(serialize(["ACK",my_unique_id]),(client_ip,client_port))
		else:
			message_list_mutex.acquire()
			if(msg not in message_list):
				message_list.append(msg)
			message_list_mutex.release()
			app_send_sock.sendto(serialize(["ACK",my_unique_id]),(client_ip,client_port))


#Send a message over UDP, including features such as acknowledgement receiving and resending
def multicast_sender(msg):
	global MULTICAST_ADDRESS
	global MULTICAST_PORT
	packet = serialize([my_unique_id,msg])
	members_list_mutex.acquire()
	list_len = len(members_list)
	members_list_mutex.release()

	app_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
	app_send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
	app_send_sock.settimeout(0.5)

	#Send the message using UDP
	app_send_sock.sendto(packet,(MULTICAST_ADDRESS,MULTICAST_PORT))

	start = time.time()
	#Receive acknowledgements from all the members of the group
	while(True):
		try:
			ack_msg,(addr,port) = app_send_sock.recvfrom(1024)
		except socket.timeout as e:
			end = time.time()
			#If the time has passed and the acks are yet to be received, resend the message
			if(end - start > 5):
				break
			continue
		ack_msg = deserialize(ack_msg)

		if(ack_msg[0] == "DISCOVER_ACK"):
			members_list_mutex.acquire()
			if(ack_msg[1] not in members_list):
				members_list.append(ack_msg[1])
			print(members_list)
			members_list_mutex.release()


def terminator():
	global end_flag
	global my_unique_id
	global terminate_flag
	global terminate_mutex
	members_list_mutex.acquire()
	members = copy.deepcopy(members_list)
	members_list_mutex.release()
	if(len(members) == 1):
		print("Shutdown Failed. There are no other runtimes.")
		return -1
	multicast_sender("TERMINATE")
	end_flag = 1


	execution_states_mutex.acquire()
	number_of_threads = len(execution_states)
	execution_states_mutex.release()

	members.remove(my_unique_id)

	print(members)
	num_of_members = len(members)

	execution_states_mutex.acquire()
	i = 0
	while(len(execution_states)>1):
		state = execution_states.pop(0)

		index = i%num_of_members
		member = copy.deepcopy(members[index])
		send_list_mutex.acquire()
		send_list.append(["MIGRATION",state,member[0],int(member[1])])
		send_list_mutex.release()
		i+=1

	if(len(execution_states)>0):
		state = execution_states.pop(0)
		index = i%num_of_members
		member = copy.deepcopy(members[index])
		send_list_mutex.acquire()
		send_list.append(["MIGRATION_FINAL",state,member[0],int(member[1])])
		send_list_mutex.release()
		execution_states_mutex.release()
	else:
		terminate_mutex.acquire()
		multicast_sender(["MAILBOX",list_of_mailboxes,my_unique_id,[members[0][0],str(members[0][1])]])
		terminate_flag = 1
		terminate_mutex.release()
		execution_states_mutex.release()
	return

def load_balancer():
	global message_list
	global message_list_mutex
	global end_flag
	multicast_sender("DISCOVER")
	time.sleep(1)
	if(end_flag == 1):
		return

	while True:
		if(end_flag == 1):
			return
		execution_states_mutex.acquire()
		num_of_states = len(execution_states)
		execution_states_mutex.release()
		multicast_sender(num_of_states)
		while True:
			if(end_flag == 1):
				return
			message_list_mutex.acquire()
			if(end_flag == 1):
				return

			members_list_mutex.acquire()
			if(end_flag == 1):
				return

			if(len(message_list) >= len(members_list)):
				print(Color.F_LightGreen,message_list,Color.F_Default)
				find_migs(message_list)
				message_list = copy.deepcopy([])
				members_list_mutex.release()
				message_list_mutex.release()
				break
			members_list_mutex.release()
			message_list_mutex.release()
		time.sleep(5)


def poll_thread():
	global machine_ip
	while(True):
		time.sleep(2)
		execution_states_mutex.acquire()
		for state in execution_states:
			send_list_mutex.acquire()
			sender_id = state[0]
			receiver_ip = state[9][0]
			receiver_port = state[9][1]
			#print(Color.F_LightBlue,"DO POLL:",["POLL",[sender_id,[machine_ip,sys.argv[1]]],receiver_ip,receiver_port],Color.F_Default)
			send_list.append(["POLL",[sender_id,[machine_ip,sys.argv[1]]],receiver_ip,receiver_port])
			send_list_mutex.release()

		execution_states_mutex.release()


def sender():
	global terminate_flag
	global terminate_mutex
	while True:
		send_list_mutex.acquire()
		while(not send_list):
			send_list_mutex.release()
			send_list_mutex.acquire()
		send_list_mutex.release()

		#Pop a message from the send_list
		send_list_mutex.acquire()
		packet = send_list.pop(0)
		send_list_mutex.release()

		sender_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		while True:
			try:
				sender_tcp_socket.close()
				sender_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sender_tcp_socket.connect((packet[2], int(packet[3])))
			except:
				continue


			if(packet[0] == "MIGRATION_FINAL"):
				packet[0] = "MIGRATION"
				msg = serialize([packet[0],packet[1],int(sys.argv[1])])
				sender_tcp_socket.send(msg)
				time.sleep(4)
				multicast_sender(["MAILBOX",list_of_mailboxes,my_unique_id,[packet[2], str(packet[3])]])
				terminate_mutex.acquire()
				terminate_flag = 1
				terminate_mutex.release()
				return

			msg = serialize([packet[0],packet[1],int(sys.argv[1])])
			sender_tcp_socket.send(msg)
			break

def tcp_listener():
	tcp_receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	tcp_receive_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	tcp_receive_socket.bind(('', int(sys.argv[1])))
	tcp_receive_socket.listen(1)

	while True:
		conn, (sender_ip, sender_port) = tcp_receive_socket.accept()
		msg = conn.recv(1024)
		if(msg == b''):
			continue

		msg = deserialize(msg)
		packet_list_mutex.acquire()
		packet_list.append(msg)

		packet_list_mutex.release()

		conn.close()

def packet_processor():
	while True:
		packet_list_mutex.acquire()
		while(not packet_list):
			packet_list_mutex.release()
			packet_list_mutex.acquire()
		packet_list_mutex.release()

		#Pop a message from the packet_list
		packet_list_mutex.acquire()
		packet = packet_list.pop(0)
		packet_list_mutex.release()

		state = packet[1]

		if(packet[0]=="MIGRATION"):
			print("MIGRATION",packet)
			execution_states_mutex.acquire()
			execution_states.append(state)
			execution_states_mutex.release()

		elif(packet[0]=="SND"):
			print("IT'S A SND MESSAGE")

			receiver_team_id = packet[1][0][0]
			receiver_member_id = packet[1][0][2]
			id = [receiver_team_id,receiver_member_id]

			flag = 0
			packet_tag = packet[1][1][0]
			if(packet_tag == "KILL_TEAM"):
				flag = 1
				list_of_mailboxes_mutex.acquire()
				for box in list_of_mailboxes:
					if(box[0][0] == id[0]):
						copy_packet = copy.deepcopy(packet)
						copy_packet[1][0][2] = copy.deepcopy(box[0][1])
						box[1].append(copy_packet)
				list_of_mailboxes_mutex.release()

			if(flag == 1):
				continue

			list_of_mailboxes_mutex.acquire()
			for box in list_of_mailboxes:
				if(box[0] == id):
					box[1].append(packet)
			list_of_mailboxes_mutex.release()


		elif(packet[0]=="RCV_ACK"):
			id = packet[1]
			execution_states_mutex.acquire()
			for recv_state in execution_states:
				if(recv_state[0] == id):
					recv_state[6] = 0
			execution_states_mutex.release()


		elif(packet[0]=="POLL"):
			id = packet[1][0]

			list_of_mailboxes_mutex.acquire()
			for box in list_of_mailboxes:
				if(box[0] == id):
					if(not box[1]):
						pass
					else:
						msg = copy.deepcopy(box[1])
						box[1] = []
						send_list_mutex.acquire()
						#print(Color.F_LightRed,"SEND BACK A POLL_ACK WITH THE MSG:",["POLL_ACK",msg,packet[1][1][0],int(packet[1][1][1])],Color.F_Default)
						print(msg[0][1][1][0])
						if(msg[0][1][1][0] == "KILL_TEAM"):
							list_of_mailboxes.remove(box)
						send_list.append(["POLL_ACK",msg,packet[1][1][0],int(packet[1][1][1])])
						send_list_mutex.release()
						break

			list_of_mailboxes_mutex.release()

		elif(packet[0]=="POLL_ACK"):
			print(Color.F_LightGreen,"RECEIVE POLL_ACK:",packet,Color.F_Default)

			msg = packet[1][0][1][1][0]
			print("msg",msg)
			print("packet",packet)

			for i in range(0,len(packet[1])):
				team_id = packet[1][i][1][0][0]
				member_id = packet[1][i][1][0][2]
				id = [team_id,member_id]

				flag = 0
				if(msg == "RCV_ACK"):
					flag = 1
					execution_states_mutex.acquire()
					for recv_state in execution_states:
						if(recv_state[0] == id):
							recv_state[6] = 0
							break
					execution_states_mutex.release()

				if(flag == 1):
					continue

				if(msg == "KILL_TEAM" or msg == "KILL_MEMBER"):
					flag = 1
					execution_states_mutex.acquire()
					for recv_state in execution_states:
						if(recv_state[0] == id):
							execution_states.remove(recv_state)
							flag = 1
							print(Color.F_LightCyan,"id:",id,"execution_states:",execution_states,Color.F_Default)
							break
					execution_states_mutex.release()

				if(flag == 1):
					continue

				execution_states_mutex.acquire()
				for recv_state in execution_states:
					if(recv_state[0] == id):
						member_id = packet[1][i][1][0][1]
						msg = packet[1][i][1][1]
						recv_state[7].append([member_id,msg])

				execution_states_mutex.release()

		else:
			print("IT IS SOMETHING ELSE")

multicast_listener_thread = threading.Thread(name = "Multicast Listener", target=multicast_listener)
multicast_listener_thread.daemon = True
multicast_listener_thread.start()

load_balancer_thread = threading.Thread(name = "Load Balancer", target=load_balancer)
load_balancer_thread.daemon = True
load_balancer_thread.start()

tcp_listener_thread = threading.Thread(name = "TCP Listener", target=tcp_listener)
tcp_listener_thread.daemon = True
tcp_listener_thread.start()

packet_processor_thread = threading.Thread(name = "Packet Processor", target=packet_processor)
packet_processor_thread.daemon = True
packet_processor_thread.start()

sender_thread = threading.Thread(name = "Sender", target=sender)
sender_thread.daemon = True
sender_thread.start()

poll_thread = threading.Thread(name = "Poll Thread", target=poll_thread)
poll_thread.daemon = True
poll_thread.start()


def runtime_manager():
	global team_id
	global execution_states
	global execution_states_mutex
	global machine_ip

	my_seqno = 0
	while(1):
		information = input().split()
		if(information[0] == "run"):
			information.pop(0)
			num_of_members = 0

			members = [[]]
			for value in information:
				if(value == "||"):
					members.append([])
					num_of_members+=1
				else:
					members[num_of_members].append(value)

			num_of_members+=1

			execution_states_mutex.acquire()

			for i in range(num_of_members):
				information = members[i]
				mailbox = [machine_ip,sys.argv[1]]

				process_id = [copy.deepcopy(team_id),i]
				file_name = information[0]
				argc = len(information)

				list_of_mailboxes_mutex.acquire()
				list_of_mailboxes.append([process_id,[]])	#[[id,[list_of_messages],[],[]..]
				list_of_mailboxes_mutex.release()

				variable_list = []
				variable_list.append(["$argc","int",argc])

				j=0
				for arg in information:
					variable_list.append(["$arg"+str(j),"str",arg])
					j+=1

				fp = open(file_name)
				row = fp.readline()
				line = row.strip()
				if((len(line.split())!=1) or (line!="#SIMPLESCRIPT")):
					remove_team_members(team_id)
					break

				list_of_labels = find_labels(fp)
				fp.seek(0,0)

				execution_states.append([process_id,file_name,0,variable_list.copy(),list_of_labels.copy(),[0,0],0,[],-1,mailbox])	#[ID, filename, PC, [[varname,type,value],[]..], [[label_name,line],..[]],[start_time,sleep_duration],send,[..recv buffer..],recv]]

			team_id[2]+=1
			execution_states_mutex.release()

		elif(information[0] == "list"):
			execution_states_mutex.acquire()
			for state in execution_states:
				print(Color.F_LightGreen,"Team id:",state[0][0],", Member id:",state[0][1],"Name:",state[1],Color.F_Default)
			execution_states_mutex.release()

		elif(information[0] == "kill"):
			kill_id = parse_id(information[1]+information[2]+information[3])
			execution_states_mutex.acquire()
			print("SEND KILL TEAM")
			found = 0
			for entry in execution_states:
				if(entry[0][0] == kill_id):
					print(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_TEAM"]],entry[9][0],entry[9][1]])
					send_list.append(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_TEAM"]],entry[9][0],entry[9][1]])
					break
			execution_states_mutex.release()

		elif(information[0] == "migrate"):
			grp_id = parse_id(information[1]+information[2]+information[3])
			thread_id = int(information[4])
			ip_addr= information[5]
			port = int(information[6])

			print(Color.F_Magenta,"grp_id:",grp_id,",thread_id:",thread_id,",ip_addr:",ip_addr,",port:",port,Color.F_Default)
			#check id it exists in the states_list
			id=[grp_id,thread_id]

			execution_states_mutex.acquire()

			found = 0
			for state in execution_states:
				if(state[0] == id):
					print(Color.F_Blue,state,Color.F_Default)
					found = 1
					break
			if(found == 0):
				print("NOT FOUND")
				continue
			send_state = copy.deepcopy(state)
			execution_states.remove(state)

			execution_states_mutex.release()

			send_list_mutex.acquire()
			send_list.append(["MIGRATION",send_state,ip_addr,port])
			send_list_mutex.release()

		elif(information[0] == "shutdown"):
			terminator()
		else:
			print("Wrong command. Please try again.")

runtime_manager_thread = threading.Thread(name = "Runtime Manager", target=runtime_manager)
runtime_manager_thread.daemon = True
runtime_manager_thread.start()

def PC_increment():
	for state in execution_states:
		if(state[0] == current_thread):
			state[2]+=1
	return "Not found"

def find_line():
	global current_thread
	for state in execution_states:
		if(state[0] == current_thread):
			fp = open(state[1])
			fp.seek(0,0)
			for i in range(state[2]):
				fp.readline()
			return(fp)
	return "Not found"

def jump(fp,labelname):
	global current_thread
	for state in execution_states:
		if(state[0] == current_thread):
			for label in state[4]:
				if(label[0] == labelname):
					state[2] = label[1]-1
					return
			return "Not found"
	return "Not found"

def find_labels(fp):
	list_of_labels = []
	counter = 2
	row = fp.readline()

	while row:
		line = row.strip()
		arguments = line.split()
		if(len(arguments)==0):
			pass
		else:
			command_tag = arguments[0]
			if(command_tag==''):
				pass
			elif(command_tag[0]=='#'):
				list_of_labels.append([command_tag,counter])

		row = fp.readline()
		counter += 1

	return(list_of_labels)

def remove_team_members(kill_id):
	global execution_states
	global current_thread


	print("SEND KILL TEAM")
	found = 0
	for entry in execution_states:
		if(entry[0][0] == kill_id):
			print(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_TEAM"]],entry[9][0],entry[9][1]])
			send_list.append(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_TEAM"]],entry[9][0],entry[9][1]])
			break
	# remove thread from execution_states
	for state in execution_states:
		if(state[0] == current_thread):
			execution_states.remove(state)
			break

	print("send_list:",send_list)

def search_variable(var_name):
	global current_thread
	for state in execution_states:
		if(state[0] == current_thread):
			for var in state[3]:
				if(var[0] == var_name):
					return var
			return "Not found"

def create_variable(var_name,type):
	global current_thread
	found=0
	for state in execution_states:
		if(state[0] == current_thread):
			state[3].append([var_name,type,0])
			return state[3][-1]

def find_variable_type(VarVal):
	global current_thread
	if(VarVal[0] == '$'):
		for state in execution_states:
			if(state[0] == current_thread):
				for var in state[3]:
					if(var[0] == VarVal):
						return [var[1],var[2]]
	elif(VarVal[0] == '\"'):
		return ["str",VarVal[1:-1]]
	else:
		return ["int",int(VarVal)]

	return([-1,-1])

def check_equality(list1,list2):
	global current_thread

	if(len(list1)!=len(list2)):
		return False

	list_length = len(list1)

	#check types
	for i in range(list_length):
		if(isinstance(list1[i],int)):
			type1 = "int"
		else:
			type1 = "str"

		type2,value2 = find_variable_type(list2[i])
		if(type2 == -1):
			variable = create_variable(list2[i],type1)
			variable[2] = list1[i]
			type2,value2 = find_variable_type(list2[i])
		if(type1!=type2):
			return False

	#check values
	for i in range(list_length):
		if(list2[i][0] != '$'):
			type2,value2 = find_variable_type(list2[i])
			if(list1[i]!=value2):
				return False

	#save values to variables
	for i in range(list_length):
		if(list2[i][0] == '$'):
			variable = search_variable(list2[i])
			variable[2] = list1[i]

	return True

while True:
	execution_states_mutex.acquire()
	terminate_mutex.acquire()
	if(terminate_flag == 1):
		terminate_mutex.release()
		print("TERMINATING")
		sys.exit(1)
	terminate_mutex.release()
	for entry in execution_states:

		terminate_mutex.acquire()
		if(terminate_flag == 1):
			terminate_mutex.release()
			print("TERMINATING")
			sys.exit(1)
		terminate_mutex.release()
		current_thread = entry[0]
		if((time.time() < entry[5][0] + entry[5][1]) or (entry[6]==1)):
			continue
		if(entry[8] != -1):
			msg_found = 0
			for msg in entry[7]:
				if(entry[8][0] == msg[0] and check_equality(msg[1],entry[8][1])):
					msg_found = 1
					print(Color.F_LightCyan,"Received message:", msg, Color.F_Default)
					found = 0
					for sender in execution_states:
						if(sender[0] == [entry[0][0],msg[0]]):
							sender[6] = 0
							found=1
							break

					if(found == 0):
						send_list_mutex.acquire()
						send_list.append(["SND",[[entry[0][0],entry[0][1],msg[0]],["RCV_ACK"]],entry[9][0],entry[9][1]])
						send_list_mutex.release()
					entry[7].remove(msg)
					entry[8] = -1
					break
			if(msg_found == 0):
				continue

		fp = find_line()
		if (fp == "Not found"):
			print("File not found")
			#delete from list
		row = fp.readline()
		if(not row):
			print(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_MEMBER"]],entry[9][0],entry[9][1]])
			send_list.append(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_MEMBER"]],entry[9][0],entry[9][1]])
			execution_states.remove(entry)
			break

		line = row.strip()
		arguments = line.split()
		length = len(arguments)
		i=0
		while(i < length):
			if(arguments[i] == '' or arguments[i] == '\t'):
				print("arg",arguments[i])
				del(arguments[i])
				length -= 1
			else:
				i+=1

		if(len(arguments) == 0):
			PC_increment()
			continue
		if(arguments[0][0]=='#'):
			arguments.pop(0)
		if(len(arguments) == 0):
			PC_increment()
			continue
		length = len(arguments)
		i=0
		while(i < length):
			while(arguments[i][0] == '\"' and arguments[i][-1] != '\"'):
				arguments[i] += " " + arguments [i+1]
				del(arguments[i+1])
				length -= 1
			i+=1

		command_tag = arguments[0]

		if(command_tag == "SET"):
			if(len(arguments)!=3):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				continue
			Var = arguments[1]
			VarVal = arguments[2]

			type,VarValValue = find_variable_type(VarVal)
			if(type==-1):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1)
				continue

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type)

			variable[2] = VarValValue

			PC_increment()

		elif(command_tag == "ADD"):
			if(len(arguments)!=4):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				break

			Var = arguments[1]
			VarValI1 = arguments[2]
			VarValI2 = arguments[3]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				break

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type1)

			variable[2] = VarValI1Value + VarValI2Value

			PC_increment()

		elif(command_tag == "SUB"):
			if(len(arguments)!=4):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				break

			Var = arguments[1]
			VarValI1 = arguments[2]
			VarValI2 = arguments[3]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				break

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type1)

			variable[2] = VarValI1Value - VarValI2Value

			PC_increment()

		elif(command_tag == "MUL"):
			if(len(arguments)!=4):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				break

			Var = arguments[1]
			VarValI1 = arguments[2]
			VarValI2 = arguments[3]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				break

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type1)

			variable[2] = VarValI1Value * VarValI2Value

			PC_increment()

		elif(command_tag == "DIV"):
			if(len(arguments)!=4):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				break

			Var = arguments[1]
			VarValI1 = arguments[2]
			VarValI2 = arguments[3]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				remove_team_members(entry[0][0])
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				break

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type1)

			variable[2] = VarValI1Value // VarValI2Value

			PC_increment()

		elif(command_tag == "MOD"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			Var = arguments[1]
			VarValI1 = arguments[2]
			VarValI2 = arguments[3]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			variable = search_variable(Var)
			if(variable == "Not found"):
				variable = create_variable(Var,type1)

			variable[2] = VarValI1Value % VarValI2Value

			PC_increment()

		elif(command_tag == "BGT"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarValI1 = arguments[1]
			VarValI2 = arguments[2]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[3][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(VarValI1Value>=VarValI2Value):
				res = jump(fp,arguments[3])
				if(res == "Not found"):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
					remove_team_members(entry[0][0])
					break
			else:
				PC_increment()

		elif(command_tag == "BGE"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarValI1 = arguments[1]
			VarValI2 = arguments[2]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[3][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(VarValI1Value>VarValI2Value):
				res = jump(fp,arguments[3])
				if(res == "Not found"):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
					remove_team_members(entry[0][0])
					break
			else:
				PC_increment()

		elif(command_tag == "BLT"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarValI1 = arguments[1]
			VarValI2 = arguments[2]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[3][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(VarValI1Value<VarValI2Value):
				res = jump(fp,arguments[3])
				if(res == "Not found"):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
					remove_team_members(entry[0][0])
					break
			else:
				PC_increment()

		elif(command_tag == "BLE"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarValI1 = arguments[1]
			VarValI2 = arguments[2]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[3][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(VarValI1Value<=VarValI2Value):
				res = jump(fp,arguments[3])
				if(res == "Not found"):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
					remove_team_members(entry[0][0])
					break
			else:
				PC_increment()

		elif(command_tag == "BEQ"):
			if(len(arguments)!=4):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarValI1 = arguments[1]
			VarValI2 = arguments[2]

			type1,VarValI1Value = find_variable_type(VarValI1)
			type2,VarValI2Value = find_variable_type(VarValI2)

			if(type1!="int" or type2!="int"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong type of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[3][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(VarValI1Value==VarValI2Value):
				res = jump(fp,arguments[3])
				if(res == "Not found"):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
					remove_team_members(entry[0][0])
					break
			else:
				PC_increment()

		elif(command_tag == "BRA"):
			if(len(arguments)!=2):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			if(arguments[1][0]!='#'):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Labels must start with '#' letter",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			label = arguments[1]
			res = jump(fp,label)
			if(res == "Not found"):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong Label",Color.F_Default)
				remove_team_members(entry[0][0])
				break

		elif(command_tag == "SND"):
			if(len(arguments)<2):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			list_of_msg = []

			flag = 0
			for i in range (1,len(arguments)):
				VarVal = arguments[i]
				type,value = find_variable_type(VarVal)
				if(type==-1):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1, Color.F_Default)
					remove_team_members(entry[0][0])
					flag = 1
					break
				if(i ==1):
					member_id = value
				else:
					list_of_msg.append(value)

			if(flag==1):
				break

			entry[6] = 1

			my_team_id = entry[0][0]
			my_member_id = entry[0][1]
			found = 0
			for recv_state in execution_states:
				if(recv_state[0] == [my_team_id,member_id]):
					recv_state[7].append([my_member_id,list_of_msg])
					found = 1

			if(found == 0):
				send_list_mutex.acquire()
				send_list.append(["SND",[[my_team_id,my_member_id,member_id],list_of_msg],entry[9][0],entry[9][1]])
				send_list_mutex.release()

			PC_increment()


		elif(command_tag == "RCV"):
			if(len(arguments)<2):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			list_of_msg = []
			flag = 0
			for i in range (1,len(arguments)):
				VarVal = arguments[i]
				if(i == 1):
					type,value = find_variable_type(VarVal)
					sender_id = value
				else:
					list_of_msg.append(VarVal)

			entry[8] = [sender_id,list_of_msg]

			PC_increment()


		elif(command_tag == "SLP"):
			if(len(arguments)!=2):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			VarVal = arguments[1]
			type,value = find_variable_type(VarVal)
			entry[5]=[time.time(),value]
			PC_increment()

		elif(command_tag == "PRN"):
			if(len(arguments)<2):
				print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong number of arguments",Color.F_Default)
				remove_team_members(entry[0][0])
				break

			print(Color.F_LightYellow,"Team " + str(entry[0][0]) + ", Process " + str(entry[0][1]), ":",Color.F_Default, end=" ")
			flag = 0
			for i in range (1,len(arguments)):
				VarVal = arguments[i]
				type,value = find_variable_type(VarVal)
				if(type==-1):
					print(Color.F_LightRed,"Syntax error at line",entry[2]+1, Color.F_Default)
					remove_team_members(entry[0][0])
					flag = 1
					break
				print(Color.F_LightYellow, value, Color.F_Default, end=" ")
			print("")
			if(flag == 1):
				break

			PC_increment()

		elif(command_tag == "RET"):

			send_list.append(["SND",[[entry[0][0],entry[0][1],entry[0][1]],["KILL_MEMBER"]],entry[9][0],entry[9][1]])
			execution_states.remove(entry)
			print(Color.F_LightRed,"RETURN",Color.F_Default)
			break

		else:
			print(Color.F_LightRed,"Syntax error at line",entry[2]+1,":Wrong command",Color.F_Default)
			remove_team_members(entry[0][0])
			continue

	execution_states_mutex.release()
