# -*- coding: utf-8 -*-
"""
This mqtt writer creates various documents and respetive
writer and readers to manage the data stream from an mqtt server

This programm requires the paho-mqtt package install with the pip commands: pip install paho-mqtt
"""
import time
import csv
import json
from collections import deque
import thread
from os import listdir, getcwd
from os.path import isfile, join
import paho.mqtt.client as paho


server = "localhost" #server address
port = 8134 #port of the server
mainTopic="trolly" # topic of the study. Everything that is published in this type is written and saved
foldername = "pilot" #Not Used yet
timeKEY='time' #
valueNameKEY = 'valueName'
valueKEY = 'value'
targetPath = getcwd() + "/"+foldername

onlyfiles = [f for f in listdir(targetPath) if isfile(join(targetPath, f))]
#print(onlyfiles)
queue = deque([]) # the shared queue between the mqtt thread and the main filewriting threat
"""
The following class handles one uuid i.e. one client. its essential task is to keep a certain file open and write any new data to it
It will up date the top row every time a new variable time is recieved.
//ToDo ->check if a certain uuid already exsists and if so copy that file to a sub folder befor overwriting its
//     -> why is this still open: it is not likley that two computers generate the same uuid but still!

The new data function likes to recieve a jason object as its argument. This jason object should contain three components.
Time in form of a float
valueName in form of a string (this string is used to locate the right colom for the Value)
value in form of a float

The jason object is converted into a dictionary the keys to recieve the right values from this dictionary can be set at the top of the
program with the variables timeKEY  valueNameKEY and valueKEY

other important notes are:
->it is self distructive i.e. after a set time of not recieving new data it kills it self.

"""
class uuidHandler:
	def __init__(self,uuid,keepalive):
		self.uuid = uuid #assosiating this handler with a certain uuid
		self.timer = current_milli_time() #remembering the start time
		self.keepalive = keepalive*60*1000 #calculating the milliseconds that we have to stay allive for after the last message
		self.file = open(str(self.uuid)+".csv","wr+") # createa file based on the uuid
		self.writer = csv.writer(self.file) #create the writer we are going to use for this file
		self.keyList = [timeKEY] #we know that the first thing in list is going to be the time key
		self.outArray = [] #
		self.t = 0
		self.prevLine = []
		self.newTime = True
		self.counter = 0 #we want to keep track of how many messages we recieved so that we can compar that with the send-count
	def newData(self,jsondata):
		self.counter+=1
		self.timer=current_milli_time()
		key = jsondata[valueNameKEY] #get the value name of the value we just recieved
		if(key not in self.keyList): # if its not in the list we:
			self.keyList.append(str(key)) #					a) append it
			self.file.seek(0,0) 	#						b) go to the top of the file
			self.file.write(','.join(self.keyList)+'\n') # 	c) rewrite the first line with the value names
			self.file.seek(0,2)     # 						d) jump back down
		value = str(jsondata[valueKEY])
		valueID = self.keyList.index(key)
		valueTime = str(jsondata[timeKEY])
		if(self.newTime): # if the time in the previous commands did not match we:
			self.newTime=False							# a)set our reminder flag to false
			self.prevLine=(len(self.keyList)+1)*[' ']	# b)clear the previous data
			self.prevLine[0]=valueTime					# c)remeber the new data
			#Befor we write a certain line we extend any string from a certain time
		if(valueTime == self.prevLine[0]):  # checking if we are still in the same time frame
			if((len(self.prevLine)-1)<valueID): #we either append or pick our place for the value
				self.prevLine.append(value)   # this is a bit dirty this assumes that a new key is always added at the back. its doing that but we should still check
			else:
				self.prevLine[valueID]=value # this is much better, we place the value in to its position
		else:   			# if we have a new time frame we:
			self.file.write(','.join(self.prevLine)+'\n') #	a) write away theold line
			self.prevLine=(len(self.keyList)+1)*[' '] 	#	b) clear the value line
			self.prevLine[0]=valueTime					#	c) add the time in the first element
			self.prevLine[valueID]=value 				#	d) store the value in its right place
			self.file.flush()							# 	e) flush the file to the disk(takes time but is safer in case of a crash)
	def close(self):  # called when the timer runs out
		self.file.write(','.join(self.prevLine)+'\n') # write the last line of data
		self.file.flush()  # flush the data
		self.file.close()  # close the file writer
	def update(self):
		if(self.timer+self.keepalive<current_milli_time()): #checking if we have reached the maximum time
			print ("realising "+self.uuid+"with %i messages recieved" % self.counter) #reporting to the console that this class selfd istrcts
			self.close()
			return True #reporting back to the main loop so that it is taken out of the dictionary
		else:
			return False



# the following function is there to calculate the running time of each uuid handler so they can sfigour out when to self distruct
current_milli_time = lambda: int(round(time.time() * 1000))


"""
The following three fuctions and the final thread.start function run the mqtt subscriber
The subscriber runs in a seperat thread so that both possible blocking commands i.e.
data_writing and network_data_receiving do not block the complete process.

"""
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))

def on_message(client, userdata, msg):
	queue.append(msg.topic+"_"+str(msg.payload))
	#print(str(msg.payload))
def run_mqtt_client():
	client = paho.Client()
	client.on_subscribe = on_subscribe
	client.on_message = on_message
	client.connect(server, port)
	client.subscribe(mainTopic+"/#", qos=1)
	client.loop_forever()
def checkspace(uuid):
	if(onlyfiles.count(uuid+".csv")>0):
		return True
	else:
		return False
thread.start_new_thread(run_mqtt_client, ())


"""
This main loop handles the file write. It essentially deques and messages recieved by the mqtt thread and hands them to the
appropriate uuidHandler and creates a new uuidHandler if it is not yet created.




"""
write_header=True
item_keys = []
handlerDict={}
while(1):
	print("---New Loop with  %i  active handler(s)---" % int(len(handlerDict)) )
	while(len(queue)>0):
		message=queue.popleft()  #get the oldest message
		uuid=message.split('_')[0].split('/')[1] # seperate out the uuid
		if(uuid not in handlerDict): # if we do not already have it we create a uuidHandler
			temp = uuidHandler(uuid,1) #the new uuid and a wait time of one minute
			handlerDict[uuid]=temp # add the handler to the Dictionary
			print("opening for: "+str(handlerDict[uuid].uuid)) #report to the user
		handlerDict[uuid].newData(json.loads(message.split('_')[1])) #push back the new data independed if its created new or not
	keyDelet=[] # array to store uuid that have self destructed
	for key in handlerDict: #calling update on all uuidHandlers
		if(handlerDict[key].update()):
			keyDelet.append(key) # if a uuidHanlder returns true it self destructed so we add his key to the list
	for key in keyDelet:   # deleting self destrcuted uuidHandlers
		del handlerDict[key]
	time.sleep(3)
