import pandas as pd
from kafka import KafkaProducer
from random import randint
from time import sleep
import sys

df=pd.read_csv('ChicagoCrimes_2008_to_2011.csv')
df=df.rename({'Unnamed: 0':'Index'},axis='columns')


broker='10.2.6.206:9092'
topic='chicago_crimes'

try:                                                                                                                    
    p = KafkaProducer(bootstrap_servers=broker)                                                                         
except Exception as e:                                                                                                  
    print("ERROR -->"+str(e))                                                                                         
    sys.exit(1)  

print('Succesfull connection to broker')

seq = 0
ln=len(df.index)
while True:
	dest=seq+randint(2,7)
	message=df[seq:dest]
	for i in message.itertuples(index=False):
		p.send(topic,bytes(str(i)))
		print(i)
	seq=dest
	if(dest>=ln-1):
		break
	sleep(randint(1,5))
