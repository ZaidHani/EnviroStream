from confluent_kafka import Producer
import time
import glob

if __name__ == "__main__":
    files = glob.glob('data/machine_2/*json')
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    
    for file in files:
        f = open(file)
        for line in f:
            p.produce('machine-2', line)
            time.sleep(0.5)
        f.close()