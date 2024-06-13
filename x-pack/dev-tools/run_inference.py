import concurrent.futures
import json
import math
import random
import sched
import sys
import time

from elasticsearch import Elasticsearch

MODEL_ID = 'my-elser'

START_TIME = 5
RUN_TIME = 10*60
NUM_WORKERS = 1000

MAX_RATE = 60

# Get this with: wget https://www.mit.edu/~ecprice/wordlist.10000
words = list(w.strip() for w in open('wordlist.10000'))
start_time = time.time()

es = Elasticsearch(hosts='http://localhost:9200', basic_auth=('elastic', 'password'), connections_per_node=NUM_WORKERS)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS)
requests = []
stats = []

def run_query(request_id):
    request_time = time.time() - start_time
    query = ' '.join(random.choices(words, k=3))
    sys.stdout.write(f't={time.time() - start_time :.2f} request {request_id}: {query}\n')
    try:
        es.ml.infer_trained_model(model_id=MODEL_ID, docs=[{'text_field': query}])
        sys.stdout.write(f't={time.time()- start_time :.2f} response {request_id}: {query}\n')
        error = False
    except Exception as e:
        sys.stdout.write(f't={time.time()- start_time :.2f} error {request_id}: {query}: {e}\n')
        error = True
    response_time = time.time() - start_time
    return request_id, request_time, response_time, error


def submit_run_query(request_id):
    requests.append(executor.submit(run_query, request_id))


def collect_stats():
    request_time = time.time() - start_time
    response = es.ml.get_trained_models_stats(model_id=MODEL_ID).body
    stats = response['trained_model_stats'][0]['deployment_stats']
    num_allocations = stats['number_of_allocations']
    node_stats = stats['nodes'][0]
    inference_count = node_stats.get('inference_count', 0)
    average_inference_time_ms = node_stats.get('average_inference_time_ms', 0.0)
    number_of_pending_requests = node_stats.get('number_of_pending_requests', 0)
    data = (request_time, num_allocations, inference_count, average_inference_time_ms / 1000, number_of_pending_requests)
    sys.stdout.write(f't={time.time()- start_time :.2f} stats: {data}\n')
    return data


def submit_collect_stats():
    stats.append(executor.submit(collect_stats))


s = sched.scheduler(time.time, time.sleep)

def linear_increase(t):
    return 1 + MAX_RATE * t / RUN_TIME

def constant(t):
    return MAX_RATE

def oscillatory(t):
    return 1 + MAX_RATE * (1 - math.cos(2 * math.pi * t / (RUN_TIME))) / 2

for t in range(0, START_TIME + RUN_TIME):
    s.enterabs(start_time + t, 1, submit_collect_stats)

rate = linear_increase

t = 0
request_id = 0
while t < START_TIME + RUN_TIME:
    s.enterabs(start_time + START_TIME + t, 1, submit_run_query, argument=(request_id, ))
    t += 1 / rate(t)
    request_id += 1


s.run()

executor.shutdown(wait=True)

requests = [r.result() for r in requests]
stats = [s.result() for s in stats]

with open('data', 'w') as file:
    json.dump(requests, file)
    file.write('\n')
    json.dump(stats, file)
