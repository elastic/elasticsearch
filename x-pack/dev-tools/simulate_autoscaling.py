from collections import deque, defaultdict
import enum
import heapq
import math
import random
import matplotlib.pyplot as plt

START_TIME = 0
END_TIME = 5*3600

DECAY_TIME = 60
AUTO_SCALE_UP_THRESHOLD = 0.9
AUTO_SCALE_DOWN_THRESHOLD = 0.8

AVG_INFERENCE_TIME = 0.1
PHYSICAL_CORES_PER_NODE = 4


class EventType(enum.IntEnum):
  ALLOCATION_STARTED = 1
  REQUEST = 2
  INFERENCE_COMPLETED = 3


def linear_increasing(time):
  return 20 * time / END_TIME

def oscallating_2h(time):
  return 100 * (1 - math.cos(2 * math.pi * time / (2*3600)))

def oscallating_5h(time):
  return 1 * (1 - math.cos(2 * math.pi * time / (5*3600)))


get_request_rate = oscallating_2h


def get_avg_inference_time(num_allocations):
  physical_cores = num_allocations // (2 * PHYSICAL_CORES_PER_NODE) * PHYSICAL_CORES_PER_NODE
  remaining_allocations = num_allocations % (2 * PHYSICAL_CORES_PER_NODE)
  if remaining_allocations < PHYSICAL_CORES_PER_NODE:
    physical_cores += remaining_allocations
  else:
    physical_cores += PHYSICAL_CORES_PER_NODE
  return AVG_INFERENCE_TIME * num_allocations / physical_cores


def get_inference_time(num_allocations):
  return random.uniform(0.5, 1.5) * get_avg_inference_time(num_allocations)


print('simulating traffic...')

events = []  # contains tuple of (time, type, allocation_id, inference_time)

# initial allocations
num_allocations = 1
for alloc_id in range(num_allocations):
  heapq.heappush(events, (0, EventType.ALLOCATION_STARTED, alloc_id, None))

# create all requests
TIME_STEP = 0.001
time = START_TIME + TIME_STEP / 2
while time < END_TIME:
  if random.random() < get_request_rate(time) * TIME_STEP:
    heapq.heappush(events, (time, EventType.REQUEST, None, None))
  time += TIME_STEP

# estimators / moving averages
sum_request_count = 0
sum_request_count_last_update = 0
sum_inference_count = 0
sum_inference_time = 0
sum_inference_last_update = 0

available_allocations = set()
inference_queue = deque()  # contains request time

# data for the graphs
est_request_counts = []
real_request_counts = []
est_inference_times = []
real_inference_times = []
wait_times = []
queue_sizes = []
num_allocations_list = []
num_ml_nodes = []

last_data_time = 0

while events:
  # handle events
  time, type, alloc_id, inference_time = heapq.heappop(events)
  if time > END_TIME:
    break

  if type == EventType.ALLOCATION_STARTED:
    available_allocations.add(alloc_id)

  elif type == EventType.REQUEST:
    decay = math.exp(-(time - sum_request_count_last_update) / DECAY_TIME)
    sum_request_count = decay * sum_request_count + 1
    sum_request_count_last_update = time
    inference_queue.append(time)

  elif type == EventType.INFERENCE_COMPLETED:
    decay = math.exp(-(time - sum_inference_last_update) / DECAY_TIME)
    sum_inference_count = decay * sum_inference_count + 1
    sum_inference_time = decay * sum_inference_time + inference_time
    sum_inference_last_update = time
    if alloc_id < num_allocations:
      available_allocations.add(alloc_id)

  while inference_queue and available_allocations:
    request_time = inference_queue.popleft()
    wait_time = time - request_time
    wait_times.append((time, wait_time))
    alloc_id = available_allocations.pop()
    inference_time = get_inference_time(num_allocations)
    heapq.heappush(events, (time + inference_time, EventType.INFERENCE_COMPLETED, alloc_id, inference_time))

  # collect data
  collect_data = time > last_data_time + 1
  if collect_data:
    last_data_time = time

  if collect_data:
    queue_sizes.append((time, len(inference_queue)))
    num_allocations_list.append((time, num_allocations))
    num_ml_nodes.append((time, math.ceil(num_allocations / (2 * PHYSICAL_CORES_PER_NODE))))

  total_weight = DECAY_TIME * (1 - math.exp(-(time - START_TIME) / DECAY_TIME))
  if not total_weight:
    continue

  est_request_count = sum_request_count * math.exp(-(time - sum_request_count_last_update) / DECAY_TIME) / total_weight
  if collect_data:
    est_request_counts.append((time, est_request_count))
    real_request_counts.append((time, get_request_rate(time)))

  est_inference_count = sum_inference_count * math.exp(-(time - sum_inference_last_update) / DECAY_TIME) / total_weight
  if not est_inference_count:
    continue

  est_inference_time = sum_inference_time * math.exp(-(time - sum_inference_last_update) / DECAY_TIME) / total_weight / est_inference_count
  if collect_data:
    est_inference_times.append((time, est_inference_time))
    real_inference_times.append((time, get_avg_inference_time(num_allocations)))

  # autoscaling
  needed_allocations = est_request_count * est_inference_time
  while needed_allocations > AUTO_SCALE_UP_THRESHOLD * num_allocations:
    heapq.heappush(events, (time, EventType.ALLOCATION_STARTED, num_allocations, None))
    num_allocations += 1
    print(time, f'autoscale up to {num_allocations} (wanted={needed_allocations})')

  while num_allocations > 1 and needed_allocations < AUTO_SCALE_DOWN_THRESHOLD * (num_allocations - 1):
    num_allocations -= 1
    print(time, f'autoscale down to {num_allocations} (wanted={needed_allocations})')


# bucket the wait times
BUCKET_SIZE = 60
bucketed_wait_times = defaultdict(list)
for time, wait in wait_times:
  bucketed_wait_times[time // BUCKET_SIZE * BUCKET_SIZE].append(wait)

max_wait_time = [(time, max(waits)) for time, waits in bucketed_wait_times.items()]
avg_wait_time = [(time, sum(waits) / len(waits)) for time, waits in bucketed_wait_times.items()]


print('rendering graphs...')

# make graphs
fig, axs = plt.subplots(5)

axs[0].set_title('Request count')
axs[0].plot(*zip(*est_request_counts))
axs[0].plot(*zip(*real_request_counts))

axs[1].set_title('Inference time')
axs[1].plot(*zip(*est_inference_times))
axs[1].plot(*zip(*real_inference_times))

axs[2].set_title('Wait time')
axs[2].plot(*zip(*max_wait_time))
axs[2].plot(*zip(*avg_wait_time))

axs[3].set_title('Queue size')
axs[3].plot(*zip(*queue_sizes))

axs[4].set_title('Num allocations / ML nodes')
axs[4].plot(*zip(*num_allocations_list))
axs[4].plot(*zip(*num_ml_nodes))

plt.tight_layout()
plt.show()
