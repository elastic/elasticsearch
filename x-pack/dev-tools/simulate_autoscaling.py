from collections import deque, defaultdict
import enum
import heapq
import math
import random
import matplotlib.pyplot as plt


START_TIME = 0
END_TIME = 5*3600
MEASUREMENT_INTERVAL = 1

AUTO_SCALE_UP_THRESHOLD = 0.9
AUTO_SCALE_DOWN_THRESHOLD = 0.8

AVG_INFERENCE_TIME = 0.1
PHYSICAL_CORES_PER_NODE = 4


random.seed(170681)


class Estimator:
  def __init__(self, value, variance, smoothing_factor):
    self.value = value
    self.variance = variance
    self.smoothing_factor = smoothing_factor

  def add(self, value, variance, dynamics_changed=False) -> None:
    process_variance = variance / self.smoothing_factor

    if dynamics_changed or abs(value - self.value)**2 / (self.variance + variance) > 100:
      process_variance *= self.smoothing_factor
      print(f'dynamic changed: value={value} state={self.value} +-/ {math.sqrt(self.variance)}')

    gain = (self.variance + process_variance) / (self.variance + process_variance + variance)
    self.value += gain * (value - self.value)
    self.variance = (1 - gain) * (self.variance + process_variance)

  def estimate(self) -> float:
    return self.value

  def lower(self):
    return max(0.0, self.value - math.sqrt(self.variance))

  def upper(self):
    return self.value + math.sqrt(self.variance)


class Simulator:

  class EventType(enum.IntEnum):
    ALLOCATION_STARTED = 1
    REQUEST = 2
    INFERENCE_COMPLETED = 3
    MEASURE_RATE = 4

  def __init__(self, get_request_rate):
    self.get_request_rate = get_request_rate

    # state of the system
    self.events = []  # contains tuple of (time, type, allocation_id, inference_time)
    self.num_allocations = 1

    # data for the graphs
    self.request_rate_data = []
    self.request_rate_estimates = []
    self.request_rate_truth = []
    self.inference_time_data = []
    self.inference_time_estimates = []
    self.inference_time_truth = []
    self.wait_times = []
    self.queue_sizes = []
    self.num_allocations_list = []
    self.num_ml_nodes = []
    self.est_loads = []
    self.real_loads = []

  def get_avg_inference_time(self):
    physical_cores = self.num_allocations // (2 * PHYSICAL_CORES_PER_NODE) * PHYSICAL_CORES_PER_NODE
    remaining_allocations = self.num_allocations % (2 * PHYSICAL_CORES_PER_NODE)
    if remaining_allocations < PHYSICAL_CORES_PER_NODE:
      physical_cores += remaining_allocations
    else:
      physical_cores += PHYSICAL_CORES_PER_NODE
    return AVG_INFERENCE_TIME * self.num_allocations / physical_cores

  def get_random_inference_time(self):
    return random.uniform(0.5, 1.5) * self.get_avg_inference_time()

  def create_events(self):
    print('creating events...')

    # initial allocations
    for alloc_id in range(self.num_allocations):
      heapq.heappush(self.events, (0, Simulator.EventType.ALLOCATION_STARTED, alloc_id, None))

    # create all requests
    TIME_STEP = 0.001
    time = START_TIME + TIME_STEP / 2
    while time < END_TIME:
      if random.random() < self.get_request_rate(time) * TIME_STEP:
        heapq.heappush(self.events, (time, Simulator.EventType.REQUEST, None, None))
      time += TIME_STEP

    # measurement timestamps
    time = 0
    while time < END_TIME:
      time += MEASUREMENT_INTERVAL
      heapq.heappush(self.events, (time, Simulator.EventType.MEASURE_RATE, None, None))

  def simulate(self):
    print('simulating traffic...')

    latency_estimator = Estimator(0.1, 100, 1e6)
    rate_estimator = Estimator(0, 100, 1e3)

    available_allocations = set()
    inference_queue = deque()  # contains request time


    last_data_time = 0

    num_requests = 0
    num_allocations_changed = False

    while self.events:
      # handle events
      time, type, alloc_id, inference_time = heapq.heappop(self.events)
      if time > END_TIME:
        break

      if type == Simulator.EventType.ALLOCATION_STARTED:
        available_allocations.add(alloc_id)

      elif type == Simulator.EventType.REQUEST:
        num_requests += 1
        inference_queue.append(time)

      elif type == Simulator.EventType.MEASURE_RATE:
        rate = num_requests / MEASUREMENT_INTERVAL
        self.request_rate_data.append((time, rate))
        variance = max(1.0, rate_estimator.estimate() * MEASUREMENT_INTERVAL) / MEASUREMENT_INTERVAL**2
        rate_estimator.add(rate, variance)
        num_requests = 0

      elif type == Simulator.EventType.INFERENCE_COMPLETED:
        variance = latency_estimator.estimate() ** 2
        latency_estimator.add(inference_time, variance, num_allocations_changed)
        num_allocations_changed = False

        if alloc_id < self.num_allocations:
          available_allocations.add(alloc_id)

      while inference_queue and available_allocations:
        request_time = inference_queue.popleft()
        wait_time = time - request_time
        self.wait_times.append((time, wait_time))
        self.inference_time_data.append((time, inference_time))
        alloc_id = available_allocations.pop()
        inference_time = self.get_random_inference_time()
        heapq.heappush(self.events, (time + inference_time, Simulator.EventType.INFERENCE_COMPLETED, alloc_id, inference_time))

      est_request_count = rate_estimator.estimate()
      est_inference_time = latency_estimator.estimate()
      needed_allocations = est_request_count * est_inference_time

      # collect data
      collect_data = time > last_data_time + 1
      if collect_data:
        last_data_time = time
        self.queue_sizes.append((time, len(inference_queue)))
        self.num_allocations_list.append((time, self.num_allocations))
        self.num_ml_nodes.append((time, math.ceil(self.num_allocations / (2 * PHYSICAL_CORES_PER_NODE))))
        self.request_rate_estimates.append((time, est_request_count))
        self.request_rate_truth.append((time, self.get_request_rate(time)))
        self.inference_time_estimates.append((time, est_inference_time))
        self.inference_time_truth.append((time, self.get_avg_inference_time()))
        self.est_loads.append((time, needed_allocations / self.num_allocations))
        self.real_loads.append((time, self.get_request_rate(time) * self.get_avg_inference_time() / self.num_allocations))

      # autoscaling
      needed_allocations_lower = latency_estimator.lower() * rate_estimator.lower()
      while needed_allocations_lower > AUTO_SCALE_UP_THRESHOLD * self.num_allocations:
        heapq.heappush(self.events, (time, Simulator.EventType.ALLOCATION_STARTED, self.num_allocations, None))
        self.num_allocations += 1
        num_allocations_changed = True
        print(time, f'autoscale up to {self.num_allocations} (wanted={needed_allocations} / {needed_allocations_lower})')

      needed_allocations_upper = latency_estimator.upper() * rate_estimator.upper()
      while self.num_allocations > 1 and needed_allocations_upper < AUTO_SCALE_DOWN_THRESHOLD * (self.num_allocations - 1):
        self.num_allocations -= 1
        num_allocations_changed = True
        print(time, f'autoscale down to {self.num_allocations} (wanted={needed_allocations} / {needed_allocations_upper})')

    print("avg num allocations:", sum(x for t,x in self.num_allocations_list) / len(self.num_allocations_list))
    print("avg wait time", sum(x for t,x in self.wait_times) / len(self.wait_times))
    print("max wait time", max(x for t,x in self.wait_times))

  @staticmethod
  def create_average_buckets(data, bucket_size):
    values = defaultdict(list)
    for time, value in data:
      values[time // bucket_size * bucket_size].append(value)
    return [(time, sum(value) / len(value)) for time, value in values.items()]

  def create_figure(self):
    print('creating figure...')

    fig, axs = plt.subplots(6)
    fig.set_size_inches(6.4, 9.6)

    axs[0].set_title('Request count')
    axs[0].plot(*zip(*self.request_rate_data), label='data')
    axs[0].plot(*zip(*self.request_rate_estimates), label='estimate')
    axs[0].plot(*zip(*self.request_rate_truth), label='truth')
    axs[0].legend(loc='upper left')

    axs[1].set_title('Inference time')
    axs[1].plot(*zip(*self.inference_time_data), label='data')
    axs[1].plot(*zip(*self.inference_time_estimates), label='estimate')
    axs[1].plot(*zip(*self.inference_time_truth), label='truth')
    axs[1].legend(loc='upper left')

    axs[2].set_title('Wait time')
    axs[2].plot(*zip(*self.wait_times), label='data')
    axs[2].plot(*zip(*Simulator.create_average_buckets(self.wait_times, 60)), label='1-minute average')
    axs[2].legend(loc='upper left')

    axs[3].set_title('Queue size')
    axs[3].plot(*zip(*self.queue_sizes), label='data')
    axs[3].legend(loc='upper left')

    axs[4].set_title('Num allocations / ML nodes')
    axs[4].plot(*zip(*self.num_allocations_list), label='allocations')
    axs[4].plot(*zip(*self.num_ml_nodes), label='ML nodes')
    axs[4].legend(loc='upper left')

    axs[5].set_title('Load')
    axs[5].plot(*zip(*self.est_loads), label='estimate')
    axs[5].plot(*zip(*self.real_loads), label='truth')
    axs[5].plot([START_TIME, END_TIME], [AUTO_SCALE_DOWN_THRESHOLD, AUTO_SCALE_DOWN_THRESHOLD], label='threshold up')
    axs[5].plot([START_TIME, END_TIME], [AUTO_SCALE_UP_THRESHOLD, AUTO_SCALE_UP_THRESHOLD], label='threshold down')
    axs[5].legend(loc='upper left')

    plt.tight_layout()
    plt.show()

  def run(self):
    self.create_events()
    self.simulate()


def linear_increasing(time):
  return 1 + 50 * time / END_TIME

def oscillating_2h(time):
  return 10 * (2 - math.cos(2 * math.pi * time / (2*3600)))

def oscillating_5h(time):
  return 50 * (1 - math.cos(2 * math.pi * time / (5*3600)))

def occasionally(time):
  return 1 / 600

def constant(time):
    return 1

def jump(time):
  return 1 if time<3600 else 100


for get_request_rate in [linear_increasing, oscillating_2h, oscillating_5h, jump, occasionally]:
  simulator = Simulator(get_request_rate)
  simulator.run()
  simulator.create_figure()
