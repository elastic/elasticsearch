from collections import deque, defaultdict
import enum
import heapq
import math
import random
import matplotlib.pyplot as plt


random.seed(170681)


class Estimator:
  """
  This implements a 1d Kalman filter with manoeuvre detection.
  Rather than a derived dynamics model, we simply fix how much we want to smooth in the steady state.
  (see also: https://en.wikipedia.org/wiki/Kalman_filter)
  """

  def __init__(self, smoothing_factor, autodetect_dynamics_change):
    self.value = None
    self.variance = None
    self.smoothing_factor = smoothing_factor
    self.autodetect_dynamics_change = autodetect_dynamics_change
    self.dynamics_changed_last_time = False

  def dynamics_change_detected(self, value, variance):
    """Returns whether if the (value, variance) is more than 10 stddev unlikely."""
    return (self.variance is not None and self.autodetect_dynamics_change
            and abs(value - self.value) ** 2.0 / (self.variance + variance) > 100.0)

  def add(self, value, variance, dynamics_changed_external=False) -> None:
    if self.variance is None:
      self.value = value
      self.variance = variance
      self.dynamics_changed_last_time = True
      return

    process_variance = variance / self.smoothing_factor

    dynamics_changed = dynamics_changed_external or self.dynamics_change_detected(value, variance)
    if dynamics_changed or self.dynamics_changed_last_time:
      # If we know we likely had a change in the quantity we're estimating or the prediction is 10 stddev off,
      # we inject extra noise in the dynamics for this step.
      process_variance = value
      print(f'dynamic changed: value={value:.3f}±{math.sqrt(variance):.3f} '
            f'state={self.value:.3f}±{self.error():.23} '
            f'(external={dynamics_changed})')
    self.dynamics_changed_last_time = dynamics_changed

    gain = (self.variance + process_variance) / (self.variance + process_variance + variance)
    self.value += gain * (value - self.value)
    self.variance = (1 - gain) * (self.variance + process_variance)

  def estimate(self) -> float:
    return self.value

  def error(self):
    return math.sqrt(self.variance) if self.variance is not None else self.value

  def lower(self):
    return None if self.value is None else max(0.0, self.value - self.error())

  def upper(self):
    return None if self.value is None else self.value + self.error()


class Autoscaler:
  """
  The autoscaler keeps track of estimates of the request rate and inference latency.
  When capacity is almost exhausted it scales up, and when there's too much overcapacity
  it scales down.
  """

  AUTOSCALE_UP_THRESHOLD = 0.9
  AUTOSCALE_DOWN_THRESHOLD = 0.85

  def __init__(self):
    self.num_allocations = 1
    self.latency_estimator = Estimator(1e2, False)
    self.rate_estimator = Estimator(1e2, True)
    self.request_count_since_last_rate_measurement = 0
    self.inference_count_since_last_measurement = 0
    self.sum_inference_time_since_last_measurement = 0
    self.num_allocation_changed_since_last_inference = True

  def add_request(self):
    self.request_count_since_last_rate_measurement += 1

  def measure_rate(self, interval):
    rate = self.request_count_since_last_rate_measurement / interval
    rate_estimate = estimate if (estimate := self.rate_estimator.estimate()) is not None else rate
    variance = max(1.0, rate_estimate * interval) / interval ** 2
    self.rate_estimator.add(rate, variance)
    self.request_count_since_last_rate_measurement = 0
    return rate

  def add_inference_time(self, inference_time):
    self.inference_count_since_last_measurement += 1
    self.sum_inference_time_since_last_measurement += inference_time

  def measure_inference_time(self):
    if not self.inference_count_since_last_measurement:
      return None

    inference_time = self.sum_inference_time_since_last_measurement / self.inference_count_since_last_measurement
    latency_estimate = estimate if (estimate := self.latency_estimator.estimate()) is not None else inference_time
    variance = latency_estimate ** 2 / self.inference_count_since_last_measurement

    self.latency_estimator.add(inference_time, variance, self.num_allocation_changed_since_last_inference)
    self.num_allocation_changed_since_last_inference = False
    self.inference_count_since_last_measurement = 0
    self.sum_inference_time_since_last_measurement = 0
    return inference_time

  def get_load(self):
    rate = self.rate_estimator.estimate()
    latency = self.latency_estimator.estimate()
    return None if rate is None or latency is None else rate * latency

  def get_load_lower(self):
    rate = self.rate_estimator.lower()
    latency = self.latency_estimator.lower()
    return None if rate is None or latency is None else rate * latency

  def get_load_upper(self):
    rate = self.rate_estimator.upper()
    latency = self.latency_estimator.upper()
    return None if rate is None or latency is None else rate * latency

  def autoscale(self):
    while (load := self.get_load_lower()) is not None and load / self.num_allocations > Autoscaler.AUTOSCALE_UP_THRESHOLD:
      self.num_allocations += 1
      self.num_allocation_changed_since_last_inference = True

    while self.num_allocations > 1 and (load := self.get_load_upper()) is not None and load / (self.num_allocations - 1) < Autoscaler.AUTOSCALE_DOWN_THRESHOLD:
      self.num_allocations -= 1
      self.num_allocation_changed_since_last_inference = True

    return self.num_allocations


class Simulator:
  """
  We simulate a Poisson process for inference arrivals with time varying rate parameter. This models a
  variety of rate dynamics: smooth ramp, smooth periodic, shock and steady.
  Inference is modelled as mean duration + noise. The mean duration captures the behaviour of
  vCPUs which is that throughput is largely constant after all physical cores on a node are occupied.

  We assume perfect load balancing, i.e. that every allocation of inference is never waiting whilst there are
  inferences to be done. Inferences can only be picked off once they have arrived and allocations can only
  pick an inference "inference duration" after they picked their last inference. We assume they are selected
  FIFO.

  The key user parameters are the number of waiting inferences and the average and maximum delay to
  receive each inference which can be calculated from the difference between when inference calls arrived
  and when they are available.
  """

  AVG_INFERENCE_TIME = 0.1
  PHYSICAL_CORES_PER_NODE = 4
  MEASUREMENT_INTERVAL = 10

  class EventType(enum.IntEnum):
    ALLOCATION_STARTED = 1
    REQUEST = 2
    INFERENCE_COMPLETED = 3
    MEASURE = 4

  def __init__(self, start_time, end_time, get_request_rate):
    self.start_time = start_time
    self.end_time = end_time
    self.get_request_rate = get_request_rate
    self.num_allocations = 1

    # data for the graphs
    self.request_rate_data = []
    self.request_rate_estimates = []
    self.request_rate_truth = []
    self.inference_time_data = []
    self.inference_time_estimates = []
    self.inference_time_truth = []
    self.load_estimates = []
    self.load_truth = []
    self.wait_times = []
    self.queue_sizes = []
    self.num_allocations_list = []
    self.num_ml_nodes = []

  def get_avg_inference_time(self):
    """
    Assumes 2*PHYSICAL_CORES_PER_NODE allocations per ML node, which have to share the
    PHYSICAL_CORES_PER_NODE real cores. Assigning more allocations to a node than its
    number of cores leads to increased inference times.
    """
    physical_cores = self.num_allocations // (2 * self.PHYSICAL_CORES_PER_NODE) * self.PHYSICAL_CORES_PER_NODE
    remaining_allocations = self.num_allocations % (2 * self.PHYSICAL_CORES_PER_NODE)
    if remaining_allocations < self.PHYSICAL_CORES_PER_NODE:
      physical_cores += remaining_allocations
    else:
      physical_cores += self.PHYSICAL_CORES_PER_NODE

    return self.AVG_INFERENCE_TIME * self.num_allocations / physical_cores

  def get_random_inference_time(self):
    """
    Return a random inference time, which consists of:
    - average inference time
    - random uniform noise multiplier (between 0.5 and 1.5)
    - environment factor, leading sporadically to 10x latency
    """
    request_factor = random.uniform(0.5, 1.5)
    environment_factor = 10 if random.random() < 0.01 else 0.90901
    return request_factor * environment_factor * self.get_avg_inference_time()

  def create_events(self):
    print('creating events...')

    events = []  # contains tuple of (time, type, allocation_id, inference_time)

    # initial allocations
    for alloc_id in range(self.num_allocations):
      heapq.heappush(events, (0, Simulator.EventType.ALLOCATION_STARTED, alloc_id, None))

    # create all requests following a Poisson process
    # this assumes TIME_STEP << 1 / rate, otherwise they'll be too few events
    TIME_STEP = 0.001
    time = self.start_time + TIME_STEP / 2
    while time < self.end_time:
      if random.random() < self.get_request_rate(time) * TIME_STEP:
        heapq.heappush(events, (time, Simulator.EventType.REQUEST, None, None))
      time += TIME_STEP

    # measurement timestamps
    time = self.start_time
    while time < self.end_time:
      time += self.MEASUREMENT_INTERVAL
      heapq.heappush(events, (time, Simulator.EventType.MEASURE, None, None))

    return events

  def simulate(self, events):
    print('simulating traffic...')

    autoscaler = Autoscaler()

    available_allocations = set()
    inference_queue = deque()  # contains request time
    last_data_time = 0

    while events:
      # handle events
      time, type, alloc_id, inference_time = heapq.heappop(events)
      if time > self.end_time:
        break

      if type == Simulator.EventType.ALLOCATION_STARTED:
        available_allocations.add(alloc_id)

      elif type == Simulator.EventType.REQUEST:
        autoscaler.add_request()
        inference_queue.append(time)

      elif type == Simulator.EventType.MEASURE:
        rate = autoscaler.measure_rate(self.MEASUREMENT_INTERVAL)
        self.request_rate_data.append((time, rate))
        inference_time = autoscaler.measure_inference_time()
        self.inference_time_data.append((time, inference_time))

      elif type == Simulator.EventType.INFERENCE_COMPLETED:
        autoscaler.add_inference_time(inference_time)
        if alloc_id < self.num_allocations:
          available_allocations.add(alloc_id)

      while inference_queue and available_allocations:
        request_time = inference_queue.popleft()
        wait_time = time - request_time
        self.wait_times.append((time, wait_time))
        alloc_id = available_allocations.pop()
        inference_time = self.get_random_inference_time()
        heapq.heappush(events, (time + inference_time, Simulator.EventType.INFERENCE_COMPLETED, alloc_id, inference_time))

      # collect data
      collect_data = time > last_data_time + 1
      if collect_data:
        last_data_time = time
        self.queue_sizes.append((time, len(inference_queue)))
        self.num_allocations_list.append((time, self.num_allocations))
        self.num_ml_nodes.append((time, math.ceil(self.num_allocations / (2 * self.PHYSICAL_CORES_PER_NODE))))
        self.request_rate_estimates.append((time, autoscaler.rate_estimator.estimate()))
        self.request_rate_truth.append((time, self.get_request_rate(time)))
        self.inference_time_estimates.append((time, autoscaler.latency_estimator.estimate()))
        self.inference_time_truth.append((time, self.get_avg_inference_time()))
        if (load := autoscaler.get_load()) is not None:
          self.load_estimates.append((time, load / self.num_allocations))
        self.load_truth.append((time, self.get_request_rate(time) * self.get_avg_inference_time() / self.num_allocations))

      # autoscale
      autoscaled_num_allocations = autoscaler.autoscale()
      if autoscaled_num_allocations != self.num_allocations:
        for alloc_id in range(self.num_allocations, autoscaled_num_allocations):
          heapq.heappush(events, (time, Simulator.EventType.ALLOCATION_STARTED, alloc_id, None))
        print(time, f'autoscale from {self.num_allocations} to {autoscaled_num_allocations}')
        self.num_allocations = autoscaled_num_allocations

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
    axs[0].legend(loc='upper right')

    axs[1].set_title('Inference time')
    axs[1].plot(*zip(*self.inference_time_data), label='data')
    axs[1].plot(*zip(*self.inference_time_estimates), label='estimate')
    axs[1].plot(*zip(*self.inference_time_truth), label='truth')
    axs[1].legend(loc='upper right')
    axs[1].set_ylim([0.05, 0.30])

    axs[2].set_title('Wait time')
    axs[2].plot(*zip(*self.wait_times), label='data')
    axs[2].plot(*zip(*Simulator.create_average_buckets(self.wait_times, 60)), label='1-minute average')
    axs[2].legend(loc='upper right')

    axs[3].set_title('Queue size')
    axs[3].plot(*zip(*self.queue_sizes), label='data')
    axs[3].legend(loc='upper right')

    axs[4].set_title('Num allocations / ML nodes')
    axs[4].plot(*zip(*self.num_allocations_list), label='allocations')
    axs[4].plot(*zip(*self.num_ml_nodes), label='ML nodes')
    axs[4].legend(loc='upper right')

    axs[5].set_title('Load')
    axs[5].plot(*zip(*self.load_estimates), label='estimate')
    axs[5].plot(*zip(*self.load_truth), label='truth')
    axs[5].plot([self.start_time, self.end_time],
                [Autoscaler.AUTOSCALE_DOWN_THRESHOLD, Autoscaler.AUTOSCALE_DOWN_THRESHOLD],
                label='threshold up')
    axs[5].plot([self.start_time, self.end_time],
                [Autoscaler.AUTOSCALE_UP_THRESHOLD, Autoscaler.AUTOSCALE_UP_THRESHOLD],
                label='threshold down')
    axs[5].legend(loc='upper right')
    axs[5].set_ylim([0.0, 1.2])

    plt.tight_layout()
    plt.show()

  def run(self):
    events = self.create_events()
    self.simulate(events)


class TrafficPatterns:
  """
  Different request traffic profiles for the simulator.
  """

  START_TIME = 0
  END_TIME = 5 * 3600

  @staticmethod
  def linear_increasing(time):
    return 1 + 50 * time / TrafficPatterns.END_TIME

  @staticmethod
  def oscillating_2h(time):
    return 10 * (2 - math.cos(2 * math.pi * time / (2*3600)))

  @staticmethod
  def oscillating_5h(time):
    return 50 * (1 - math.cos(2 * math.pi * time / (5*3600)))

  @staticmethod
  def occasionally(_):
    return 1 / 600

  @staticmethod
  def constant(_):
    return 10

  @staticmethod
  def jumps(time):
    return 1 if time % 3600 < 1800 else 100

  @staticmethod
  def all():
    return [TrafficPatterns.linear_increasing, TrafficPatterns.oscillating_2h, TrafficPatterns.oscillating_5h,
            TrafficPatterns.occasionally, TrafficPatterns.constant, TrafficPatterns.jumps]


def main():
  for get_request_rate in TrafficPatterns.all():
    simulator = Simulator(TrafficPatterns.START_TIME, TrafficPatterns. END_TIME, get_request_rate)
    simulator.run()
    simulator.create_figure()


if __name__ == "__main__":
  main()
