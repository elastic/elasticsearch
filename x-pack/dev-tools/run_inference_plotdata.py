import json
from collections import defaultdict

import matplotlib.pyplot as plt

data = list(open('data'))
requests = json.loads(data[0])
stats = json.loads(data[1])


def get_request_count(requests):
  values = defaultdict(int)
  for request_id, request_time, response_time, _ in requests:
    values[int(request_time // 1)] += 1
  return [(t, values.get(t, 0.0)) for t in range(0, max(values)+1)]

def get_error_count(requests):
  values = defaultdict(int)
  for request_id, request_time, response_time, error in requests:
    values[int(request_time // 1)] += 1 if error else 0
  return [(t, values.get(t, 0.0)) for t in range(0, max(values)+1)]

def get_request_time(requests):
  return sorted((request_time, response_time - request_time) for request_id, request_time, response_time, _ in requests)

def get_number_of_allocations(stats):
  return sorted((request_time, number_of_allocations) for request_time, number_of_allocations, _, _, _ in stats)

def derivative(series):
  return [(series[i][0], series[i][1] - series[i-1][1]) for i in range(1, len(series))]

def get_inference_count(stats):
  return derivative(sorted((request_time, inference_count) for request_time, _, inference_count, _, _ in stats))

def get_inference_time(stats):
  total_time = derivative(sorted((request_time, inference_count * inference_time) for request_time, _,inference_count, inference_time, _ in stats))
  inference_count = get_inference_count(stats)
  return [(total_time[i][0], total_time[i][1] / inference_count[i][1]) for i in range(len(total_time)) if inference_count[i][1]]

def get_pending_count(stats):
  return sorted((request_time, pending_count) for request_time, _, _, _, pending_count in stats)


request_count = get_request_count(requests)
error_count = get_error_count(requests)
request_time = get_request_time(requests)

number_of_allocations = get_number_of_allocations(stats)
inference_count = get_inference_count(stats)
inference_time = get_inference_time(stats)
pending_count = get_pending_count(stats)

fig, axs = plt.subplots(4)
fig.set_size_inches(6.4, 6.4)

axs[0].set_title('Request count')
axs[0].plot(*zip(*request_count), label='request count')
axs[0].plot(*zip(*error_count), label='error count')
axs[0].plot(*zip(*inference_count), label='response count')
axs[0].legend(loc='lower right')

axs[1].set_title('Inference time')
axs[1].plot(*zip(*request_time), label='inference + wait time')
axs[1].plot(*zip(*inference_time), label='inference time')
axs[1].legend(loc='lower right')

axs[2].set_title('Number of allocations')
axs[2].plot(*zip(*number_of_allocations), label='number of allocations')
axs[2].legend(loc='lower right')

axs[3].set_title('Pending count')
axs[3].plot(*zip(*pending_count), label='pending count')
axs[3].legend(loc='lower right')

plt.tight_layout()
plt.show()
