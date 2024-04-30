import math

import numpy as np

# Estimator...

# The critical factor which determines the degree of smoothing is the ratio of the
# time and allocation noise variances to the measurement noise variance. If we aren't
# interested in accurate error estimates we can just set the measurement noise variance
# to 1.0.
VAR_R = 1.0


H = np.array([1.0, 0.0])
x_0 = np.array([0.0, 0.0])
P_0 = np.eye(2) * 64.0 * VAR_R


def compute_Q(var_t: float, var_a: float) -> np.ndarray:
    # We assume mean zero var_t noise on the average inference duration and mean zero
    # var_a noise on the constant of proportionality between the allocation count and
    # the average inference duration.
    var_t *= VAR_R
    var_a *= VAR_R
    return np.array([[var_t, 0.0], [0.0, var_a]])


def compute_F(a: float) -> np.ndarray:
    # Assume that the average inference duration dynmaics are:
    #
    #  x_k = x_{k-1} + c * a_{k-1}
    #
    # Here, c is the constant of proportionality between the allocation count and the
    # inference time. It is used to account for the ratio physical and virtual cores
    # the node is using for inference.
    return np.array([[1.0, a], [0.0, 1.0]])


def compute_x_k_km1(F: np.ndarray, x_km1_km1: np.ndarray) -> np.ndarray:
    # Compute the predicted state prior to the k'th measurement update.
    return F @ x_km1_km1


def compute_P_k_km1(F: np.ndarray, P_km1_km1: np.ndarray, Q: np.ndarray) -> np.ndarray:
    # Compute the predicted error covariance prior to the k'th measurement update.
    return F @ P_km1_km1 @ F.T + Q


def compute_K_k(P_k_km1: np.ndarray) -> np.ndarray:
    # Compute the Kalman gain for the k'th measurement update. Note S is a scalar so we
    # can just divide by it.
    return P_k_km1 @ H.T / (H @ P_k_km1 @ H.T + VAR_R)


def compute_x_k_k(x_k_km1: np.ndarray, K_k: np.ndarray, z: float) -> np.ndarray:
    # Compute the state estimate after the k'th measurement update.
    return x_k_km1 + K_k * (z - np.dot(H, x_k_km1))


def compute_P_k_k(K_k: np.ndarray, P_k_km1: np.ndarray) -> np.ndarray:
    # Compute the error covariance after the k'th measurement update.
    return P_k_km1 - np.outer(K_k, H @ P_k_km1)


class DurationEstimator:
    def __init__(
        self,
        var_t: float = 1e-10,
        var_a: float = 1e-12,
        adjust_interval: float = 100.0,
    ):
        self.noise_var_t = var_t
        self.noise_var_a = var_a
        self.x_k = x_0
        self.P_k = P_0
        self.values_since_last_change = 0
        self.last_allocations = 0
        self.adjust_interval = adjust_interval

    def add(self, duration: float, allocations: float) -> None:
        scale = 1 + (1e4 - 1) * math.exp(
            -self.values_since_last_change / self.adjust_interval
        )
        if allocations != self.last_allocations:
            self.last_allocations = allocations
            self.values_since_last_change = 0
        else:
            self.values_since_last_change += 1
        F_k = compute_F(allocations)
        x_k_km1 = compute_x_k_km1(F_k, self.x_k)
        P_k_km1 = compute_P_k_km1(
            F_k, self.P_k, compute_Q(self.noise_var_t, scale * self.noise_var_a)
        )
        K_k = compute_K_k(P_k_km1)
        self.x_k = compute_x_k_k(x_k_km1, K_k, duration)
        self.P_k = compute_P_k_k(K_k, P_k_km1)

    def estimate(self, allocations: int | None = None) -> float:
        return self.x_k[0] + self.x_k[1] * (
            allocations if allocations is not None else self.last_allocations
        )


# System...

AVG_INFERENCE_TIME = 0.1
PHYSICAL_CORES_PER_NODE = 4


def get_avg_inference_time(num_allocations):
    physical_cores = (
        num_allocations // (2 * PHYSICAL_CORES_PER_NODE) * PHYSICAL_CORES_PER_NODE
    )
    remaining_allocations = num_allocations % (2 * PHYSICAL_CORES_PER_NODE)
    if remaining_allocations < PHYSICAL_CORES_PER_NODE:
        physical_cores += remaining_allocations
    else:
        physical_cores += PHYSICAL_CORES_PER_NODE
    return AVG_INFERENCE_TIME * num_allocations / physical_cores


def get_inference_time(num_allocations):
    return np.random.uniform(0.5, 1.5) * get_avg_inference_time(num_allocations)


# Simulation...

ALLOCATION_SCHEDULE = np.concatenate(
    [
        1 * np.ones(2000),
        2 * np.ones(3000),
        1 * np.ones(4500),
        4 * np.ones(5000),
        7 * np.ones(2000),
        8 * np.ones(3000),
        3 * np.ones(7000),
        2 * np.ones(2000),
        5 * np.ones(3000),
        1 * np.ones(5000),
        7 * np.ones(9000),
    ]
)


def estimate_average_inference_duration(
    var_t: float = 1e-10, var_a: float = 1e-12, adjust_interval: float = 100.0
) -> tuple[list[float], list[float], list[float]]:
    """
    return: ("inference duration", "true average inference duration", "estimated average inference duration")
    """
    rr = []
    zz = []
    zze = []
    estimator = DurationEstimator(var_t, var_a, adjust_interval)
    for a in ALLOCATION_SCHEDULE:
        z = get_inference_time(a)
        estimator.add(z, a)
        rr.append(z)
        zz.append(get_avg_inference_time(a))
        zze.append(estimator.estimate())
    return rr, zz, zze
