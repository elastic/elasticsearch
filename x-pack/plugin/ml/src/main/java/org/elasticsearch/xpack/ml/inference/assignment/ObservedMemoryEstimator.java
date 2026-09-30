/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Derives a stable, per-allocation native memory estimate for each trained model deployment from the actual
 * (OS-reported) resident set size observed at runtime.
 *
 * <p>The raw signal is the peak resident set size on the busiest node divided by the number of allocations running
 * there. Because a single spike must not immediately relax the memory guards, the estimate <em>ratchets up
 * instantly</em> (the moment a larger value is seen it is adopted, so the planner and scaler stay conservative and
 * OOM-safe) but <em>decays slowly</em> (an exponentially weighted step back down) so that a transient peak does not
 * permanently over-reserve memory. This asymmetric smoothing is deliberately biased towards avoiding
 * under-estimation, which is what causes out-of-memory failures.
 */
public class ObservedMemoryEstimator {

    /**
     * The fraction of the gap between the current estimate and a smaller raw sample that is stepped down on each
     * update. A small value means the estimate decays slowly (many samples must confirm the lower usage before the
     * estimate drops), keeping the memory guards conservative.
     */
    static final double DECAY_FACTOR = 0.1;

    private final Map<String, Long> effectivePerAllocationBytesByDeployment = new ConcurrentHashMap<>();

    /**
     * Fold a new observation for a deployment into its effective per-allocation memory estimate.
     *
     * @param deploymentId the deployment the observation belongs to
     * @param peakRssBytes the peak resident set size (bytes) observed on the busiest node for this deployment
     * @param allocationsOnNode the number of allocations running on that node (values &lt; 1 are treated as 1)
     * @return the updated effective per-allocation memory estimate in bytes
     */
    public long update(String deploymentId, long peakRssBytes, int allocationsOnNode) {
        long rawPerAllocation = peakRssBytes / Math.max(1, allocationsOnNode);
        return effectivePerAllocationBytesByDeployment.merge(
            deploymentId,
            rawPerAllocation,
            (previous, raw) -> raw >= previous ? raw : (long) (previous - DECAY_FACTOR * (previous - raw))
        );
    }

    /**
     * @return the current effective per-allocation memory estimate for the deployment, or {@code null} if no
     * observation has been folded in yet.
     */
    public Long get(String deploymentId) {
        return effectivePerAllocationBytesByDeployment.get(deploymentId);
    }

    /**
     * Forget any state held for a deployment (e.g. once it has been stopped).
     */
    public void remove(String deploymentId) {
        effectivePerAllocationBytesByDeployment.remove(deploymentId);
    }

    /**
     * Forget all deployment state (e.g. when all assignments are cleared).
     */
    public void clear() {
        effectivePerAllocationBytesByDeployment.clear();
    }
}
