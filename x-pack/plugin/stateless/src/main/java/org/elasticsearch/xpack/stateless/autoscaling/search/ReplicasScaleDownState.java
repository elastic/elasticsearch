/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.search;

import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks replica scale-down recommendations per index, including the number of signals received and the
 * highest replica count recommended across all signals.
 * <p> Thread-safe for concurrent access as updateMaxReplicasRecommended is atomic.
 */
public class ReplicasScaleDownState {
    private final ConcurrentHashMap<String, PerIndexState> scaleDownStateByIndex;

    ReplicasScaleDownState() {
        scaleDownStateByIndex = new ConcurrentHashMap<>();
    }

    /**
     * Clear state for every index.
     */
    void clearState() {
        scaleDownStateByIndex.clear();
    }

    /**
     * Clear state for the given indices.
     * @param indices the indices to clear.
     */
    void clearStateForIndices(Collection<String> indices) {
        if (indices != null && indices.isEmpty() == false) {
            scaleDownStateByIndex.entrySet().removeIf(e -> indices.contains(e.getKey()));
        }
    }

    /**
     * Clear state for all indices not given.
     * @param indices the indices to keep.
     */
    void clearStateExceptForIndices(Collection<String> indices) {
        if (indices == null || indices.isEmpty()) {
            scaleDownStateByIndex.clear();
        } else {
            scaleDownStateByIndex.entrySet().removeIf(e -> indices.contains(e.getKey()) == false);
        }
    }

    /**
     * Get the current state for the given index.
     * @param index the index
     * @return the state for the given index, or null if no state exists
     */
    @Nullable
    PerIndexState getState(String index) {
        return scaleDownStateByIndex.get(index);
    }

    /**
     * Update the max number of replicas to scale down to for the given index. Also increments the
     * signal count for the given index.
     * @param index the index
     * @param replicasRecommended the recommended number of replicas
     * @return the updated state for the given index
     */
    PerIndexState updateMaxReplicasRecommended(String index, int replicasRecommended) {
        return scaleDownStateByIndex.compute(index, (k, existing) -> {
            if (existing == null) {
                return new PerIndexState(1, replicasRecommended);
            }
            return new PerIndexState(existing.signalCount() + 1, Math.max(existing.maxReplicasRecommended(), replicasRecommended));
        });
    }

    boolean isEmpty() {
        return scaleDownStateByIndex.isEmpty();
    }

    record PerIndexState(int signalCount, int maxReplicasRecommended) {}
}
