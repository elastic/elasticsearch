/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutionStats;

/**
 * Tracker object for enrich policy execution stats.
 *
 * The execution stats are tracked externally from the {@link EnrichPolicyExecutor} so that they may be shared across transport actions
 * on the master node. The executor's dependencies make it difficult to share via dependency injection.
 */
public class EnrichPolicyExecutionStatsTracker {

    private final AtomicLong totalExecutionCount = new AtomicLong(0L);
    private final AtomicLong totalExecutionTime = new AtomicLong(0L);
    private final AtomicLong minExecutionTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxExecutionTime = new AtomicLong(0L);
    private final AtomicLong totalRepeatExecutionCount = new AtomicLong(0L);
    private final AtomicLong totalTimeBetweenExecutions = new AtomicLong(0L);
    private final AtomicLong minTimeBetweenExecutions = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxTimeBetweenExecutions = new AtomicLong(0L);
    private final ConcurrentMap<String, Long> previousExecutionCompletionTimes = new ConcurrentHashMap<>();

    void captureCompletedExecution(String policyName, long startTime, long endTime) {
        long duration = Math.max(0, endTime - startTime);
        totalExecutionCount.incrementAndGet();
        totalExecutionTime.addAndGet(duration);
        minExecutionTime.accumulateAndGet(duration, Math::min);
        maxExecutionTime.accumulateAndGet(duration, Math::max);
        Long lastExec = previousExecutionCompletionTimes.get(policyName);
        if (lastExec != null) {
            long sinceLastRun = Math.max(0, startTime - lastExec);
            totalRepeatExecutionCount.incrementAndGet();
            totalTimeBetweenExecutions.addAndGet(sinceLastRun);
            minTimeBetweenExecutions.accumulateAndGet(sinceLastRun, Math::min);
            maxTimeBetweenExecutions.accumulateAndGet(sinceLastRun, Math::max);
        }
        previousExecutionCompletionTimes.put(policyName, endTime);
    }

    public ExecutionStats getStats() {
        // Snap min times to -1 if they do not have values set.
        long minExecTime = minExecutionTime.get();
        if (Long.MAX_VALUE == minExecTime) {
            minExecTime = -1L;
        }
        long minTimeBetweenExecs = minTimeBetweenExecutions.get();
        if (Long.MAX_VALUE == minTimeBetweenExecs) {
            minTimeBetweenExecs = -1L;
        }
        // Calculate average safely
        long totalRepeatExecutions = totalRepeatExecutionCount.get();
        long totalTimeBetweenExecs = totalTimeBetweenExecutions.get();
        long averageTimeBetweenExecs = totalRepeatExecutions == 0 ? 0 : totalTimeBetweenExecs / totalRepeatExecutions;
        return new ExecutionStats(
            totalExecutionCount.get(),
            totalExecutionTime.get(),
            minExecTime,
            maxExecutionTime.get(),
            totalRepeatExecutions,
            totalTimeBetweenExecs,
            averageTimeBetweenExecs,
            minTimeBetweenExecs,
            maxTimeBetweenExecutions.get()
        );
    }
}
