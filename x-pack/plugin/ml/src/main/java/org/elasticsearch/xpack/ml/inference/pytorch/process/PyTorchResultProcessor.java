/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.time.Instant;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

public class PyTorchResultProcessor {

    public record RecentStats(long requestsProcessed, Double avgInferenceTime) {}

    public record ResultStats(
        LongSummaryStatistics timingStats,
        int errorCount,
        int numberOfPendingResults,
        Instant lastUsed,
        long peakThroughput,
        RecentStats recentStats
    ) {}

    private static final Logger logger = LogManager.getLogger(PyTorchResultProcessor.class);
    static long REPORTING_PERIOD_MS = TimeValue.timeValueMinutes(1).millis();

    private final ConcurrentMap<String, PendingResult> pendingResults = new ConcurrentHashMap<>();
    private final String deploymentId;
    private final Consumer<ThreadSettings> threadSettingsConsumer;
    private volatile boolean isStopping;
    private final LongSummaryStatistics timingStats;
    private int errorCount;
    private long peakThroughput;

    private LongSummaryStatistics lastPeriodSummaryStats;
    private RecentStats lastPeriodStats;
    private long currentPeriodEndTimeMs;
    private long lastResultTimeMs;
    private final long startTime;
    private final LongSupplier currentTimeMsSupplier;

    public PyTorchResultProcessor(String deploymentId, Consumer<ThreadSettings> threadSettingsConsumer) {
        this(deploymentId, threadSettingsConsumer, System::currentTimeMillis);
    }

    // for testing
    PyTorchResultProcessor(String deploymentId, Consumer<ThreadSettings> threadSettingsConsumer, LongSupplier currentTimeSupplier) {
        this.deploymentId = Objects.requireNonNull(deploymentId);
        this.timingStats = new LongSummaryStatistics();
        this.lastPeriodSummaryStats = new LongSummaryStatistics();
        this.threadSettingsConsumer = Objects.requireNonNull(threadSettingsConsumer);
        this.currentTimeMsSupplier = currentTimeSupplier;
        this.startTime = currentTimeSupplier.getAsLong();
        this.currentPeriodEndTimeMs = startTime + REPORTING_PERIOD_MS;

    }

    public void registerRequest(String requestId, ActionListener<PyTorchInferenceResult> listener) {
        pendingResults.computeIfAbsent(requestId, k -> new PendingResult(listener));
    }

    /**
     * Call this method when the caller is no longer waiting on the request response.
     * Note that the pending result listener will not be notified.
     *
     * @param requestId The request ID that is no longer being waited on
     */
    public void ignoreResponseWithoutNotifying(String requestId) {
        pendingResults.remove(requestId);
    }

    public void process(PyTorchProcess process) {
        try {
            Iterator<PyTorchResult> iterator = process.readResults();
            while (iterator.hasNext()) {
                PyTorchResult result = iterator.next();
                PyTorchInferenceResult inferenceResult = result.inferenceResult();
                if (inferenceResult != null) {
                    processInferenceResult(inferenceResult);
                }
                ThreadSettings threadSettings = result.threadSettings();
                if (threadSettings != null) {
                    threadSettingsConsumer.accept(threadSettings);
                }
            }
        } catch (Exception e) {
            // No need to report error as we're stopping
            if (isStopping == false) {
                logger.error(new ParameterizedMessage("[{}] Error processing results", deploymentId), e);
            }
            pendingResults.forEach(
                (id, pendingResult) -> pendingResult.listener.onResponse(
                    new PyTorchInferenceResult(
                        id,
                        null,
                        null,
                        isStopping
                            ? "inference canceled as process is stopping"
                            : "inference native process died unexpectedly with failure [" + e.getMessage() + "]"
                    )
                )
            );
            pendingResults.clear();
        } finally {
            pendingResults.forEach(
                (id, pendingResult) -> pendingResult.listener.onResponse(
                    new PyTorchInferenceResult(id, null, null, "inference canceled as process is stopping")
                )
            );
            pendingResults.clear();
        }
        logger.debug(() -> new ParameterizedMessage("[{}] Results processing finished", deploymentId));
    }

    void processInferenceResult(PyTorchInferenceResult inferenceResult) {
        logger.trace(() -> new ParameterizedMessage("[{}] Parsed result with id [{}]", deploymentId, inferenceResult.getRequestId()));
        processResult(inferenceResult);
        PendingResult pendingResult = pendingResults.remove(inferenceResult.getRequestId());
        if (pendingResult == null) {
            logger.debug(() -> new ParameterizedMessage("[{}] no pending result for [{}]", deploymentId, inferenceResult.getRequestId()));
        } else {
            pendingResult.listener.onResponse(inferenceResult);
        }
    }

    public synchronized ResultStats getResultStats() {
        long currentMs = currentTimeMsSupplier.getAsLong();
        long currentPeriodStartTimeMs = startTime + Intervals.alignToFloor(currentMs - startTime, REPORTING_PERIOD_MS);

        // Do we have results from the previous period?
        RecentStats rs = null;
        if (lastResultTimeMs >= currentPeriodStartTimeMs) {
            // if there is a result for the last period then set it.
            // lastPeriodStats will be null when more than one period
            // has passed without a result.
            rs = lastPeriodStats;
        } else if (lastResultTimeMs >= currentPeriodStartTimeMs - REPORTING_PERIOD_MS) {
            // there was a result in the last period but not one
            // in this period to close off the last period stats.
            // The stats are valid return them here
            rs = new RecentStats(lastPeriodSummaryStats.getCount(), lastPeriodSummaryStats.getAverage());
            peakThroughput = Math.max(peakThroughput, lastPeriodSummaryStats.getCount());
        }

        if (rs == null) {
            // no results processed in the previous period
            rs = new RecentStats(0L, null);
        }

        return new ResultStats(
            new LongSummaryStatistics(timingStats.getCount(), timingStats.getMin(), timingStats.getMax(), timingStats.getSum()),
            errorCount,
            pendingResults.size(),
            lastResultTimeMs > 0 ? Instant.ofEpochMilli(lastResultTimeMs) : null,
            this.peakThroughput,
            rs
        );
    }

    private synchronized void processResult(PyTorchInferenceResult result) {
        if (result.isError()) {
            errorCount++;
            return;
        }

        timingStats.accept(result.getTimeMs());

        lastResultTimeMs = currentTimeMsSupplier.getAsLong();
        if (lastResultTimeMs > currentPeriodEndTimeMs) {
            // rolled into the next period
            peakThroughput = Math.max(peakThroughput, lastPeriodSummaryStats.getCount());
            // TODO min inference time
            if (lastResultTimeMs > currentPeriodEndTimeMs + REPORTING_PERIOD_MS) {
                // We have skipped one or more periods,
                // there is no data for the last period
                lastPeriodStats = null;
            } else {
                lastPeriodStats = new RecentStats(lastPeriodSummaryStats.getCount(), lastPeriodSummaryStats.getAverage());
            }

            lastPeriodSummaryStats = new LongSummaryStatistics();
            lastPeriodSummaryStats.accept(result.getTimeMs());

            // set to the end of the current bucket
            currentPeriodEndTimeMs = startTime + Intervals.alignToCeil(lastResultTimeMs - startTime, REPORTING_PERIOD_MS);
        } else {
            lastPeriodSummaryStats.accept(result.getTimeMs());
        }
    }

    public void stop() {
        isStopping = true;
    }

    public static class PendingResult {
        public final ActionListener<PyTorchInferenceResult> listener;

        public PendingResult(ActionListener<PyTorchInferenceResult> listener) {
            this.listener = Objects.requireNonNull(listener);
        }
    }
}
