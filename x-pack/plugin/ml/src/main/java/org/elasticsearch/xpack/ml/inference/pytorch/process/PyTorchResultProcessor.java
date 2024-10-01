/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.inference.pytorch.results.AckResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ErrorResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;

import java.time.Instant;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class PyTorchResultProcessor {

    public record RecentStats(long requestsProcessed, Double avgInferenceTime, long cacheHitCount) {}

    public record ResultStats(
        LongSummaryStatistics timingStats,
        LongSummaryStatistics timingStatsExcludingCacheHits,
        int errorCount,
        long cacheHitCount,
        int numberOfPendingResults,
        Instant lastUsed,
        long peakThroughput,
        RecentStats recentStats
    ) {}

    private static final Logger logger = LogManager.getLogger(PyTorchResultProcessor.class);
    static long REPORTING_PERIOD_MS = TimeValue.timeValueMinutes(1).millis();

    private final ConcurrentMap<String, PendingResult> pendingResults = new ConcurrentHashMap<>();
    private final String modelId;
    private final Consumer<ThreadSettings> threadSettingsConsumer;
    private volatile boolean isStopping;
    private final LongSummaryStatistics timingStats;
    private final LongSummaryStatistics timingStatsExcludingCacheHits;
    private int errorCount;
    private long cacheHitCount;
    private long peakThroughput;

    private LongSummaryStatistics lastPeriodSummaryStats;
    private long lastPeriodCacheHitCount;
    private RecentStats lastPeriodStats;
    private long currentPeriodEndTimeMs;
    private long lastResultTimeMs;
    private final long startTime;
    private final LongSupplier currentTimeMsSupplier;
    private final CountDownLatch processorCompletionLatch = new CountDownLatch(1);

    public PyTorchResultProcessor(String modelId, Consumer<ThreadSettings> threadSettingsConsumer) {
        this(modelId, threadSettingsConsumer, System::currentTimeMillis);
    }

    // for testing
    PyTorchResultProcessor(String modelId, Consumer<ThreadSettings> threadSettingsConsumer, LongSupplier currentTimeSupplier) {
        this.modelId = Objects.requireNonNull(modelId);
        this.timingStats = new LongSummaryStatistics();
        this.timingStatsExcludingCacheHits = new LongSummaryStatistics();
        this.lastPeriodSummaryStats = new LongSummaryStatistics();
        this.threadSettingsConsumer = Objects.requireNonNull(threadSettingsConsumer);
        this.currentTimeMsSupplier = currentTimeSupplier;
        this.startTime = currentTimeSupplier.getAsLong();
        this.currentPeriodEndTimeMs = startTime + REPORTING_PERIOD_MS;
    }

    public void registerRequest(String requestId, ActionListener<PyTorchResult> listener) {
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

                if (result.inferenceResult() != null) {
                    processInferenceResult(result);
                } else if (result.threadSettings() != null) {
                    threadSettingsConsumer.accept(result.threadSettings());
                    processThreadSettings(result);
                } else if (result.ackResult() != null) {
                    processAcknowledgement(result);
                } else if (result.errorResult() != null) {
                    processErrorResult(result);
                } else {
                    // will should only get here if the native process
                    // has produced a partially valid result, one that
                    // is accepted by the parser but does not have any
                    // content
                    handleUnknownResultType(result);
                }
            }
        } catch (Exception e) {
            // No need to report error as we're stopping
            if (isStopping == false) {
                logger.error(() -> "[" + modelId + "] Error processing results", e);
            }
            var errorResult = new ErrorResult(
                isStopping
                    ? "inference canceled as process is stopping"
                    : "inference native process died unexpectedly with failure [" + e.getMessage() + "]"
            );
            notifyAndClearPendingResults(errorResult);
        } finally {
            notifyAndClearPendingResults(new ErrorResult("inference canceled as process is stopping"));
            processorCompletionLatch.countDown();
        }
        logger.debug(() -> "[" + modelId + "] Results processing finished");
    }

    private void notifyAndClearPendingResults(ErrorResult errorResult) {
        if (pendingResults.size() > 0) {
            logger.warn(format("[%s] clearing [%d] requests pending results", modelId, pendingResults.size()));
        }
        pendingResults.forEach(
            (id, pendingResult) -> pendingResult.listener.onResponse(new PyTorchResult(id, null, null, null, null, null, errorResult))
        );
        pendingResults.clear();
    }

    void processInferenceResult(PyTorchResult result) {
        PyTorchInferenceResult inferenceResult = result.inferenceResult();
        assert inferenceResult != null;

        logger.debug(() -> format("[%s] Parsed inference result with id [%s]", modelId, result.requestId()));
        PendingResult pendingResult = pendingResults.remove(result.requestId());
        if (pendingResult == null) {
            logger.debug(() -> format("[%s] no pending result for inference [%s]", modelId, result.requestId()));
        } else {
            pendingResult.listener.onResponse(result);
        }
    }

    void processThreadSettings(PyTorchResult result) {
        ThreadSettings threadSettings = result.threadSettings();
        assert threadSettings != null;

        logger.debug(() -> format("[%s] Parsed thread settings result with id [%s]", modelId, result.requestId()));
        PendingResult pendingResult = pendingResults.remove(result.requestId());
        if (pendingResult == null) {
            logger.debug(() -> format("[%s] no pending result for thread settings [%s]", modelId, result.requestId()));
        } else {
            pendingResult.listener.onResponse(result);
        }
    }

    void processAcknowledgement(PyTorchResult result) {
        AckResult ack = result.ackResult();
        assert ack != null;

        logger.debug(() -> format("[%s] Parsed ack result with id [%s]", modelId, result.requestId()));
        PendingResult pendingResult = pendingResults.remove(result.requestId());
        if (pendingResult == null) {
            logger.debug(() -> format("[%s] no pending result for ack [%s]", modelId, result.requestId()));
        } else {
            pendingResult.listener.onResponse(result);
        }
    }

    void processErrorResult(PyTorchResult result) {
        ErrorResult errorResult = result.errorResult();
        assert errorResult != null;

        // Only one result is processed at any time, but we need to stop this happening part way through another thread getting stats
        synchronized (this) {
            errorCount++;
        }

        logger.debug(() -> format("[%s] Parsed error with id [%s]", modelId, result.requestId()));
        PendingResult pendingResult = pendingResults.remove(result.requestId());
        if (pendingResult == null) {
            logger.debug(() -> format("[%s] no pending result for error [%s]", modelId, result.requestId()));
        } else {
            pendingResult.listener.onResponse(result);
        }
    }

    void handleUnknownResultType(PyTorchResult result) {
        if (result.requestId() != null) {
            PendingResult pendingResult = pendingResults.remove(result.requestId());
            if (pendingResult == null) {
                logger.error(() -> format("[%s] no pending result listener for unknown result type [%s]", modelId, result));
            } else {
                String msg = format("[%s] pending result listener cannot handle unknown result type [%s]", modelId, result);
                logger.error(msg);
                var errorResult = new ErrorResult(msg);
                pendingResult.listener.onResponse(new PyTorchResult(result.requestId(), null, null, null, null, null, errorResult));
            }
        } else {
            // Cannot look up the listener without a request id
            // all that can be done in this case is log a message.
            // The result parser requires a request id so this
            // code should not be hit.
            logger.error(() -> format("[%s] cannot process unknown result type [%s]", modelId, result));
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
            rs = new RecentStats(lastPeriodSummaryStats.getCount(), lastPeriodSummaryStats.getAverage(), lastPeriodCacheHitCount);
            peakThroughput = Math.max(peakThroughput, lastPeriodSummaryStats.getCount());
        }

        if (rs == null) {
            // no results processed in the previous period
            rs = new RecentStats(0L, null, 0L);
        }

        return new ResultStats(
            cloneSummaryStats(timingStats),
            cloneSummaryStats(timingStatsExcludingCacheHits),
            errorCount,
            cacheHitCount,
            pendingResults.size(),
            lastResultTimeMs > 0 ? Instant.ofEpochMilli(lastResultTimeMs) : null,
            this.peakThroughput,
            rs
        );
    }

    private static LongSummaryStatistics cloneSummaryStats(LongSummaryStatistics stats) {
        return new LongSummaryStatistics(stats.getCount(), stats.getMin(), stats.getMax(), stats.getSum());
    }

    public synchronized void updateStats(PyTorchResult result) {
        Long timeMs = result.timeMs();
        if (timeMs == null) {
            assert false : "time_ms should be set for an inference result";
            timeMs = 0L;
        }
        boolean isCacheHit = Boolean.TRUE.equals(result.isCacheHit());
        timingStats.accept(timeMs);

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
                lastPeriodStats = new RecentStats(
                    lastPeriodSummaryStats.getCount(),
                    lastPeriodSummaryStats.getAverage(),
                    lastPeriodCacheHitCount
                );
            }

            lastPeriodCacheHitCount = 0;
            lastPeriodSummaryStats = new LongSummaryStatistics();
            lastPeriodSummaryStats.accept(timeMs);

            // set to the end of the current bucket
            currentPeriodEndTimeMs = startTime + Intervals.alignToCeil(lastResultTimeMs - startTime, REPORTING_PERIOD_MS);
        } else {
            lastPeriodSummaryStats.accept(timeMs);
        }

        if (isCacheHit) {
            cacheHitCount++;
            lastPeriodCacheHitCount++;
        } else {
            // don't include cache hits when recording inference time
            timingStatsExcludingCacheHits.accept(timeMs);
        }
    }

    public void stop() {
        isStopping = true;
    }

    /**
     * Waits for specified amount of time for the processor to complete.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout
     * @throws TimeoutException if the results processor has not completed after exceeding the timeout period
     */
    public void awaitCompletion(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            if (processorCompletionLatch.await(timeout, unit) == false) {
                throw new TimeoutException(format("Timed out waiting for pytorch results processor to complete for model id %s", modelId));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info(format("[%s] Interrupted waiting for pytorch results processor to complete", modelId));
        }
    }

    public static class PendingResult {
        public final ActionListener<PyTorchResult> listener;

        public PendingResult(ActionListener<PyTorchResult> listener) {
            this.listener = Objects.requireNonNull(listener);
        }
    }
}
