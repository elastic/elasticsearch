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

public class PyTorchResultProcessor {

    private static final Logger logger = LogManager.getLogger(PyTorchResultProcessor.class);

    private final ConcurrentMap<String, PendingResult> pendingResults = new ConcurrentHashMap<>();

    private final String deploymentId;
    private volatile boolean isStopping;
    private final LongSummaryStatistics timingStats;
    private final Consumer<ThreadSettings> threadSettingsConsumer;
    private Instant lastUsed;

    public PyTorchResultProcessor(String deploymentId, Consumer<ThreadSettings> threadSettingsConsumer) {
        this.deploymentId = Objects.requireNonNull(deploymentId);
        this.timingStats = new LongSummaryStatistics();
        this.threadSettingsConsumer = Objects.requireNonNull(threadSettingsConsumer);
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

    public void process(NativePyTorchProcess process) {
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

    private void processInferenceResult(PyTorchInferenceResult inferenceResult) {
        logger.trace(() -> new ParameterizedMessage("[{}] Parsed result with id [{}]", deploymentId, inferenceResult.getRequestId()));
        if (inferenceResult.isError() == false) {
            synchronized (this) {
                timingStats.accept(inferenceResult.getTimeMs());
                lastUsed = Instant.now();
            }
        }
        PendingResult pendingResult = pendingResults.remove(inferenceResult.getRequestId());
        if (pendingResult == null) {
            logger.debug(() -> new ParameterizedMessage("[{}] no pending result for [{}]", deploymentId, inferenceResult.getRequestId()));
        } else {
            pendingResult.listener.onResponse(inferenceResult);
        }
    }

    public synchronized LongSummaryStatistics getTimingStats() {
        return new LongSummaryStatistics(timingStats.getCount(), timingStats.getMin(), timingStats.getMax(), timingStats.getSum());
    }

    public synchronized Instant getLastUsed() {
        return lastUsed;
    }

    public int numberOfPendingResults() {
        return pendingResults.size();
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
