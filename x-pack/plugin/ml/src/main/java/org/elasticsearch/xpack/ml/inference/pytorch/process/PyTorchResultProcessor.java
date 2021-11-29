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
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;

import java.time.Instant;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PyTorchResultProcessor {

    private static final Logger logger = LogManager.getLogger(PyTorchResultProcessor.class);

    private final ConcurrentMap<String, PendingResult> pendingResults = new ConcurrentHashMap<>();

    private final String deploymentId;
    private volatile boolean isStopping;
    private final LongSummaryStatistics timingStats;
    private Instant lastUsed;

    public PyTorchResultProcessor(String deploymentId) {
        this.deploymentId = Objects.requireNonNull(deploymentId);
        this.timingStats = new LongSummaryStatistics();
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
    public void ignoreResposeWithoutNotifying(String requestId) {
        pendingResults.remove(requestId);
    }

    public void process(NativePyTorchProcess process) {
        try {
            Iterator<PyTorchResult> iterator = process.readResults();
            while (iterator.hasNext()) {
                PyTorchResult result = iterator.next();
                logger.trace(() -> new ParameterizedMessage("[{}] Parsed result with id [{}]", deploymentId, result.getRequestId()));
                processResult(result);
                PendingResult pendingResult = pendingResults.remove(result.getRequestId());
                if (pendingResult == null) {
                    logger.debug(() -> new ParameterizedMessage("[{}] no pending result for [{}]", deploymentId, result.getRequestId()));
                } else {
                    pendingResult.listener.onResponse(result);
                }
            }
        } catch (Exception e) {
            // No need to report error as we're stopping
            if (isStopping == false) {
                logger.error(new ParameterizedMessage("[{}] Error processing results", deploymentId), e);
            }
            pendingResults.forEach(
                (id, pendingResult) -> pendingResult.listener.onResponse(
                    new PyTorchResult(
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
                    new PyTorchResult(id, null, null, "inference canceled as process is stopping")
                )
            );
            pendingResults.clear();
        }
        logger.debug(() -> new ParameterizedMessage("[{}] Results processing finished", deploymentId));
    }

    public synchronized LongSummaryStatistics getTimingStats() {
        return new LongSummaryStatistics(timingStats.getCount(), timingStats.getMin(), timingStats.getMax(), timingStats.getSum());
    }

    private synchronized void processResult(PyTorchResult result) {
        if (result.isError() == false) {
            timingStats.accept(result.getTimeMs());
            lastUsed = Instant.now();
        }
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
        public final ActionListener<PyTorchResult> listener;

        public PendingResult(ActionListener<PyTorchResult> listener) {
            this.listener = Objects.requireNonNull(listener);
        }
    }
}
