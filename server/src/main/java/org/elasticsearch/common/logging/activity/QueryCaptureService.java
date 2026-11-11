/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Runs best-effort query capture on a bounded worker queue without blocking the search completion thread.
 */
public final class QueryCaptureService {
    private static final Logger logger = LogManager.getLogger(QueryCaptureService.class);
    private final Executor executor;
    private final LongCounter submitted;
    private final LongCounter completed;
    private final LongCounter processingFailures;
    private final LongCounter dropped;

    /** The reason why the captured query was not processed */
    public enum DropReason {
        QUEUE_FULL("queue_full"),
        CIRCUIT_BREAKER("circuit_breaker"), // TODO
        SAMPLER("sampler"); // TODO

        private final Map<String, Object> attributes;

        DropReason(String reason) {
            attributes = Map.of("es_query_capture_drop_reason", reason);
        }
    }

    /** Uses the node's single capture executor. */
    public QueryCaptureService(ThreadPool threadPool, MeterRegistry meterRegistry) {
        assert threadPool.executor(ThreadPool.Names.QUERY_CAPTURE) instanceof EsThreadPoolExecutor
            : "Query capture executor must be an instance of EsThreadPoolExecutor";

        executor = threadPool.executor(ThreadPool.Names.QUERY_CAPTURE);
        submitted = meterRegistry.registerLongCounter(
            "es.search.query_capture.submitted.total",
            "Captures submitted for processing",
            "count"
        );
        completed = meterRegistry.registerLongCounter(
            "es.search.query_capture.completed.total",
            "Captures whose worker processing finished, including failures",
            "count"
        );
        processingFailures = meterRegistry.registerLongCounter(
            "es.search.query_capture.processing_failures.total",
            "Failures during capture processing",
            "count"
        );
        dropped = meterRegistry.registerLongCounter(
            "es.search.query_capture.dropped.total",
            "Captures discarded before processing, by reason",
            "count"
        );
    }

    /**
     * Submits capture work, retaining the response before the caller can release it. A null response represents a failed query.
     * Rejected captures are dropped; the executor records rejections in its thread-pool statistics.
     * The runnable must only read the response and must not retain borrowed resources after returning.
     */
    public void execute(Runnable capture, @Nullable RefCounted response) {
        if (response != null) {
            response.incRef();
        }
        // EsThreadPoolExecutor delivers rejection to AbstractRunnable and calls onAfter on both execution and rejection.
        var task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                try {
                    capture.run();
                } finally {
                    completed.increment();
                }
            }

            @Override
            public void onFailure(Exception e) {
                recordProcessingFailure();
                logger.warn("query capture processing failed", e);
            }

            @Override
            public void onRejection(Exception e) {
                recordDropped(DropReason.QUEUE_FULL);
            }

            @Override
            public void onAfter() {
                if (response != null) {
                    response.decRef();
                }
            }
        };
        executor.execute(task);
        submitted.increment();
    }

    /** Records a pre-processing drop without acquiring response references or submitting work. */
    public void recordDropped(DropReason reason) {
        dropped.incrementBy(1, reason.attributes);
    }

    /** Includes failures caught by individual sinks, which do not escape to the worker. */
    void recordProcessingFailure() {
        processingFailures.increment();
    }
}
