/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.FeatureFlag;
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
import java.util.function.Consumer;

/**
 * For capturing and processing ES|QL and search queries.
 * Queries are captured by wrapping the ActionListener, on response a capture context is built
 * and passed to the {@link QuerySink}s for processing. Sinks are loaded from the {@link QuerySinkRegistry}.
 * The entry point is {@link #interceptor(QuerySink)} returning interceptor for wrapping the actions
 * and handing off the repsonse to the sinks for processing.
 *
 * In async mode the captured queries are passed to the {@code query_capture} executor for processing.
 * In sync mode the processing is performed directly on the action's response thread before notifying
 * the listener.
 *
 * On class construction counters are added to the metrics registry for tracking submitted requests,
 * failures and dropped requests.
 */
public final class QueryCaptureService {
    /** Enables async worker processing; disabling this flag preserves the direct blocking behavior. */
    public static final FeatureFlag ASYNC_FEATURE_FLAG = new FeatureFlag("query_capture_async");

    private static final Logger logger = LogManager.getLogger(QueryCaptureService.class);
    private final boolean asynchronous;
    private final Executor executor;
    private final QuerySinkRegistry sinkRegistry;
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

    /** Combines the node's plugin sinks with its single capture executor and counters. */
    public QueryCaptureService(ThreadPool threadPool, MeterRegistry meterRegistry, QuerySinkRegistry sinkRegistry) {
        this(threadPool, meterRegistry, sinkRegistry, ASYNC_FEATURE_FLAG.isEnabled());
    }

    // Explicit mode selection lets tests exercise both paths independently of the build's feature flags.
    QueryCaptureService(ThreadPool threadPool, MeterRegistry meterRegistry, QuerySinkRegistry sinkRegistry, boolean asynchronous) {
        this.asynchronous = asynchronous;
        this.sinkRegistry = sinkRegistry;
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
     * Creates an interceptor with an action specific logger plus any sinks loaded from the registry.
     * @param queryLogger The action specific logger (ES|QL or search)
     * @return An interceptor for capturing and processing search activity
     */
    public <C extends QueryCaptureContext> Interceptor<C> interceptor(QuerySink<C> queryLogger) {
        return new Interceptor<>(sinkRegistry.createInterceptor(queryLogger));
    }

    /** Binds an action's sinks to this service */
    public final class Interceptor<C extends QueryCaptureContext> {
        private final QueryInterceptor<C> delegate;

        private Interceptor(QueryInterceptor<C> delegate) {
            this.delegate = delegate;
        }

        /**
         * Wraps the listener for response capturing and executes the action
         *
         * @param listener The listener to wrap
         * @param contextBuilder Capture context builder
         * @param action The search action
         */
        public <Req, R extends RefCounted> void wrapAndRun(
            ActionListener<R> listener,
            QueryCaptureContextBuilder<C, Req, R> contextBuilder,
            Consumer<ActionListener<R>> action
        ) {
            if (asynchronous) {
                delegate.wrapAndRun(listener, contextBuilder, action, QueryCaptureService.this);
            } else {
                delegate.wrapAndRun(listener, contextBuilder, action);
            }
        }
    }

    /**
     * Submits capture work, retaining the response before the caller can release it. A null response represents a failed query.
     * The runnable must only read the response and must not retain borrowed resources after returning.
     * Rejected captures are dropped; the executor records rejections in its thread-pool statistics.
     */
    void execute(Runnable capture, @Nullable RefCounted response) {
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
    void recordDropped(DropReason reason) {
        dropped.incrementBy(1, reason.attributes);
    }

    /** Includes failures caught by individual sinks, which do not escape to the worker. */
    void recordProcessingFailure() {
        processingFailures.increment();
    }
}
