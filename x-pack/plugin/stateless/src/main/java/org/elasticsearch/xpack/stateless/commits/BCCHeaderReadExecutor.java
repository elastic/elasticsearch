/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.InstrumentedThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;

/**
 * Executor that limits concurrent BCC header reads to the shard read thread pool size
 * to prevent memory exhaustion when processing referenced BCCs during recovery operations.
 */
public class BCCHeaderReadExecutor implements Executor {
    private final Logger logger = LogManager.getLogger(BCCHeaderReadExecutor.class);

    private final InstrumentedThrottledTaskRunner<ActionListener<Releasable>> throttledFetchExecutor;

    public BCCHeaderReadExecutor(ThreadPool threadPool, MeterRegistry meterRegistry) {
        this.throttledFetchExecutor = new InstrumentedThrottledTaskRunner<>(
            "bcc_header_read",
            // TODO revert before merging: QA-only, forces queueing so the metrics have something to show.
            // Production value: threadPool.info(SHARD_READ_THREAD_POOL).getMax() -- with that limit we don't hurt reading performance,
            // but we avoid OOMing if the latest BCC references too many BCCs.
            1,
            threadPool.generic(),
            meterRegistry,
            threadPool::relativeTimeInNanos
        );
    }

    @Override
    public void execute(Runnable command) {
        throttledFetchExecutor.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    command.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to read a BCC header", e);
            }

            @Override
            public String toString() {
                return command.toString();
            }
        });
    }
}
