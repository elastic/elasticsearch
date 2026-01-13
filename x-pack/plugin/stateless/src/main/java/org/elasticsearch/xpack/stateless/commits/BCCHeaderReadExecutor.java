/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;

/**
 * Executor that limits concurrent BCC header reads to the shard read thread pool size
 * to prevent memory exhaustion when processing referenced BCCs during recovery operations.
 */
public class BCCHeaderReadExecutor implements Executor {
    private final Logger logger = LogManager.getLogger(BCCHeaderReadExecutor.class);

    private final ThrottledTaskRunner throttledFetchExecutor;

    public BCCHeaderReadExecutor(ThreadPool threadPool) {
        this.throttledFetchExecutor = new ThrottledTaskRunner(
            BCCHeaderReadExecutor.class.getCanonicalName(),
            // With this limit we don't hurt reading performance, but we avoid OOMing if
            // the latest BCC references too many BCCs.
            threadPool.info(SHARD_READ_THREAD_POOL).getMax(),
            threadPool.generic()
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
