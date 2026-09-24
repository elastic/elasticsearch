/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.PREWARM_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;

/**
 * Executor that limits concurrent BCC header reads to avoid memory exhaustion when processing referenced BCCs during
 * recovery operations. The tasks themselves run on the generic thread pool; the limit here is admission control only.
 */
public class BCCHeaderReadExecutor implements Executor {
    private final Logger logger = LogManager.getLogger(BCCHeaderReadExecutor.class);

    /**
     * Overrides how many BCC header reads may run concurrently. Values below {@code 1} mean the limit is derived from
     * the thread pool sizes instead. Not dynamic: the limit is fixed when the underlying task runner is constructed.
     */
    public static final Setting<Integer> MAX_CONCURRENCY_SETTING = Setting.intSetting(
        "stateless.commits.bcc_header_read.max_concurrency",
        -1,
        -1,
        Setting.Property.NodeScope
    );

    private final ThrottledTaskRunner throttledFetchExecutor;

    public BCCHeaderReadExecutor(Settings settings, ThreadPool threadPool) {
        this.throttledFetchExecutor = new ThrottledTaskRunner(
            BCCHeaderReadExecutor.class.getCanonicalName(),
            maxConcurrency(settings, threadPool),
            threadPool.generic()
        );
    }

    static int maxConcurrency(Settings settings, ThreadPool threadPool) {
        return maxConcurrency(
            MAX_CONCURRENCY_SETTING.get(settings),
            threadPool.info(PREWARM_THREAD_POOL).getMax(),
            threadPool.info(SHARD_READ_THREAD_POOL).getMax()
        );
    }

    /**
     * Header reads happen while prewarming and recovering shards, so the limit scales with the prewarm pool, which
     * grows with the processor count on larger nodes. The shard read pool size acts as a floor so that the limit never
     * drops below the value it was originally derived from, which on small nodes exceeds the prewarm pool size.
     */
    static int maxConcurrency(int override, int prewarmMaxThreads, int shardReadMaxThreads) {
        if (override > 0) {
            return override;
        }
        return Math.max(prewarmMaxThreads, shardReadMaxThreads);
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
