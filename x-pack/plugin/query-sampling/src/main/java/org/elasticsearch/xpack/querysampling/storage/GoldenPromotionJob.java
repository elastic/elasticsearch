/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Periodically runs {@link GoldenUpdater}. Only the elected master does, as every node has the plugin and each would
 * otherwise make a copy of the same version. A round that finds the previous one still going does nothing.
 */
public final class GoldenPromotionJob {

    private static final Logger logger = LogManager.getLogger(GoldenPromotionJob.class);

    // keeps a near-zero setting from keeping the master busy
    static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1);

    private final BiConsumer<Integer, ActionListener<GoldenUpdater.Result>> grow;
    private final BooleanSupplier electedMaster;
    private final AtomicBoolean running = new AtomicBoolean();
    private final LongAdder promotions = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private volatile int limit = 1;

    public GoldenPromotionJob(BiConsumer<Integer, ActionListener<GoldenUpdater.Result>> grow, BooleanSupplier electedMaster) {
        this.grow = grow;
        this.electedMaster = electedMaster;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.GOLDEN_MAX_QUERIES, value -> this.limit = value);
    }

    public Scheduler.Cancellable start(ThreadPool threadPool, Executor executor, TimeValue interval) {
        TimeValue effective = interval.compareTo(MIN_INTERVAL) < 0 ? MIN_INTERVAL : interval;
        return threadPool.scheduleWithFixedDelay(this::run, effective, executor);
    }

    public void run() {
        if (electedMaster.getAsBoolean() == false || running.compareAndSet(false, true) == false) {
            return;
        }
        ActionListener<GoldenUpdater.Result> done = ActionListener.runAfter(ActionListener.wrap(result -> {
            if (result.version() > 0) {
                promotions.increment();
                logger.info(
                    "made version [{}] of the golden dataset from [{}] with [{}] new queries, [{}] left out",
                    result.version(),
                    result.sourceVersion(),
                    result.added(),
                    result.dropped()
                );
            }
        }, e -> {
            failed.increment();
            logger.debug("failed to grow the golden dataset", e);
        }), () -> running.set(false));
        try {
            grow.accept(limit, done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }

    public long promotions() {
        return promotions.sum();
    }

    public long failed() {
        return failed.sum();
    }
}
