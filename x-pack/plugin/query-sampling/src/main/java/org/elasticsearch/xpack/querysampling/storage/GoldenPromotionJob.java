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
 * Grows the golden dataset every so often, by what {@link GoldenGrower} does. Only the elected master does it. Every node has the
 * plugin, and a version that each of them makes would be a copy of the others, while the elected master is always exactly one node.
 * A round that finds the one before still going does nothing.
 */
public final class GoldenPromotionJob {

    private static final Logger logger = LogManager.getLogger(GoldenPromotionJob.class);

    /**
     * The shortest time between two rounds, so that a setting that is hardly more than zero does not keep the master busy.
     */
    static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1);

    private final BiConsumer<Integer, ActionListener<GoldenGrower.Result>> grow;
    private final BooleanSupplier electedMaster;
    private final AtomicBoolean running = new AtomicBoolean();
    private final LongAdder promotions = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private volatile int limit = 1;

    /**
     * @param grow          grows the dataset by a version of at most the number of queries that it is given
     * @param electedMaster whether this node is the elected master
     */
    public GoldenPromotionJob(BiConsumer<Integer, ActionListener<GoldenGrower.Result>> grow, BooleanSupplier electedMaster) {
        this.grow = grow;
        this.electedMaster = electedMaster;
    }

    /**
     * Follows the setting of the most queries of a version, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.GOLDEN_MAX_QUERIES, value -> this.limit = value);
    }

    /**
     * Starts running every {@code interval}, until the thread pool shuts down.
     */
    public Scheduler.Cancellable start(ThreadPool threadPool, Executor executor, TimeValue interval) {
        TimeValue effective = interval.compareTo(MIN_INTERVAL) < 0 ? MIN_INTERVAL : interval;
        return threadPool.scheduleWithFixedDelay(this::run, effective, executor);
    }

    /**
     * One round: grows the dataset, if this node is the one to do it and the round before is over.
     */
    public void run() {
        if (electedMaster.getAsBoolean() == false || running.compareAndSet(false, true) == false) {
            return;
        }
        ActionListener<GoldenGrower.Result> done = ActionListener.runAfter(ActionListener.wrap(result -> {
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

    /**
     * Versions that this node made.
     */
    public long promotions() {
        return promotions.sum();
    }

    /**
     * Rounds that did not go through.
     */
    public long failed() {
        return failed.sum();
    }
}
