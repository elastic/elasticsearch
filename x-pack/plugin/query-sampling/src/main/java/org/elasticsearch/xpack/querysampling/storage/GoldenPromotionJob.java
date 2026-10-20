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
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Grows the golden dataset with {@link GoldenUpdater}. {@link MasterTasks} runs it periodically, on the elected master
 * only, as each node would otherwise make a copy of the same version. A round that finds the previous one still going
 * does nothing.
 */
public final class GoldenPromotionJob {

    private static final Logger logger = LogManager.getLogger(GoldenPromotionJob.class);

    // keeps a near-zero setting from keeping the master busy
    static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1);

    private final BiConsumer<Integer, ActionListener<GoldenUpdater.Result>> grow;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile int limit = 1;

    public GoldenPromotionJob(BiConsumer<Integer, ActionListener<GoldenUpdater.Result>> grow) {
        this.grow = grow;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.GOLDEN_MAX_QUERIES, value -> this.limit = value);
    }

    /** The configured interval, but not so short that it keeps the master busy. */
    public static TimeValue interval(TimeValue configured) {
        return configured.compareTo(MIN_INTERVAL) < 0 ? MIN_INTERVAL : configured;
    }

    public void run() {
        if (running.compareAndSet(false, true) == false) {
            return;
        }
        ActionListener<GoldenUpdater.Result> done = ActionListener.runAfter(ActionListener.wrap(result -> {
            if (result.version() > 0) {
                logger.info(
                    "made version [{}] of the golden dataset from [{}] with [{}] new queries, [{}] left out",
                    result.version(),
                    result.sourceVersion(),
                    result.added(),
                    result.dropped()
                );
            }
        }, e -> { logger.debug("failed to grow the golden dataset", e); }), () -> running.set(false));
        try {
            grow.accept(limit, done);
        } catch (Exception e) {
            done.onFailure(e);
        }
    }
}
