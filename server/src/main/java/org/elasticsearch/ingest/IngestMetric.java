/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.metrics.CounterMetric;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>Metrics to measure ingest actions.
 * <p>This counts measure documents and timings for a given scope.
 * The scope is determined by the calling code. For example you can use this class to count all documents across all pipeline,
 * or you can use this class to count documents for a given pipeline or a specific processor.
 * This class does not make assumptions about it's given scope.
 */
class IngestMetric {

    private static final Logger logger = LogManager.getLogger(IngestMetric.class);

    /**
     * The time it takes to complete the measured item.
     */
    private final CounterMetric ingestTimeInNanos = new CounterMetric();
    /**
     * The current count of things being measure. Should most likely ever be 0 or 1.
     * Useful when aggregating multiple metrics to see how many things are in flight.
     */
    private final AtomicLong ingestCurrent = new AtomicLong();
    /**
     * The ever increasing count of things being measured
     */
    private final CounterMetric ingestCount = new CounterMetric();
    /**
     * The only increasing count of failures
     */
    private final CounterMetric ingestFailed = new CounterMetric();

    /**
     * Call this prior to the ingest action.
     */
    void preIngest() {
        ingestCurrent.incrementAndGet();
    }

    private static long doubleDecrementLogMillis = -1;

    /**
     * Call this after the performing the ingest action, even if the action failed.
     * @param ingestTimeInNanos The time it took to perform the action.
     */
    void postIngest(long ingestTimeInNanos) {
        // this is decrementAndGet with a zero floor, logging, and an assert
        ingestCurrent.updateAndGet(current -> {
            if (current > 0) {
                current--;
            } else {
                // then we've double decremented!
                long now = System.currentTimeMillis();
                if (now - doubleDecrementLogMillis > TimeUnit.HOURS.toMillis(1)) {
                    logger.warn("ingest metric current count double-decremented", new Throwable());
                    doubleDecrementLogMillis = now;
                }
                // fail at dev-time so we fix this
                assert false;
            }
            return current;
        });

        this.ingestTimeInNanos.inc(ingestTimeInNanos);
        ingestCount.inc();
    }

    /**
     * Call this if the ingest action failed.
     */
    void ingestFailed() {
        ingestFailed.inc();
    }

    /**
     * <p>Add two sets of metrics together.
     * <p><strong>Note -</strong> this method does <strong>not</strong> add the current count values.
     * The current count value is ephemeral and requires a increase/decrease operation pairs to keep the value correct.
     *
     * @param metrics The metric to add.
     */
    void add(IngestMetric metrics) {
        ingestCount.inc(metrics.ingestCount.count());
        ingestTimeInNanos.inc(metrics.ingestTimeInNanos.count());
        ingestFailed.inc(metrics.ingestFailed.count());
    }

    /**
     * Creates a serializable representation for these metrics.
     */
    IngestStats.Stats createStats() {
        // we track ingestTime at nanosecond resolution, but IngestStats uses millisecond resolution for reporting
        long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(ingestTimeInNanos.count());
        return new IngestStats.Stats(ingestCount.count(), ingestTimeInMillis, ingestCurrent.get(), ingestFailed.count());
    }
}
