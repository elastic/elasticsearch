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

    /**
     * Call this after the performing the ingest action, even if the action failed.
     * @param ingestTimeInNanos The time it took to perform the action.
     */
    void postIngest(long ingestTimeInNanos) {
        long current = ingestCurrent.decrementAndGet();
        if (current < 0) {
            /*
             * This ought to never happen. However if it does, it's incredibly bad because ingestCurrent being negative causes a
             * serialization error that prevents the nodes stats API from working. So we're doing 3 things here:
             * (1) Log a stack trace at warn level so that the Elasticsearch engineering team can track down and fix the source of the
             * bug if it still exists
             * (2) Throw an AssertionError if assertions are enabled so that we are aware of the bug
             * (3) Increment the counter back up so that we don't hit serialization failures
             */
            logger.warn("Current ingest counter decremented below 0", new RuntimeException());
            assert false : "ingest metric current count double-decremented";
            ingestCurrent.incrementAndGet();
        }
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
        // It is possible for the current count to briefly drop below 0, causing serialization problems. See #90319
        long currentCount = Math.max(0, ingestCurrent.get());
        return new IngestStats.Stats(ingestCount.count(), ingestTimeInMillis, currentCount, ingestFailed.count());
    }
}
