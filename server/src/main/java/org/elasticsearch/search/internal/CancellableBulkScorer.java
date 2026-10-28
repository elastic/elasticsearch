/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A {@link BulkScorer} wrapper that runs a {@link Runnable} on a regular basis
 * so that the query can be interrupted.
 */
public final class CancellableBulkScorer extends BulkScorer {

    // we use the BooleanScorer window size as a base interval in order to make sure that we do not
    // slow down boolean queries
    private static final int INITIAL_INTERVAL = 1 << 12;

    private static final int MAX_INTERVAL = 1 << 14;

    // Threshold for adaptive interval growth, only increase the interval if the previous batch completed faster than this.
    private static final long INTERVAL_GROWTH_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final BulkScorer scorer;
    private final Runnable checkCancelled;

    public CancellableBulkScorer(BulkScorer scorer, Runnable checkCancelled) {
        this.scorer = Objects.requireNonNull(scorer);
        this.checkCancelled = Objects.requireNonNull(checkCancelled);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        int interval = INITIAL_INTERVAL;
        long lastCheckTime = System.nanoTime();

        while (min < max) {
            checkCancelled.run();
            lastCheckTime = System.nanoTime();

            final int newMax = (int) Math.min((long) min + interval, max);
            min = scorer.score(collector, acceptDocs, min, newMax);

            long elapsed = System.nanoTime() - lastCheckTime;
            if (elapsed < INTERVAL_GROWTH_THRESHOLD_NANOS) {
                interval = Math.min(interval << 1, MAX_INTERVAL);
            }
        }
        checkCancelled.run();
        return min;
    }

    @Override
    public long cost() {
        return scorer.cost();
    }

    // exposed for testing
    static int getMaxInterval() {
        return MAX_INTERVAL;
    }
}
