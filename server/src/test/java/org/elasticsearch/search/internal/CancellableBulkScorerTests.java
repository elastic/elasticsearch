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
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CancellableBulkScorerTests extends ESTestCase {

    /**
     * Verifies multiple cancellation checks occur during scoring (not just start/end).
     */
    public void testCancellationIsCheckedRegularly() throws IOException {
        AtomicInteger cancellationChecks = new AtomicInteger(0);
        AtomicBoolean cancelled = new AtomicBoolean(false);

        Runnable checkCancelled = () -> {
            cancellationChecks.incrementAndGet();
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        };

        int totalDocs = 100_000;
        BulkScorer scorer = new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
                return max;
            }

            @Override
            public long cost() {
                return totalDocs;
            }
        };

        CancellableBulkScorer cancellableBulkScorer = new CancellableBulkScorer(scorer, checkCancelled);
        cancellableBulkScorer.score(createNoOpCollector(), null, 0, totalDocs);
        assertThat("Should have multiple cancellation checks", cancellationChecks.get(), equalTo(9));
    }

    /**
     * Verifies that after cancellation is requested, at most MAX_INTERVAL additional documents are processed before scoring stops.
     */
    public void testCancellationStopsScoring() throws IOException {
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicInteger docsScored = new AtomicInteger(0);

        Runnable checkCancelled = () -> {
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        };

        int totalDocs = 100_000;
        int cancellationPoint = 10_000;
        BulkScorer scorer = new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                for (int i = min; i < max; i++) {
                    docsScored.incrementAndGet();
                    if (docsScored.get() == cancellationPoint) {
                        cancelled.set(true);
                    }
                }
                return max;
            }

            @Override
            public long cost() {
                return totalDocs;
            }
        };

        CancellableBulkScorer cancellableBulkScorer = new CancellableBulkScorer(scorer, checkCancelled);
        expectThrows(TaskCancelledException.class, () -> cancellableBulkScorer.score(createNoOpCollector(), null, 0, totalDocs));

        assertThat(
            "Should stop within MAX_INTERVAL of cancellation point",
            docsScored.get(),
            lessThan(cancellationPoint + CancellableBulkScorer.getMaxInterval())
        );
    }

    /**
     * Verifies intervals still grow for efficiency, but never exceed MAX_INTERVAL.
     * Ensures we didn't break the optimization for fast queries.
     */
    public void testIntervalGrowsGeometrically() throws IOException {
        AtomicInteger checkCount = new AtomicInteger(0);
        List<Integer> intervals = new ArrayList<>();

        BulkScorer scorer = new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
                intervals.add(max - min);
                return max;
            }

            @Override
            public long cost() {
                return 1_000_000;
            }
        };

        Runnable checkCancelled = checkCount::incrementAndGet;
        CancellableBulkScorer cancellableBulkScorer = new CancellableBulkScorer(scorer, checkCancelled);
        cancellableBulkScorer.score(createNoOpCollector(), null, 0, 200_000);

        for (int i = 1; i < intervals.size() - 1; i++) {
            assertTrue(
                "Intervals should grow: intervals["
                    + i
                    + "]="
                    + intervals.get(i)
                    + " should be >= intervals["
                    + (i - 1)
                    + "]="
                    + intervals.get(i - 1),
                intervals.get(i) >= intervals.get(i - 1)
            );
        }

        for (int interval : intervals) {
            assertThat("No interval should exceed MAX_INTERVAL", interval, lessThanOrEqualTo(CancellableBulkScorer.getMaxInterval()));
        }
    }

    private LeafCollector createNoOpCollector() {
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {}
        };
    }
}
