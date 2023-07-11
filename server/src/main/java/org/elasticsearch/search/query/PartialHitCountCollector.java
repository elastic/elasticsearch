/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extension of {@link TotalHitCountCollector} that supports early termination of total hits counting based on a provided threshold.
 * Note that the total hit count may be retrieved from {@link org.apache.lucene.search.Weight#count(LeafReaderContext)},
 * in which case early termination is only applied to the leaves that do collect documents.
 */
class PartialHitCountCollector extends TotalHitCountCollector {

    private final HitsThresholdChecker hitsThresholdChecker;
    private boolean earlyTerminated;

    PartialHitCountCollector(int totalHitsThreshold) {
        this(new GlobalHitsThresholdChecker(totalHitsThreshold));
    }

    PartialHitCountCollector(HitsThresholdChecker hitsThresholdChecker) {
        this.hitsThresholdChecker = hitsThresholdChecker;
    }

    @Override
    public ScoreMode scoreMode() {
        // Does not need scores like TotalHitCountCollector (COMPLETE_NO_SCORES), but not exhaustive as it early terminates.
        return ScoreMode.TOP_DOCS;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        earlyTerminateIfNeeded();
        return new FilterLeafCollector(super.getLeafCollector(context)) {
            @Override
            public void collect(int doc) throws IOException {
                earlyTerminateIfNeeded();
                hitsThresholdChecker.incrementHitCount();
                super.collect(doc);
            }
        };
    }

    private void earlyTerminateIfNeeded() {
        if (hitsThresholdChecker.isThresholdReached()) {
            earlyTerminated = true;
            throw new CollectionTerminatedException();
        }
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }

    abstract static class HitsThresholdChecker {
        abstract void incrementHitCount();

        abstract boolean isThresholdReached();
    }

    static class GlobalHitsThresholdChecker extends HitsThresholdChecker {
        private final int totalHitsThreshold;
        private final AtomicInteger numCollected = new AtomicInteger();

        GlobalHitsThresholdChecker(int totalHitsThreshold) {
            this.totalHitsThreshold = totalHitsThreshold;
        }

        @Override
        void incrementHitCount() {
            numCollected.incrementAndGet();
        }

        @Override
        boolean isThresholdReached() {
            return numCollected.getAcquire() >= totalHitsThreshold;
        }
    }

    static final HitsThresholdChecker EXACT_HITS_COUNT_THRESHOLD_CHECKER = new HitsThresholdChecker() {
        @Override
        void incrementHitCount() {
            // noop
        }

        @Override
        boolean isThresholdReached() {
            return false;
        }
    };

    static class CollectorManager implements org.apache.lucene.search.CollectorManager<PartialHitCountCollector, Void> {
        private final HitsThresholdChecker hitsThresholdChecker;
        private boolean earlyTerminated;
        private int totalHits;

        CollectorManager(int totalHitsThreshold) {
            if (totalHitsThreshold == Integer.MAX_VALUE) {
                this.hitsThresholdChecker = PartialHitCountCollector.EXACT_HITS_COUNT_THRESHOLD_CHECKER;
            } else {
                this.hitsThresholdChecker = new PartialHitCountCollector.GlobalHitsThresholdChecker(totalHitsThreshold);
            }
        }

        @Override
        public PartialHitCountCollector newCollector() {
            return new PartialHitCountCollector(hitsThresholdChecker);
        }

        @Override
        public Void reduce(Collection<PartialHitCountCollector> collectors) throws IOException {
            assert totalHits == 0;
            for (PartialHitCountCollector collector : collectors) {
                this.totalHits += collector.getTotalHits();
                if (collector.hasEarlyTerminated()) {
                    earlyTerminated = true;
                }
            }
            return null;
        }

        public boolean hasEarlyTerminated() {
            return earlyTerminated;
        }

        public int getTotalHits() {
            return totalHits;
        }
    }
}
