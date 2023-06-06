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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Collector} that early terminates collection after <code>maxCountHits</code> docs have been collected.
 */
class EarlyTerminatingCollector extends FilterCollector {
    static final class EarlyTerminationException extends RuntimeException {
        private EarlyTerminationException(String msg) {
            super(msg);
        }

        @Override
        public Throwable fillInStackTrace() {
            // never re-thrown so we can save the expensive stacktrace
            return this;
        }
    }

    private final ThresholdChecker thresholdChecker;
    private final boolean forceTermination;

    /**
     * Creates a new early terminating collector.
     *
     * @param delegate The delegated collector.
     * @param maxCountHits The number of documents to collect before termination.
     * @param forceTermination Whether the collection should be terminated with an exception ({@link EarlyTerminationException})
     *                         that is not caught by other {@link Collector} or with a {@link CollectionTerminatedException} otherwise.
     */
    EarlyTerminatingCollector(final Collector delegate, int maxCountHits, boolean forceTermination) {
        super(delegate);
        this.thresholdChecker = ThresholdChecker.create(maxCountHits);
        this.forceTermination = forceTermination;
    }
    /**
     * Creates a new early terminating collector.
     *
     * @param delegate The delegated collector.
     * @param thresholdChecker The component responsible for checking whether the collection should be terminated or not.
     * @param forceTermination Whether the collection should be terminated with an exception ({@link EarlyTerminationException})
     *                         that is not caught by other {@link Collector} or with a {@link CollectionTerminatedException} otherwise.
     */
    EarlyTerminatingCollector(final Collector delegate, ThresholdChecker thresholdChecker, boolean forceTermination) {
        super(delegate);
        this.thresholdChecker = thresholdChecker;
        this.forceTermination = forceTermination;
    }

    @Override
    public ScoreMode scoreMode() {
        // Let the query know that this collector doesn't intend to collect all hits.
        ScoreMode scoreMode = super.scoreMode();
        if (scoreMode.isExhaustive()) {
            scoreMode = scoreMode.needsScores() ? ScoreMode.TOP_DOCS_WITH_SCORES : ScoreMode.TOP_DOCS;
        }
        return scoreMode;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (thresholdChecker.isThresholdReached()) {
            earlyTerminate();
        }
        return new FilterLeafCollector(super.getLeafCollector(context)) {
            @Override
            public void collect(int doc) throws IOException {
                thresholdChecker.incrementHitCount();
                if (thresholdChecker.isThresholdReached()) {
                    earlyTerminate();
                }
                super.collect(doc);
            }
        };
    }

    private void earlyTerminate() {
        thresholdChecker.earlyTerminate();
        if (forceTermination) {
            throw new EarlyTerminationException("early termination [CountBased]");
        } else {
            throw new CollectionTerminatedException();
        }
    }

    /**
     * Returns true if this collector has early terminated.
     */
    boolean hasEarlyTerminated() {
        return thresholdChecker.hasEarlyTerminated();
    }

    abstract static class ThresholdChecker {
        abstract boolean isThresholdReached();
        abstract void incrementHitCount();
        abstract void earlyTerminate();
        abstract boolean hasEarlyTerminated();

        static ThresholdChecker create(int numHitsThreshold) {
            return new ThresholdChecker() {
                private int numCollected = 0;
                private boolean earlyTerminated;

                @Override
                boolean isThresholdReached() {
                    return numCollected > numHitsThreshold;
                }

                @Override
                void incrementHitCount() {
                    numCollected++;
                }

                @Override
                void earlyTerminate() {
                    earlyTerminated = true;
                }

                @Override
                boolean hasEarlyTerminated() {
                    return earlyTerminated;
                }
            };
        }

        static ThresholdChecker createShared(int numHitsThreshold) {
            return new ThresholdChecker() {
                private final AtomicInteger numCollected = new AtomicInteger();
                private volatile boolean earlyTerminated;

                @Override
                boolean isThresholdReached() {
                    return numCollected.getAcquire() > numHitsThreshold;
                }

                @Override
                void incrementHitCount() {
                    numCollected.incrementAndGet();
                }

                @Override
                void earlyTerminate() {
                    earlyTerminated = true;
                }

                @Override
                boolean hasEarlyTerminated() {
                    return earlyTerminated;
                }
            };
        }
    }
}
