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
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A {@link Collector} that forcibly early terminates collection after a certain number of hits have been collected.
 * Terminates the collection across all collectors by throwing an {@link EarlyTerminationException} once the threshold is reached.
 */
class TerminateAfterCollector implements Collector {
    static final class EarlyTerminationException extends RuntimeException {
        @Override
        public Throwable fillInStackTrace() {
            // never re-thrown so we can save the expensive stacktrace
            return this;
        }
    }

    private final Collector collector;
    private final int maxCountHits;
    private int numCollected;

    /**
     *
     * @param maxCountHits the number of hits to collect, after which the collection must be early terminated
     */
    TerminateAfterCollector(Collector collector, int maxCountHits) {
        this.collector = collector;
        this.maxCountHits = maxCountHits;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (numCollected >= maxCountHits) {
            earlyTerminate();
        }

        LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(context);
        } catch (CollectionTerminatedException e) {
            leafCollector = null;
        }

        final LeafCollector finalLeafCollector = leafCollector;
        return new LeafCollector() {
            LeafCollector innerLeafCollector = finalLeafCollector;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (innerLeafCollector != null) {
                    innerLeafCollector.setScorer(new FilterScorable(scorer) {
                        @Override
                        public void setMinCompetitiveScore(float minScore) {
                            // Ignore calls to setMinCompetitiveScore so that if the wrapped collector wants to skip low-scoring hits,
                            // we are still able to terminate the collection when the threshold is reached. Otherwise, we'd stop counting.
                            // which would effectively cancel terminate_after for aggs: they would see all docs without early termination.
                        }
                    });
                }
            }

            @Override
            public void collect(int doc) throws IOException {
                if (++numCollected > maxCountHits) {
                    earlyTerminate();
                }
                if (innerLeafCollector != null) {
                    try {
                        innerLeafCollector.collect(doc);
                    } catch (CollectionTerminatedException e) {
                        innerLeafCollector = null;
                    }
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        // we can return the score mode from the wrapped collector: terminate_after on its own is not exhaustive and does not need scores
        return collector.scoreMode();
    }

    @Override
    public void setWeight(Weight weight) {
        collector.setWeight(weight);
    }

    private static void earlyTerminate() {
        throw new EarlyTerminationException();
    }
}
