/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * A {@link Collector} that forcibly early terminates collection after a certain number of hits have been collected.
 * Terminates the collection across all collectors by throwing an {@link EarlyTerminationException} once the threshold is reached.
 */
class TerminateAfterCollector implements Collector {
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

    private final int maxCountHits;
    private int numCollected;

    /**
     *
     * @param maxCountHits the number of hits to collect, after which the collection must be early terminated
     */
    TerminateAfterCollector(int maxCountHits) {
        this.maxCountHits = maxCountHits;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (numCollected >= maxCountHits) {
            earlyTerminate();
        }
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
                if (++numCollected > maxCountHits) {
                    earlyTerminate();
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        // this collector is not exhaustive, as it early terminates, and never needs scores
        return ScoreMode.TOP_DOCS;
    }

    private void earlyTerminate() {
        throw new EarlyTerminationException("early termination [CountBased]");
    }
}
