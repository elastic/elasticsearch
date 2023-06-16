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

/**
 * Extension of {@link TotalHitCountCollector} that supports early termination of total hits tracking based on a provided threshold
 */
class PartialHitCountCollector extends TotalHitCountCollector {

    private final int totalHitsThreshold;
    private boolean earlyTerminated;

    PartialHitCountCollector(int totalHitsThreshold) {
        this.totalHitsThreshold = totalHitsThreshold;
    }

    @Override
    public ScoreMode scoreMode() {
        // Does not need scores like TotalHitCountCollector (COMPLETE_NO_SCORES), but not exhaustive as it early terminates.
        return ScoreMode.TOP_DOCS;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (getTotalHits() >= totalHitsThreshold) {
            earlyTerminate();
        }
        LeafCollector leafCollector = super.getLeafCollector(context);
        return new FilterLeafCollector(leafCollector) {
            @Override
            public void collect(int doc) throws IOException {
                super.collect(doc);
                if (getTotalHits() >= totalHitsThreshold) {
                    earlyTerminate();
                }
            }
        };
    }

    private void earlyTerminate() {
        earlyTerminated = true;
        throw new CollectionTerminatedException();
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
