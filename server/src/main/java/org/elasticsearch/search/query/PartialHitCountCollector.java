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
 * Extension of {@link TotalHitCountCollector} that supports early termination of total hits counting based on a provided threshold.
 * Note that the total hit count may be retrieved from {@link org.apache.lucene.search.Weight#count(LeafReaderContext)},
 * in which case early termination is only applied to the leaves that do collect documents.
 */
class PartialHitCountCollector extends TotalHitCountCollector {

    private final int totalHitsThreshold;
    // we could reuse the counter that TotalHitCountCollector has and exposes through getTotalHits(),
    // but that would make us early terminate also when retrieving count from Weight#count and would
    // cause a behaviour that's difficult to explain and test.
    private int numCollected = 0;
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
        if (numCollected >= totalHitsThreshold) {
            earlyTerminateIfNeeded();
        }
        return new FilterLeafCollector(super.getLeafCollector(context)) {
            @Override
            public void collect(int doc) throws IOException {
                if (++numCollected > totalHitsThreshold) {
                    earlyTerminateIfNeeded();
                }
                super.collect(doc);
            }
        };
    }

    private void earlyTerminateIfNeeded() {
        earlyTerminated = true;
        throw new CollectionTerminatedException();
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
