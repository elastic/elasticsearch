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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

//TODO: probably we don't want to count total hits using an atomic integer but it was easier to do this for now
public class ConcurrentTotalHitCountCollector implements Collector {
    private Weight weight;
    private final AtomicInteger totalHits = new AtomicInteger(0);

    /** Returns how many hits matched the search. */
    public int getTotalHits() {
        return totalHits.get();
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        int leafCount = weight == null ? -1 : weight.count(context);
        if (leafCount != -1) {
            while (true) {
                int total = getTotalHits();
                if (totalHits.compareAndSet(total, total + leafCount)) {
                    throw new CollectionTerminatedException();
                }
            }
        }
        return new LeafCollector() {

            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) {
                while (true) {
                    int total = getTotalHits();
                    if (totalHits.compareAndSet(total, total + 1)) {
                        return;
                    }
                }
            }
        };
    }
}
