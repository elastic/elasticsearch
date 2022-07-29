/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/** A collector that profiles how much time is spent calling it. */
final class ProfileCollector extends FilterCollector {

    private long time;

    /** Sole constructor. */
    ProfileCollector(Collector in) {
        super(in);
    }

    /** Return the wrapped collector. */
    public Collector getDelegate() {
        return in;
    }

    @Override
    public ScoreMode scoreMode() {
        final long start = System.nanoTime();
        try {
            return super.scoreMode();
        } finally {
            time += Math.max(1, System.nanoTime() - start);
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final long start = System.nanoTime();
        final LeafCollector inLeafCollector;
        try {
            inLeafCollector = super.getLeafCollector(context);
        } finally {
            time += Math.max(1, System.nanoTime() - start);
        }
        return new FilterLeafCollector(inLeafCollector) {

            @Override
            public void collect(int doc) throws IOException {
                final long start = System.nanoTime();
                try {
                    super.collect(doc);
                } finally {
                    time += Math.max(1, System.nanoTime() - start);
                }
            }

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                final long start = System.nanoTime();
                try {
                    super.setScorer(scorer);
                } finally {
                    time += Math.max(1, System.nanoTime() - start);
                }
            }
        };
    }

    /** Return the total time spent on this collector. */
    public long getTime() {
        return time;
    }

}
