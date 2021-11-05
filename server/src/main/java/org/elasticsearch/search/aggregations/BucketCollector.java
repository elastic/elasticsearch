/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * A Collector that can collect data in separate buckets.
 */
public abstract class BucketCollector implements Collector {

    public static final BucketCollector NO_OP_COLLECTOR = new BucketCollector() {

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext reader) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public void preCollection() throws IOException {
            // no-op
        }

        @Override
        public void postCollection() throws IOException {
            // no-op
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    @Override
    public abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException;

    /**
     * Pre collection callback.
     */
    public abstract void preCollection() throws IOException;

    /**
     * Post-collection callback.
     */
    public abstract void postCollection() throws IOException;

}
