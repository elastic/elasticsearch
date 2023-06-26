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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;

/**
 * A Collector that can collect data in separate buckets.
 */
public abstract class BucketCollector {

    public static final BucketCollector NO_OP_BUCKET_COLLECTOR = new BucketCollector() {

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) {
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

        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    public static final Collector NO_OP_COLLECTOR = new Collector() {

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    };

    public abstract LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException;

    /**
     * Pre collection callback.
     */
    public abstract void preCollection() throws IOException;

    /**
     * Post-collection callback.
     */
    public abstract void postCollection() throws IOException;

    /**
     *  Indicates what features are required from the scorer.
     */
    public abstract ScoreMode scoreMode();

    /**
     * Return this BucketCollector wrapped as a {@link Collector}
     */
    public final Collector asCollector() {
        return new BucketCollectorWrapper(this);
    }

    private record BucketCollectorWrapper(BucketCollector bucketCollector) implements Collector {

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return bucketCollector.getLeafCollector(new AggregationExecutionContext(context, null, null, null));
        }

        @Override
        public ScoreMode scoreMode() {
            return bucketCollector.scoreMode();
        }
    }
}
