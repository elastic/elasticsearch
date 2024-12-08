/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationPath.PathElement;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * A {@link BucketCollector} that records collected doc IDs and buckets and
 * allows to replay a subset of the collected buckets.
 */
public abstract class DeferringBucketCollector extends BucketCollector {

    /** Sole constructor. */
    public DeferringBucketCollector() {}

    /** Set the deferred collectors. */
    public abstract void setDeferredCollector(Iterable<BucketCollector> deferredCollectors);

    /**
     * Replay the deferred hits on the selected buckets.
     */
    public abstract void prepareSelectedBuckets(LongArray selectedBuckets) throws IOException;

    /**
     * Wrap the provided aggregator so that it behaves (almost) as if it had
     * been collected directly.
     */
    public Aggregator wrap(final Aggregator in, BigArrays bigArrays) {
        return new WrappedAggregator(in);
    }

    protected static class WrappedAggregator extends Aggregator {
        private final Aggregator in;

        WrappedAggregator(Aggregator in) {
            this.in = in;
        }

        @Override
        public ScoreMode scoreMode() {
            return in.scoreMode();
        }

        @Override
        public void close() {
            in.close();
        }

        @Override
        public String name() {
            return in.name();
        }

        @Override
        public Aggregator parent() {
            return in.parent();
        }

        @Override
        public Aggregator subAggregator(String name) {
            return in.subAggregator(name);
        }

        @Override
        public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
            return in.buildAggregations(owningBucketOrds);
        }

        @Override
        public void releaseAggregations() {
            in.releaseAggregations();
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return in.buildEmptyAggregation();
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            in.collectDebugInfo(add);
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            throw new IllegalStateException(
                "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper."
            );
        }

        @Override
        public void preCollection() throws IOException {
            throw new IllegalStateException(
                "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper."
            );
        }

        @Override
        public void postCollection() throws IOException {
            throw new IllegalStateException(
                "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper."
            );
        }

        @Override
        public Aggregator resolveSortPath(PathElement next, Iterator<PathElement> path) {
            return in.resolveSortPath(next, path);
        }

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            throw new UnsupportedOperationException("Can't sort on deferred aggregations");
        }

        @Override
        public Aggregator[] subAggregators() {
            return in.subAggregators();
        }
    }

}
