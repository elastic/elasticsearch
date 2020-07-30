/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationPath.PathElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Iterator;

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
    public abstract void prepareSelectedBuckets(long... selectedBuckets) throws IOException;

    /**
     * Wrap the provided aggregator so that it behaves (almost) as if it had
     * been collected directly.
     */
    public Aggregator wrap(final Aggregator in) {
        return new WrappedAggregator(in);
    }

    protected class WrappedAggregator extends Aggregator {
        private Aggregator in;

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
        public SearchContext context() {
            return in.context();
        }

        @Override
        public Aggregator subAggregator(String name) {
            return in.subAggregator(name);
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            return in.buildAggregations(owningBucketOrds);
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return in.buildEmptyAggregation();
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
            throw new IllegalStateException(
                    "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper.");
        }

        @Override
        public void preCollection() throws IOException {
            throw new IllegalStateException(
                    "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper.");
        }

        @Override
        public void postCollection() throws IOException {
            throw new IllegalStateException(
                    "Deferred collectors cannot be collected directly. They must be collected through the recording wrapper.");
        }

        @Override
        public Aggregator resolveSortPath(PathElement next, Iterator<PathElement> path) {
            return in.resolveSortPath(next, path);
        }

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            throw new UnsupportedOperationException("Can't sort on deferred aggregations");
        }
    }

}
