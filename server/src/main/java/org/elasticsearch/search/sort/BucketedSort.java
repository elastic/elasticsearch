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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * Type specialized sort implementations designed for use in aggregations.
 */
public abstract class BucketedSort implements Releasable {
    // TODO priority queue semantics to support mulitiple hits in the buckets
    protected final BigArrays bigArrays;
    private final SortOrder order;
    private final DocValueFormat format;
    private long maxBucket = -1;

    private BucketedSort(BigArrays bigArrays, SortOrder order, DocValueFormat format) {
        this.bigArrays = bigArrays;
        this.order = order;
        this.format = format;
    }

    /**
     * The order of the sort.
     */
    public final SortOrder getOrder() {
        return order;
    }

    /**
     * The format to use when presenting the results.
     */
    public final DocValueFormat getFormat() {
        return format;
    }

    /**
     * Get the value for a bucket.
     */
    public final Object getValue(long bucket) {
        if (bucket > maxBucket) {
            return null;
        }
        return getValueForBucket(bucket);
    }

    /**
     * Get the {@linkplain Leaf} implementation that'll do that actual collecting.
     */
    public abstract Leaf forLeaf(LeafReaderContext ctx) throws IOException;

    /**
     * Does this sort need scores? Most don't, but sorting on {@code _score} does.
     */
    public abstract boolean needsScores();

    /**
     * The {@linkplain BigArray} backing this sort.
     */
    protected abstract BigArray buckets();

    /**
     * Grow the {@linkplain BigArray} backing this sort to account for new buckets.
     */
    protected abstract void grow(long minSize);

    /**
     * Get the value for a bucket. This will only be called if the bucket was hit.
     */
    protected abstract Object getValueForBucket(long bucket);

    @Override
    public void close() {
        buckets().close();
    }

    /**
     * Performs the actual collection against a {@linkplain LeafReaderContext}.
     */
    public abstract class Leaf implements ScorerAware {
        /**
         * Collect this doc, returning {@code true} if it is competitive.
         */
        public final boolean hit(int doc, long bucket) throws IOException {
            if (false == advanceExact(doc)) {
                return false;
            }
            if (bucket > maxBucket) {
                assert maxBucket + 1 == bucket :"expected bucket to be [" + (maxBucket + 1) + "] but was [" + bucket + "]";
                maxBucket = bucket;
                if (bucket >= buckets().size()) {
                    grow(bucket + 1);
                }
                setValue(doc, bucket);
                return true;
            }
            return setIfCompetitive(doc, bucket);
        }

        /**
         * Move the underlying data source reader to the doc and return
         * {@code true} if there is data for the sort value.
         */
        protected abstract boolean advanceExact(int doc) throws IOException;

        /**
         * Set the value for a particular bucket to the value that doc has for the sort.
         */
        protected abstract void setValue(int doc, long bucket) throws IOException;

        /**
         * If the value that doc has for the sort is competitive with the other values
         * then set it.
         */
        protected abstract boolean setIfCompetitive(int doc, long bucket) throws IOException;
    }

    public abstract static class ForLongs extends BucketedSort {
        private LongArray buckets = bigArrays.newLongArray(1);

        public ForLongs(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format) {
            super(bigArrays, sortOrder, format);
        }

        @Override
        public boolean needsScores() { return false; }

        @Override
        protected final BigArray buckets() { return buckets; }

        @Override
        protected final void grow(long minSize) {
            buckets = bigArrays.grow(buckets, minSize);
        }

        @Override
        public final Object getValueForBucket(long bucket) {
            return buckets.get(bucket);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract long docValue() throws IOException;

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setValue(int doc, long bucket) throws IOException {
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(int doc, long bucket) throws IOException {
                long docSort = docValue();
                long bestSort = buckets.get(bucket); 
                if (getOrder().reverseMul() * Double.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }
        }
    }

    public abstract static class ForDoubles extends BucketedSort {
        private DoubleArray buckets = bigArrays.newDoubleArray(1);

        public ForDoubles(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format) {
            super(bigArrays, sortOrder, format);
        }

        @Override
        public boolean needsScores() { return false; }

        @Override
        protected final BigArray buckets() { return buckets; }

        @Override
        protected final void grow(long minSize) {
            buckets = bigArrays.grow(buckets, minSize);
        }

        @Override
        public final Object getValueForBucket(long bucket) {
            return buckets.get(bucket);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract double docValue() throws IOException;

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setValue(int doc, long bucket) throws IOException {
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(int doc, long bucket) throws IOException {
                double docSort = docValue();
                double bestSort = buckets.get(bucket); 
                if (getOrder().reverseMul() * Double.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }
        }
    }

    public abstract static class ForFloats extends BucketedSort {
        private FloatArray buckets = bigArrays.newFloatArray(1);

        public ForFloats(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format) {
            super(bigArrays, sortOrder, format);
        }

        @Override
        protected final BigArray buckets() { return buckets; }

        @Override
        protected final void grow(long minSize) {
            buckets = bigArrays.grow(buckets, minSize);
        }

        @Override
        public final Object getValueForBucket(long bucket) {
            return buckets.get(bucket);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract float docValue() throws IOException;

            @Override
            protected final void setValue(int doc, long bucket) throws IOException {
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(int doc, long bucket) throws IOException {
                float docSort = docValue();
                float bestSort = buckets.get(bucket); 
                if (getOrder().reverseMul() * Float.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }

        }
    }
}
