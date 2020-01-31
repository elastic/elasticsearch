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
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * Type specialized sort implementations designed for use in aggregations.
 */
public abstract class BucketedSort implements Releasable {
    // TODO priority queue semantics to support multiple hits in the buckets
    protected final BigArrays bigArrays;
    private final SortOrder order;
    private final DocValueFormat format;

    public BucketedSort(BigArrays bigArrays, SortOrder order, DocValueFormat format) {
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
     * The format to use when presenting the values.
     */
    public final DocValueFormat getFormat() {
        return format;
    }

    /**
     * Get the value for a bucket if it has been collected, null otherwise.
     */
    public final SortValue getValue(long bucket) {
        if (bucket >= buckets().size()) {
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
     * This will only be called if the array is too small.
     */
    protected abstract void grow(long minSize);

    /**
     * Get the value for a bucket. This will only be called if the bucket was collected.
     */
    protected abstract SortValue getValueForBucket(long bucket);

    /**
     * Performs the actual collection against a {@linkplain LeafReaderContext}.
     */
    public abstract class Leaf implements ScorerAware {
        /**
         * Collect this doc, returning {@code true} if it is competitive.
         */
        public final boolean collectIfCompetitive(int doc, long bucket) throws IOException {
            if (false == advanceExact(doc)) {
                return false;
            }
            if (bucket >= buckets().size()) {
                grow(bucket + 1);
                setValue(bucket);
                return true;
            }
            return setIfCompetitive(bucket);
        }

        /**
         * Move the underlying data source reader to the doc and return
         * {@code true} if there is data for the sort value.
         */
        protected abstract boolean advanceExact(int doc) throws IOException;

        /**
         * Set the value for a particular bucket to the value that doc has for the sort.
         * This is called when we're *sure* we haven't yet seen the bucket.
         */
        protected abstract void setValue(long bucket) throws IOException;

        /**
         * If the value that doc has for the sort is competitive with the other values
         * then set it. This is called for buckets we *might* have already seen. So
         * implementers will have to check for "empty" buckets in their own way. The
         * vaguery here is for two reasons:
         * <ul>
         * <li>When we see a bucket that won't fit in our arrays we oversize them so
         *     we don't have to grow them by 1 every time.</li>
         * <li>Buckets don't always arrive in order and our storage is "dense" on the
         *     bucket ordinal. For example, we might get bucket number 4 grow the array
         *     to fit it, and *then* get bucket number 3.</li>
         * </ul>
         */
        protected abstract boolean setIfCompetitive(long bucket) throws IOException;
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code double} keys.
     */
    public abstract static class ForDoubles extends BucketedSort {
        private DoubleArray buckets = bigArrays.newDoubleArray(1);

        public ForDoubles(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format) {
            super(bigArrays, sortOrder, format);
            // NaN is a sentinel value for "unused"
            buckets.set(0, Double.NaN);
        }

        @Override
        public boolean needsScores() { return false; }

        @Override
        protected final BigArray buckets() { return buckets; }

        @Override
        protected final void grow(long minSize) {
            long oldSize = buckets.size();
            buckets = bigArrays.grow(buckets, minSize);
            buckets.fill(oldSize, buckets.size(), Double.NaN);
        }

        @Override
        public final SortValue getValueForBucket(long bucket) {
            double val = buckets.get(bucket);
            if (Double.isNaN(val)) {
                return null;
            }
            return SortValue.from(val);
        }

        @Override
        public final void close() {
            buckets.close();
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract double docValue() throws IOException;

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setValue(long bucket) throws IOException {
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(long bucket) throws IOException {
                double docSort = docValue();
                double bestSort = buckets.get(bucket);
                // The NaN check is important here because it needs to always lose.
                if (false == Double.isNaN(bestSort) && getOrder().reverseMul() * Double.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }
        }
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code float} keys.
     */
    public abstract static class ForFloats extends BucketedSort {
        private FloatArray buckets = bigArrays.newFloatArray(1);

        public ForFloats(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format) {
            super(bigArrays, sortOrder, format);
            // NaN is a sentinel value for "unused"
            buckets.set(0, Float.NaN);
        }

        @Override
        protected final BigArray buckets() { return buckets; }

        @Override
        protected final void grow(long minSize) {
            long oldSize = buckets.size();
            buckets = bigArrays.grow(buckets, minSize);
            buckets.fill(oldSize, buckets.size(), Float.NaN);
        }

        @Override
        public final SortValue getValueForBucket(long bucket) {
            float val = buckets.get(bucket);
            if (Float.isNaN(val)) {
                return null;
            }
            return SortValue.from(val);
        }

        @Override
        public final void close() {
            buckets.close();
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract float docValue() throws IOException;

            @Override
            protected final void setValue(long bucket) throws IOException {
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(long bucket) throws IOException {
                float docSort = docValue();
                float bestSort = buckets.get(bucket);
                // The NaN check is important here because it needs to always lose.
                if (false == Float.isNaN(bestSort) && getOrder().reverseMul() * Float.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }

        }
    }

    /**
     * Superclass for implementations of {@linkplain BucketedSort} for {@code long} keys.
     */
    public abstract static class ForLongs extends BucketedSort {
        /**
         * Tracks which buckets have been seen before so we can *always*
         * set the value in that case. We need this because there isn't a
         * sentinel value in the {@code long} type that we can use for this
         * like NaN in {@code double} or {@code float}.
         */
        private BitArray seen = new BitArray(1, bigArrays);
        /**
         * The actual values.
         */
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
        public final SortValue getValueForBucket(long bucket) {
            if (bucket > Integer.MAX_VALUE) {
                /* We throw exceptions if we try to collect buckets bigger
                 * than an int so we *can't* have seen any of these. */
                return null;
            }
            if (false == seen.get((int) bucket)) {
                /* Buckets we haven't seen must be null here so we can
                 * skip "gaps" in seen buckets. */
                return null;
            }
            return SortValue.from(buckets.get(bucket));
        }

        @Override
        public final void close() {
            Releasables.close(seen, buckets);
        }

        protected abstract class Leaf extends BucketedSort.Leaf {
            protected abstract long docValue() throws IOException;

            @Override
            public final void setScorer(Scorable scorer) {}

            @Override
            protected final void setValue(long bucket) throws IOException {
                seen.set(bucketIsInt(bucket));
                buckets.set(bucket, docValue());
            }

            @Override
            protected final boolean setIfCompetitive(long bucket) throws IOException {
                long docSort = docValue();
                int intBucket = bucketIsInt(bucket);
                if (false == seen.get(intBucket)) {
                    seen.set(intBucket);
                    buckets.set(bucket, docSort);
                    return true;
                }
                long bestSort = buckets.get(bucket); 
                if (getOrder().reverseMul() * Double.compare(bestSort, docSort) <= 0) {
                    return false;
                }
                buckets.set(bucket, docSort);
                return true;
            }

            private int bucketIsInt(long bucket) {
                if (bucket > Integer.MAX_VALUE) {
                    throw new UnsupportedOperationException("Long sort keys don't support more than [" + Integer.MAX_VALUE + "] buckets");
                    // I don't feel too bad about that because it'd take about 16 GB of memory....
                }
                return (int) bucket;
            }
        }
    }
}
