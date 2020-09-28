/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;

/**
 * AbstractHyperLogLogPlusPlus instance that only supports linear counting. The maximum number of hashes supported
 * by the structure is determined at construction time.
 *
 * This structure expects all the added values to be distinct and therefore there are no checks
 * if an element has been previously added.
 */
final class HyperLogLogPlusPlusSparse extends AbstractHyperLogLogPlusPlus implements Releasable {

    private final LinearCounting lc;

    /**
     * Create an sparse HLL++ algorithm where capacity is the maximum number of hashes this structure can hold
     * per bucket.
     */
    HyperLogLogPlusPlusSparse(int precision, BigArrays bigArrays, int capacity, int initialSize) {
        super(precision);
        this.lc = new LinearCounting(precision, bigArrays, capacity, initialSize);
    }

    @Override
    public long maxOrd() {
        return lc.sizes.size();
    }

    @Override
    public long cardinality(long bucketOrd) {
        return lc.cardinality(bucketOrd);
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return LINEAR_COUNTING;
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return lc.values(bucketOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        throw new IllegalArgumentException("Implementation does not support HLL structures");
    }

    @Override
    public void collect(long bucket, long hash) {
        lc.collect(bucket, hash);
    }

    @Override
    public void close() {
        Releasables.close(lc);
    }

    protected void addEncoded(long bucket, int encoded) {
        lc.addEncoded(bucket, encoded);
    }

    private static class LinearCounting extends AbstractLinearCounting implements Releasable {

        private final int capacity;
        private final BigArrays bigArrays;
        private final LinearCountingIterator iterator;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        // Number of elements stored.
        private IntArray values;
        private IntArray sizes;

        LinearCounting(int p, BigArrays bigArrays, int capacity, int initialSize) {
            super(p);
            this.bigArrays = bigArrays;
            this.capacity = capacity;
            IntArray values = null;
            IntArray sizes = null;
            boolean success = false;
            try {
                values = bigArrays.newIntArray(initialSize * capacity);
                sizes = bigArrays.newIntArray(initialSize);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(values, sizes);
                }
            }
            this.values = values;
            this.sizes = sizes;
            iterator = new LinearCountingIterator(this, capacity);
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            assert encoded != 0;
            return set(bucketOrd, encoded);
        }

        @Override
        protected int size(long bucketOrd) {
            if (bucketOrd >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(bucketOrd);
            assert size == recomputedSize(bucketOrd);
            return size;
        }

        @Override
        protected HashesIterator values(long bucketOrd) {
            iterator.reset(bucketOrd, size(bucketOrd));
            return iterator;
        }

        private long index(long bucketOrd, int index) {
            return (bucketOrd * capacity) + index;
        }

        private int get(long bucketOrd, int index) {
            long globalIndex = index(bucketOrd, index);
            if (values.size() < globalIndex) {
                return 0;
            }
            return values.get(globalIndex);
        }

        private int set(long bucketOrd, int value) {
            int size = size(bucketOrd);
            if (size == 0) {
                sizes = bigArrays.grow(sizes, bucketOrd + 1);
                values = bigArrays.grow(values, (bucketOrd + 1) * capacity);
            }
            values.set(index(bucketOrd, size), value);
            return sizes.increment(bucketOrd, 1);
        }

        private int recomputedSize(long bucketOrd) {
            for (int i = 0; i < capacity; ++i) {
                final int v = get(bucketOrd, i);
                if (v == 0) {
                    return i;
                }
            }
            return capacity;
        }

        @Override
        public void close() {
            Releasables.close(values, sizes);
        }
    }

    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final LinearCounting lc;
        private final int capacity;
        long start;
        long end;
        private int value, size;
        private long pos;

        LinearCountingIterator(LinearCounting lc, int capacity) {
            this.lc = lc;
            this.capacity = capacity;
        }

        void reset(long bucketOrd, int size) {
            this.start = bucketOrd * capacity;
            this.size = size;
            this.end = start + size;
            this.pos = start;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < end) {
               value = lc.values.get(pos++);
               return true;
            }
            return false;
        }

        @Override
        public int value() {
            return value;
        }
    }
}
