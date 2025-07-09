/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * AbstractHyperLogLogPlusPlus instance that only supports linear counting. The maximum number of hashes supported
 * by the structure is determined at construction time.
 *
 * This structure expects all the added values to be distinct and therefore there are no checks
 * if an element has been previously added.
 */
final class HyperLogLogPlusPlusSparse extends AbstractHyperLogLogPlusPlus implements Releasable {

    // TODO: consider a hll sparse structure
    private final LinearCounting lc;

    /**
     * Create an sparse HLL++ algorithm where capacity is the maximum number of hashes this structure can hold
     * per bucket.
     */
    HyperLogLogPlusPlusSparse(int precision, BigArrays bigArrays, long initialBuckets) {
        super(precision);
        this.lc = new LinearCounting(precision, bigArrays, initialBuckets);
    }

    /** Needs to be called before adding elements into a bucket */
    protected void ensureCapacity(long bucketOrd, long size) {
        lc.ensureCapacity(bucketOrd, size);
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

        private final BigArrays bigArrays;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        // Number of elements stored.
        private ObjectArray<IntArray> values;
        private IntArray sizes;

        LinearCounting(int p, BigArrays bigArrays, long initialBuckets) {
            super(p);
            this.bigArrays = bigArrays;
            ObjectArray<IntArray> values = null;
            IntArray sizes = null;
            boolean success = false;
            try {
                values = bigArrays.newObjectArray(initialBuckets);
                sizes = bigArrays.newIntArray(initialBuckets);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(values, sizes);
                }
            }
            this.values = values;
            this.sizes = sizes;
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            assert encoded != 0;
            return set(bucketOrd, encoded);
        }

        protected void ensureCapacity(long bucketOrd, long size) {
            values = bigArrays.grow(values, bucketOrd + 1);
            sizes = bigArrays.grow(sizes, bucketOrd + 1);
            IntArray value = values.get(bucketOrd);
            if (value == null) {
                value = bigArrays.newIntArray(size);
            } else {
                value = bigArrays.grow(value, size);
            }
            values.set(bucketOrd, value);
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

        private HashesIterator values(long bucketOrd) {
            return new LinearCountingIterator(values.get(bucketOrd), size(bucketOrd));
        }

        private int set(long bucketOrd, int value) {
            // This assumes that ensureCapacity has been called before
            assert values.get(bucketOrd) != null : "Added a value without calling ensureCapacity";
            IntArray array = values.get(bucketOrd);
            int size = sizes.get(bucketOrd);
            array.set(size, value);
            return sizes.increment(bucketOrd, 1);
        }

        private int recomputedSize(long bucketOrd) {
            IntArray array = values.get(bucketOrd);
            if (array == null) {
                return 0;
            }
            for (int i = 0; i < array.size(); ++i) {
                final int v = array.get(i);
                if (v == 0) {
                    return i;
                }
            }
            return Math.toIntExact(array.size());
        }

        @Override
        public void close() {
            for (int i = 0; i < values.size(); i++) {
                Releasables.close(values.get(i));
            }
            Releasables.close(values, sizes);
        }
    }

    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final IntArray values;
        private final int size;
        private int value;
        private long pos;

        LinearCountingIterator(IntArray values, int size) {
            this.values = values;
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < size) {
                value = values.get(pos++);
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
