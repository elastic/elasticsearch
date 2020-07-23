/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

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

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;

/**
 * Hyperloglog counter, implemented based on pseudo code from
 * http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf and its appendix
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
 *
 * Trying to understand what this class does without having read the paper is considered adventurous.
 *
 * It supports storing several HyperLogLog structures which are identified by a bucket number.
 */
public final class HyperLogLog implements Releasable {

    private static final long[] HLLPRECISIONTOTHRESHOLDS = new long[] {
        2,
        5,
        11,
        23,
        47,
        95,
        191,
        383,
        767,
        1535,
        3071,
        6143,
        12287,
        24575,
        350000 };

    /**
     * Compute the required threshold for the given precision.
     */
    public static long thresholdFromPrecision(int precision) {
        if (precision < AbstractHyperLogLog.MIN_PRECISION) {
            throw new IllegalArgumentException("Min precision is " + AbstractHyperLogLog.MIN_PRECISION + ", got " + precision);
        }
        if (precision > AbstractHyperLogLog.MAX_PRECISION) {
            throw new IllegalArgumentException("Max precision is " + AbstractHyperLogLog.MAX_PRECISION + ", got " + precision);
        }
        return HLLPRECISIONTOTHRESHOLDS[precision - 4];
    }

    private final SingletonHyperLogLog hll;

    public HyperLogLog(int precision, BigArrays bigArrays, long initialBucketCount) {
        hll = new SingletonHyperLogLog(bigArrays, initialBucketCount, precision);
    }

    public int precision() {
        return hll.precision();
    }

    public long maxBucket() {
        return hll.runLens.size() >>> hll.precision();
    }

    public void addRunLen(long bucket, int register, int runLen) {
        hll.ensureCapacity(bucket + 1);
        hll.bucket = bucket;
        hll.addRunLen(register, runLen);
    }

    public long cardinality(long bucket) {
        hll.bucket = bucket;
        return hll.cardinality();
    }

    public AbstractHyperLogLog getHyperLogLog(long bucket) {
        hll.bucket = bucket;
        return hll;
    }

    @Override
    public void close() {
        Releasables.close(hll);
    }

    private static class SingletonHyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        private final HyperLogLogIterator iterator;
        // array for holding the runlens.
        private ByteArray runLens;
        // Defines the position of the data structure. Callers of this object should set this value
        // before calling any of the methods.
        protected long bucket;

        SingletonHyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens =  bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
            this.iterator = new HyperLogLogIterator(this, precision, m);
        }

        @Override
        protected void addRunLen(int register, int encoded) {
            final long bucketIndex = (bucket << p) + register;
            runLens.set(bucketIndex, (byte) Math.max(encoded, runLens.get(bucketIndex)));
        }

        @Override
        protected RunLenIterator getRunLens() {
            iterator.reset(bucket);
            return iterator;
        }

        protected void ensureCapacity(long numBuckets) {
            runLens = bigArrays.grow(runLens, numBuckets << p);
        }

        @Override
        public void close() {
            Releasables.close(runLens);
        }
    }

    private static class HyperLogLogIterator implements AbstractHyperLogLog.RunLenIterator {

        private final SingletonHyperLogLog hll;
        private final int m, p;
        int pos;
        long start;
        private byte value;

        HyperLogLogIterator(SingletonHyperLogLog hll, int p, int m) {
            this.hll = hll;
            this.m = m;
            this.p = p;
        }

        void reset(long bucket) {
            pos = 0;
            start = bucket << p;
        }

        @Override
        public boolean next() {
            if (pos < m) {
                value = hll.runLens.get(start + pos);
                pos++;
                return true;
            }
            return false;
        }

        @Override
        public byte value() {
            return value;
        }
    }
}
