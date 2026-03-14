/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.CopyableBucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * An exponential histogram bucket represented by its index and associated bucket count.
 * @param index the index of the bucket
 * @param count the number of values in that bucket.
 */
public record IndexWithCount(long index, long count) {

    public static ExponentialHistogram.Buckets asBuckets(int scale, List<IndexWithCount> bucketIndices) {
        return new ExponentialHistogram.Buckets() {

            @Override
            public CopyableBucketIterator iterator() {
                return Iterator.create(bucketIndices, scale);
            }

            @Override
            public CopyableBucketIterator reverseIterator() {
                return Iterator.createReversed(bucketIndices, scale);
            }

            @Override
            public OptionalLong maxBucketIndex() {
                if (bucketIndices.isEmpty()) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(bucketIndices.get(bucketIndices.size() - 1).index);
            }

            @Override
            public int bucketCount() {
                return bucketIndices.size();
            }

            @Override
            public long valueCount() {
                throw new UnsupportedOperationException("not implemented");
            }
        };
    }

    public static List<IndexWithCount> fromIterator(BucketIterator iterator) {
        List<IndexWithCount> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(new IndexWithCount(iterator.peekIndex(), iterator.peekCount()));
            iterator.advance();
        }
        return result;
    }

    private static class Iterator implements CopyableBucketIterator {
        private final List<IndexWithCount> buckets;
        private final int scale;
        private int position;
        private final boolean iterateForwards;

        static Iterator create(List<IndexWithCount> buckets, int scale) {
            return new Iterator(buckets, scale, 0, true);
        }

        static Iterator createReversed(List<IndexWithCount> buckets, int scale) {
            return new Iterator(buckets, scale, buckets.size() - 1, false);
        }

        private Iterator(List<IndexWithCount> buckets, int scale, int position, boolean iterateForwards) {
            this.buckets = buckets;
            this.scale = scale;
            this.position = position;
            this.iterateForwards = iterateForwards;
        }

        @Override
        public boolean hasNext() {
            return iterateForwards ? position < buckets.size() : position >= 0;
        }

        @Override
        public long peekCount() {
            return buckets.get(position).count;
        }

        @Override
        public long peekIndex() {
            return buckets.get(position).index;
        }

        @Override
        public void advance() {
            if (iterateForwards) {
                position++;
            } else {
                position--;
            }
        }

        @Override
        public int scale() {
            return scale;
        }

        @Override
        public CopyableBucketIterator copy() {
            return new Iterator(buckets, scale, position, iterateForwards);
        }
    }
}
