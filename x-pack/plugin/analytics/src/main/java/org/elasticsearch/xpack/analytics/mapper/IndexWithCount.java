/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

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
                return new Iterator(bucketIndices, scale, 0);
            }

            @Override
            public OptionalLong maxBucketIndex() {
                if (bucketIndices.isEmpty()) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(bucketIndices.get(bucketIndices.size() - 1).index);
            }

            @Override
            public long valueCount() {
                throw new UnsupportedOperationException("not implemented");
            }
        };
    }

    public static List<IndexWithCount> fromIterator(CopyableBucketIterator iterator) {
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

        Iterator(List<IndexWithCount> buckets, int scale, int position) {
            this.buckets = buckets;
            this.scale = scale;
            this.position = position;
        }

        @Override
        public boolean hasNext() {
            return position < buckets.size();
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
            position++;
        }

        @Override
        public int scale() {
            return scale;
        }

        @Override
        public CopyableBucketIterator copy() {
            return new Iterator(buckets, scale, position);
        }
    }
}
