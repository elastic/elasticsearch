/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

public interface ExponentialHistogram {

    // scale of 38 is the largest scale where at the borders we don't run into problems due to floating point precision
    // theoretically, a MAX_SCALE of 51 would work and would still cover the entire range of double values
    // if we want to use something larger, we'll have to rework the math of converting from double to indices and back
    // One option would be to use "Quadruple": https://github.com/m-vokhm/Quadruple
    int MAX_SCALE = 38;

    // Only use 62 bit at max to allow to compute the difference between the smallest and largest index without causing overflow
    // Also the extra bit gives us room for some tricks for compact storage
    int MAX_INDEX_BITS = 62;
    long MAX_INDEX = (1L << MAX_INDEX_BITS) - 1;
    long MIN_INDEX = -MAX_INDEX;

    int scale();

    ZeroBucket zeroBucket();

    BucketIterator positiveBuckets();

    BucketIterator negativeBuckets();

    /**
     * Returns the highest populated bucket index, taking both negative and positive buckets into account;
     * If there are no buckets populated, Long.MIN_VALUE shall be returned.
     */
    long maximumBucketIndex();

    /**
     * Iterator over the non-empty buckets.
     */
    interface BucketIterator {
        boolean hasNext();

        long peekCount();

        long peekIndex();

        void advance();

        int scale();

        BucketIterator copy();
    }

}
