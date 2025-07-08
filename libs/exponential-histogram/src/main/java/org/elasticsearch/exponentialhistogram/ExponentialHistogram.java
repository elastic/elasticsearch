/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

public interface ExponentialHistogram {

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
