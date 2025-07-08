/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

public interface ExponentialHistogramBuilder {

    void setZeroBucket(ZeroBucket zeroBucket);
    boolean tryAddBucket(long index, long count, boolean isPositive);
    void resetBuckets(int newScale);
}
