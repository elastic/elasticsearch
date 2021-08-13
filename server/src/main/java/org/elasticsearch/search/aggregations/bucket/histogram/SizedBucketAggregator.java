/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;

/**
 * An aggregator capable of reporting bucket sizes in requested units. Used by RateAggregator for calendar-based buckets.
 */
public interface SizedBucketAggregator {
    /**
     * Reports size of the particular bucket in requested units.
     */
    double bucketSize(long bucket, Rounding.DateTimeUnit unit);

    /**
     * Reports size of all buckets in requested units. Throws an exception if it is not possible to calculate without knowing
     * the concrete bucket.
     */
    double bucketSize(Rounding.DateTimeUnit unit);
}
