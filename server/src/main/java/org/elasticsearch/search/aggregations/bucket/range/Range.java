/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code range} aggregation. Defines multiple buckets, each associated with a pre-defined value range of a field,
 * and where the value of that fields in all documents in each bucket fall in the bucket's range.
 */
public interface Range extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific range
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

        /**
         * @return  The lower bound of the range
         */
        Object getFrom();

        /**
         * @return The string value for the lower bound of the range
         */
        String getFromAsString();

        /**
         * @return The upper bound of the range (excluding)
         */
        Object getTo();

        /**
         * @return The string value for the upper bound of the range (excluding)
         */
        String getToAsString();
    }

    /**
     * Return the buckets of this range aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

}
