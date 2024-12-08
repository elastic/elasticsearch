/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * An aggregation that computes the rate of the values in the current bucket by adding all values in the bucket and dividing
 * it by the size of the bucket.
 */
public interface Rate extends NumericMetricsAggregation.SingleValue {

    /**
     * The rate.
     */
    double getValue();
}
