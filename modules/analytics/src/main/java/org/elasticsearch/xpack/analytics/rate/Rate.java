/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
