/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * An aggregation that computes the average of the values in the current bucket.
 */
public interface Avg extends NumericMetricsAggregation.SingleValue {

    /**
     * The average value.
     */
    double getValue();
}
