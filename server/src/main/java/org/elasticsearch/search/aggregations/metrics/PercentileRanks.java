/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

/**
 * An aggregation that computes approximate percentiles given values.
 */
public interface PercentileRanks extends NumericMetricsAggregation.MultiValue, Iterable<Percentile> {

    String TYPE_NAME = "percentile_ranks";

    /**
     * Return the percentile for the given value.
     */
    double percent(double value);

    /**
     * Return the percentile for the given value as a String.
     */
    String percentAsString(double value);
}
