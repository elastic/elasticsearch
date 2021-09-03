/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * Statistics over a set of values (either aggregated over field data or scripts)
 */
public interface Stats extends NumericMetricsAggregation.MultiValue {

    /**
     * @return The number of values that were aggregated.
     */
    long getCount();

    /**
     * @return The minimum value of all aggregated values.
     */
    double getMin();

    /**
     * @return The maximum value of all aggregated values.
     */
    double getMax();

    /**
     * @return The avg value over all aggregated values.
     */
    double getAvg();

    /**
     * @return The sum of aggregated values.
     */
    double getSum();

    /**
     * @return The minimum value of all aggregated values as a String.
     */
    String getMinAsString();

    /**
     * @return The maximum value of all aggregated values as a String.
     */
    String getMaxAsString();

    /**
     * @return The avg value over all aggregated values as a String.
     */
    String getAvgAsString();

    /**
     * @return The sum of aggregated values as a String.
     */
    String getSumAsString();

}
