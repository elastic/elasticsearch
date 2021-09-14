/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

public interface Boxplot extends NumericMetricsAggregation.MultiValue {

    /**
     * @return The minimum value of all aggregated values.
     */
    double getMin();

    /**
     * @return The maximum value of all aggregated values.
     */
    double getMax();

    /**
     * @return The first quartile of all aggregated values.
     */
    double getQ1();

    /**
     * @return The second quartile of all aggregated values.
     */
    double getQ2();

    /**
     * @return The third quartile of all aggregated values.
     */
    double getQ3();

    /**
     * @return The minimum value of all aggregated values as a String.
     */
    String getMinAsString();

    /**
     * @return The maximum value of all aggregated values as a String.
     */
    String getMaxAsString();

    /**
     * @return The first quartile of all aggregated values as a String.
     */
    String getQ1AsString();

    /**
     * @return The second quartile of all aggregated values as a String.
     */
    String getQ2AsString();

    /**
     * @return The third quartile of all aggregated values as a String.
     */
    String getQ3AsString();

}
