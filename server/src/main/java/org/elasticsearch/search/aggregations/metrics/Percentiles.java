/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * An aggregation that computes approximate percentiles.
 */
public interface Percentiles extends NumericMetricsAggregation.MultiValue, Iterable<Percentile> {

    String TYPE_NAME = "percentiles";

    /**
     * Return the value associated with the provided percentile.
     */
    double percentile(double percent);

    /**
     * Return the value associated with the provided percentile as a String.
     */
    String percentileAsString(double percent);

}
