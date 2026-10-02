/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

/**
 * PromQL data types representing the possible return types of PromQL queries.
 */
public enum PromqlDataType {

    /**
     * An instant vector is a set of time series containing a single sample for each time series, all sharing the same timestamp.
     * Example:
     * <pre>
     * http_requests_total{job="api-server"} 1027
     * http_requests_total{job="frontend"} 314
     * </pre>
     * An instant vector is typically created by evaluating an instant selector:
     * <pre>
     * http_requests_total
     * </pre>
     * Or by applying a within series aggregation over a range vector:
     * <pre>
     * rate(http_requests_total[5m])
     * </pre>
     */
    INSTANT_VECTOR("instant_vector"),

    /**
     * A range vector is a set of time series containing a range of data points over time for each time series.
     * Example:
     * <pre>
     * http_requests_total{job="api-server"} [1020, 1023, 1027]
     * http_requests_total{job="frontend"} [310, 312, 314]
     * </pre>
     * A range vector is typically created by evaluating a range selector:
     * <pre>
     * http_requests_total[5m]
     * </pre>
     * Or by a subquery over an instant vector:
     * <pre>
     * http_requests_total[1m:10s]
     * </pre>
     */
    RANGE_VECTOR("range_vector"),

    /**
     * A scalar is a simple numeric floating point value.
     * Example:
     * <pre>
     * 42.0
     * </pre>
     * A scalar is typically created by evaluating a scalar literal:
     * <pre>
     * 42
     * </pre>
     * Or by calling a function that returns a scalar:
     * <pre>
     * pi()
     * </pre>
     * Or by converting an instant vector with a single element to a scalar.
     * <pre>
     * scalar(http_requests_total{job="api-server"})
     * </pre>
     */
    SCALAR("scalar");

    private final String displayName;

    PromqlDataType(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
