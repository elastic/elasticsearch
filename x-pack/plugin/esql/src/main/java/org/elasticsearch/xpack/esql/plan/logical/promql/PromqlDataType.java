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
     */
    INSTANT_VECTOR,
    /**
     * A range vector is a set of time series containing a range of data points over time for each time series.
     */
    RANGE_VECTOR,
    /**
     * A scalar is a simple numeric floating point value.
     */
    SCALAR;
}
