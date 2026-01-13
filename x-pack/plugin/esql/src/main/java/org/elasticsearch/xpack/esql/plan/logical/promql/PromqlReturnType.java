/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * The return type of a PromQL expression.
 */
public enum PromqlReturnType {
    INSTANT_VECTOR,
    RANGE_VECTOR,
    SCALAR;

    /**
     * A range vector is a set of time series containing a range of data points over time for each time series.
     *
     * @return true if the return type is a range vector.
     */
    public boolean isRangeVector() {
        return this == RANGE_VECTOR;
    }

    /**
     * An instant vector is a set of time series containing a single sample for each time series, all sharing the same timestamp.
     *
     * @return true if the return type is an instant vector.
     */
    public boolean isInstantVector() {
        return this == INSTANT_VECTOR;
    }

    /**
     * A scalar is a simple numeric floating point value.
     *
     * @return true if the return type is a scalar.
     */
    public boolean isScalar() {
        return this == SCALAR;
    }

    public static boolean isRangeVector(LogicalPlan plan) {
        return getReturnType(plan).isRangeVector();
    }

    public static boolean isInstantVector(LogicalPlan plan) {
        return getReturnType(plan).isInstantVector();
    }

    public static boolean isScalar(LogicalPlan plan) {
        return getReturnType(plan).isScalar();
    }

    private static PromqlReturnType getReturnType(LogicalPlan plan) {
        if (plan instanceof PromqlPlan hasReturnType) {
            return hasReturnType.returnType();
        }
        throw new IllegalArgumentException("Logical plan " + plan.getClass().getSimpleName() + " is not a PromqlPlan");
    }
}
