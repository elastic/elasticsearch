/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Marker interface for PromQL-specific logical plan nodes.
 */
public interface PromqlPlan {

    /**
     * The PromQL return type of this plan node.
     */
    PromqlDataType returnType();

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a range vector
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsRangeVector(LogicalPlan plan) {
        return getReturnType(plan) == PromqlDataType.RANGE_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns an instant vector
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsInstantVector(LogicalPlan plan) {
        return getReturnType(plan) == PromqlDataType.INSTANT_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a scalar
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsScalar(LogicalPlan plan) {
        return getReturnType(plan) == PromqlDataType.SCALAR;
    }

    static PromqlDataType getReturnType(@Nullable LogicalPlan plan) {
        return switch (plan) {
            case PromqlPlan promqlPlan -> promqlPlan.returnType();
            case null -> null;
            default -> throw new IllegalArgumentException("Logical plan " + plan.getClass().getSimpleName() + " is not a PromqlPlan");
        };
    }
}
