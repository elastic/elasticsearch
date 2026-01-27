/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Marker interface for PromQL-specific logical plan nodes.
 */
public interface PromqlPlan {

    /**
     * Returns any grouping attributes, for example those added via {@code by(...)},
     * or {@link FieldAttribute#timeSeriesAttribute(Source)} (group by all).
     * <p>
     * Note: The value and step column are added by {@link PromqlCommand#output()}
     * and should not be added by implementations of this interface.
     */
    List<Attribute> output();

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
        return getType(plan) == PromqlDataType.RANGE_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns an instant vector
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsInstantVector(LogicalPlan plan) {
        return getType(plan) == PromqlDataType.INSTANT_VECTOR;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a scalar
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsScalar(LogicalPlan plan) {
        return getType(plan) == PromqlDataType.SCALAR;
    }

    @Nullable
    static PromqlDataType getType(@Nullable LogicalPlan plan) {
        if (plan instanceof PromqlPlan promqlPlan) {
            return promqlPlan.returnType();
        }
        return null;
    }
}
