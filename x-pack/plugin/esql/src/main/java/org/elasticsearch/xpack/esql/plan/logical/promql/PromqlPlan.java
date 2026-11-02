/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Marker interface for PromQL-specific logical plan nodes.
 */
public interface PromqlPlan {
    /**
     * Returns the result's labels: the ones it promotes, for example those added via {@code by(...)}, and its
     * {@code _timeseries} metadata ({@link UnresolvedPromqlAttribute#metadata}) when it keeps labels it does not promote.
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
     * This node's ES|QL plan, built bottom-up from its children's results. A node reads the labels of its input through
     * references ({@link UnresolvedPromqlAttribute}) and never looks at which columns its input carries:
     * {@code BindPromqlLabelReferences} binds them right after translation. {@code translation} carries the shared services:
     * recursion into the child, and the aggregation and {@link TranslationContext#eval eval} helpers every node composes
     * its result from.
     */
    TranslationContext.IntermediateResult translate(TranslationContext translation);

    /**
     * Whether this node is transparent to relabel ({@code label_replace}/{@code label_join}) placement: it does NOT form a
     * series-identity grouping/partition/matching boundary, so a relabel appearing below it is consumed by this node's own
     * enclosing consumer rather than by this node.
     * <p>
     * Return {@code false} only for nodes that bind the enclosing series identity - across-series aggregation
     * ({@code by}/{@code without}), across-series reduction ({@code topk}/{@code bottomk}), and vector binary operators.
     * Per-series and value/type-shaping nodes (selectors, {@code rate}-style within-series aggregates, scalar/vector
     * conversions, {@code histogram_quantile}) are transparent for this purpose even when they reshape labels, because a
     * relabel below them still feeds the same enclosing aggregation. This is used by the original label-function translation,
     * which is outside the aggregation prototype.
     */
    default boolean isIdentityTransparent() {
        return true;
    }

    /**
     * Utility methods to check the return type of a PromqlPlan
     *
     * @param plan the logical plan to check
     * @return true if the plan returns a scalar
     * @throws IllegalArgumentException if the plan is not a PromqlPlan
     */
    static boolean returnsScalar(LogicalPlan plan) {
        return plan.resolved() && getType(plan) == PromqlDataType.SCALAR;
    }

    /**
     * Returns the PromQL data type for the given plan, or {@code null} if it's not a PromQL plan.
     * Handles {@link UnresolvedPromqlFunction} by looking up the function's output type from the registry.
     */
    @Nullable
    static PromqlDataType getType(@Nullable LogicalPlan plan) {
        if (plan instanceof UnresolvedPromqlFunction unresolved) {
            PromqlFunctionDefinition def = PromqlFunctionRegistry.INSTANCE.functionMetadata(unresolved.functionName());
            return def != null ? def.functionType().outputType : null;
        }
        if (plan instanceof PromqlPlan promqlPlan) {
            return promqlPlan.returnType();
        }
        return null;
    }
}
