/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.promql.MetadataManipulationFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.ResultOrderingFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ResultOrderingFunction.ResultOrdering;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

/**
 * Injects an {@link OrderBy} above a PromQL result so that identity-preserving sort functions
 * order the final rows rather than the per-sample grain below {@link TimeSeriesCollapse}.
 * <p>
 * Ordering is only observable for instant queries. Range queries keep the input order and emit
 * a warning: Prometheus itself discards sort order after a range evaluation, and injecting an
 * {@code OrderBy} here would still be overwritten by the matrix layout.
 * <p>
 * Runs once in the analyzer Initialize batch after {@link TranslateTimeSeriesCollapse} and before
 * {@link TranslatePromqlToEsqlPlan}, so the {@code OrderBy} is built over {@link PromqlCommand#output()}
 * attributes that translation later projects back to the same {@code NameId}s.
 */
public final class AddPromqlResultOrder extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        Holder<TimeSeriesCollapse> collapseMatch = new Holder<>();
        plan.forEachUp(TimeSeriesCollapse.class, collapse -> {
            if (collapseMatch.get() == null && collapse.child() instanceof PromqlCommand) {
                collapseMatch.set(collapse);
            }
        });
        if (collapseMatch.get() != null) {
            TimeSeriesCollapse collapse = collapseMatch.get();
            PromqlCommand cmd = (PromqlCommand) collapse.child();
            LogicalPlan wrapped = wrapIfNeeded(collapse, cmd, context);
            if (wrapped == collapse) {
                return plan;
            }
            return plan.transformUp(TimeSeriesCollapse.class, c -> c == collapse ? wrapped : c);
        }

        Holder<PromqlCommand> commandMatch = new Holder<>();
        plan.forEachUp(PromqlCommand.class, cmd -> {
            if (commandMatch.get() == null) {
                commandMatch.set(cmd);
            }
        });
        if (commandMatch.get() != null) {
            PromqlCommand cmd = commandMatch.get();
            LogicalPlan wrapped = wrapIfNeeded(cmd, cmd, context);
            if (wrapped == cmd) {
                return plan;
            }
            return plan.transformUp(PromqlCommand.class, c -> c == cmd ? wrapped : c);
        }
        return plan;
    }

    private static LogicalPlan wrapIfNeeded(LogicalPlan node, PromqlCommand cmd, AnalyzerContext context) {
        ResultOrderingFunction ordering = resultOrdering(cmd.promqlPlan());
        if (ordering != null) {
            if (cmd.isInstantQuery() == false) {
                String name = ordering instanceof PromqlFunctionCall call ? call.functionName() : "sort";
                HeaderWarning.addWarning("{}: ordering is discarded for range queries", name);
                return node;
            }
            ResultOrdering result = ordering.resultOrdering(cmd.output(), context.configuration());
            if (result.orders().isEmpty()) {
                return node;
            }
            LogicalPlan child = node;
            if (result.syntheticKeys().isEmpty() == false) {
                child = new Eval(cmd.source(), node, result.syntheticKeys());
            }
            return new OrderBy(cmd.source(), child, result.orders());
        }
        return node;
    }

    /**
     * The ordering function whose order the result keeps: the top-level one, or one below steps that pass its values
     * through unchanged. Prometheus keeps an instant query's order through each step that maps series one to one; below
     * one that changes the values, such as {@code abs}, ordering by the result's value would not give that order.
     */
    private static ResultOrderingFunction resultOrdering(LogicalPlan plan) {
        if (plan instanceof ResultOrderingFunction ordering) {
            return ordering;
        }
        if (plan instanceof MetadataManipulationFunction relabel) {
            return resultOrdering(relabel.child());
        }
        // A filter keeps the vector side's samples in their order; between vectors that is the left side, unless
        // group_right makes the right side the one Prometheus iterates. and and unless keep the left side's.
        if (plan instanceof VectorBinaryComparison comparison && comparison.filterMode()) {
            if (PromqlPlan.returnsScalar(comparison.left())) {
                return resultOrdering(comparison.right());
            }
            if (comparison.match().grouping() != VectorMatch.Joining.RIGHT) {
                return resultOrdering(comparison.left());
            }
        }
        if (plan instanceof VectorBinarySet set && set.op() != VectorBinarySet.SetOp.UNION) {
            return resultOrdering(set.left());
        }
        return null;
    }
}
