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
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlFunctionCall;
import org.elasticsearch.xpack.esql.plan.logical.promql.ResultOrderingFunction;
import org.elasticsearch.xpack.esql.plan.logical.promql.ResultOrderingFunction.ResultOrdering;
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
        if (cmd.promqlPlan() instanceof ResultOrderingFunction ordering) {
            if (cmd.isInstantQuery() == false) {
                String name = cmd.promqlPlan() instanceof PromqlFunctionCall call ? call.functionName() : "sort";
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
}
