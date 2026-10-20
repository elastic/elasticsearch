/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.TranslationContext;

/**
 * Translates PromQL logical plan into ESQL plan. Runs before {@link TranslateTimeSeriesAggregate} to convert
 * PromQL-specific nodes into standard ESQL nodes (TimeSeriesAggregate, Aggregate, Eval, etc.). Examples:
 * <pre>
 * PromQL: sum by (cluster) (rate(http_requests[5m]))
 * Result: TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 *
 * PromQL: time() - avg(sum by (cluster) (rate(http_requests[5m])))
 * Result: Eval[time() - avg_result]
 *           \_ Aggregate[avg(sum_result), groupBy=[step]]
 *                 \_ TimeSeriesAggregate[sum(rate(value)), groupBy=[step, cluster]]
 * </pre>
 * Mechanism: a {@link TranslationContext} per command; recursive descent via {@link PromqlPlan#translate} where every
 * PromQL node produces an {@code IntermediateResult} its parent composes, and {@link TranslationContext#translateFinal}
 * stitches the top-level forms (single branch, {@code or} union) into finished tables.
 */
public final class TranslatePromqlToEsqlPlan extends AnalyzerRules.ParameterizedAnalyzerRule<PromqlCommand, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(PromqlCommand cmd, AnalyzerContext context) {
        return new TranslationContext(cmd, context).translateFinal();
    }
}
