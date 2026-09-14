/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import static org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse.isZeroLimit;

/**
 * Populates a {@link TimeSeriesCollapse} that wraps a {@link PromqlCommand} with the bounds
 * extracted from the inner PromqlCommand. The PromqlCommand itself stays in place as the child and
 * is translated to ESQL nodes by {@link TranslatePromqlToEsqlPlan} on a subsequent pass in the
 * same batch.
 * <p>
 * The bounds ({@code start}, {@code end}, {@code stepBucketSize}) are {@code null} at parse time
 * and are copied from the {@link PromqlCommand} here rather than resolved independently to avoid
 * divergence from the PROMQL evaluation. This is why the rule must run before
 * {@link TranslatePromqlToEsqlPlan}: it requires the collapse's child to still be a
 * {@link PromqlCommand} and throws otherwise.
 * <p>
 * The grouping columns ({@code dimensions}) are derived from the child's output at use time by
 * {@link TimeSeriesCollapse#dimensions()} and are no longer copied here.
 * <p>
 * {@link TimeSeriesCollapse} is only valid with a {@link PromqlCommand} child: parse rules enforce
 * that for ES|QL text, but callers that build plans by hand must stack the collapse the same way or
 * optimization fails fast here instead of later with unresolved bounds.
 */
public final class TranslateTimeSeriesCollapse extends AnalyzerRules.ParameterizedAnalyzerRule<TimeSeriesCollapse, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;
    }

    @Override
    protected LogicalPlan rule(TimeSeriesCollapse collapse, AnalyzerContext context) {
        // Missing Prometheus indices have already been lowered to an empty child by analysis.
        if (isZeroLimit(collapse.child())) {
            return collapse.child();
        }
        if (collapse.child() instanceof PromqlCommand pc) {
            // Bounds expressions flow straight through; the Mapper folds them when building TimeSeriesCollapseExec.
            // Dimensions are derived from the child output at use time (TimeSeriesCollapse#dimensions()).
            return new TimeSeriesCollapse(
                collapse.source(),
                pc,
                collapse.value(),
                collapse.step(),
                pc.start(),
                pc.end(),
                pc.resolveTimeBucketSize()
            );
        }
        throw new QlIllegalArgumentException(
            "TimeSeriesCollapse requires a PromqlCommand child; got [{}]",
            collapse.child().getClass().getName()
        );
    }
}
