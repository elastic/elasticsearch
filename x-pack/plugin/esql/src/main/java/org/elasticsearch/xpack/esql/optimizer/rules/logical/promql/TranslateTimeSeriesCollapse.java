/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

/**
 * Populates a {@link TimeSeriesCollapse} that wraps a {@link PromqlCommand} with the dimensions and
 * bounds extracted from the inner PromqlCommand. The PromqlCommand itself stays in place as the
 * child and is translated to ESQL nodes by {@link TranslatePromqlToEsqlPlan} on a subsequent pass
 * in the same batch.
 * <p>
 * {@link TimeSeriesCollapse} is only valid with a {@link PromqlCommand} child: parse rules enforce that
 * for ES|QL text, but callers that build plans by hand must stack collapse the same way or optimization
 * fails fast here instead of later with unresolved bounds.
 */
public final class TranslateTimeSeriesCollapse extends OptimizerRules.ParameterizedOptimizerRule<
    TimeSeriesCollapse,
    LogicalOptimizerContext> {

    public TranslateTimeSeriesCollapse() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesCollapse collapse, LogicalOptimizerContext context) {
        if (collapse.child() instanceof PromqlCommand pc) {
            // pc.promqlPlan().output() is the dimension list by construction: PromqlCommand.output() is
            // [value, step] ++ promqlPlan.output(). ResolvePromqlFunctions has reshaped promqlPlan during
            // analysis, so by the time this optimizer rule runs the output is representative.
            // Bounds expressions flow straight through; the Mapper folds them when building TimeSeriesCollapseExec.
            return new TimeSeriesCollapse(
                collapse.source(),
                pc,
                collapse.value(),
                collapse.step(),
                pc.promqlPlan().output(),
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
