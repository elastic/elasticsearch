/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

public final class SubstituteApproximationPlan extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan, LogicalOptimizerContext context) {
        if (context.configuration().approximationSettings() == null) {
            return logicalPlan;
        } else {
            Approximation.verifyPlan(logicalPlan);
            // Returns an approximation plan with some placeholders (e.g. for the sample probability).
            // These placeholders will be replaced after executing the corresponding subplans.
            return ApproximationPlan.get(logicalPlan, context.configuration().approximationSettings());
        }
    }
}
