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

/**
 * If query approximation is enabled, this rule substitutes the original plan
 * with an approximation plan.
 */
public final class SubstituteApproximationPlan extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan, LogicalOptimizerContext context) {
        if (context.configuration().approximationSettings() == null) {
            // Approximation is not enabled
            return logicalPlan;
        } else if (Approximation.verifyPlan(logicalPlan) == null) {
            // Plan is not suitable for approximation
            return logicalPlan;
        } else {
            // Returns an approximation plan with a placeholders for the sample probability.
            // This placeholder will be replaced after executing the corresponding subplans.
            return ApproximationPlan.get(logicalPlan, context.configuration().approximationSettings());
        }
    }
}
