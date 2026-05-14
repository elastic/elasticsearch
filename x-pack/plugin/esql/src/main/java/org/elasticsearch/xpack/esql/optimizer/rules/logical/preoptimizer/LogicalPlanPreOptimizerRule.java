/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * A rule that can be applied to an analyzed logical plan before it is optimized.
 */
public interface LogicalPlanPreOptimizerRule {

    /**
     * Apply the rule to the logical plan.
     *
     * @param plan     the analyzed logical plan to pre-optimize
     * @param listener the listener returning the pre-optimized plan when pre-optimization rule is applied
     */
    void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener);
}
