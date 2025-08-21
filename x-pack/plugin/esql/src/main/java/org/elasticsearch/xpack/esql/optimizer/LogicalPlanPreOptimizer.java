/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * The class is responsible for invoking any steps that need to be applied to the logical plan,
 * before this is being optimized.
 * <p>
 * This is useful, especially if you need to execute some async tasks before the plan is optimized.
 * </p>
 */
public class LogicalPlanPreOptimizer {

    private final LogicalPreOptimizerContext preOptimizerContext;

    public LogicalPlanPreOptimizer(LogicalPreOptimizerContext preOptimizerContext) {
        this.preOptimizerContext = preOptimizerContext;
    }

    /**
     * Pre-optimize a logical plan.
     *
     * @param plan     the analyzed logical plan to pre-optimize
     * @param listener the listener returning the pre-optimized plan when pre-optimization is complete
     */
    public void preOptimize(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        if (plan.analyzed() == false) {
            listener.onFailure(new IllegalStateException("Expected analyzed plan"));
            return;
        }

        doPreOptimize(plan, listener.delegateFailureAndWrap((l, preOptimized) -> {
            preOptimized.setPreOptimized();
            listener.onResponse(preOptimized);
        }));
    }

    private void doPreOptimize(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        // this is where we will be executing async tasks
        listener.onResponse(plan);
    }
}
