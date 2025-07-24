/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer.InferenceFunctionConstantFolding;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer.PreOptimizerRule;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

import java.util.List;

/**
 * The class is responsible for invoking any steps that need to be applied to the logical plan,
 * before this is being optimized.
 * <p>
 * This is useful, especially if you need to execute some async tasks before the plan is optimized.
 * </p>
 */
public class LogicalPlanPreOptimizer {

    private final List<PreOptimizerRule> rules;

    public LogicalPlanPreOptimizer(TransportActionServices services, LogicalPreOptimizerContext preOptimizerContext) {
        rules = List.of(new InferenceFunctionConstantFolding(services.bulkInferenceRunner(), preOptimizerContext.foldCtx()));
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

    /**
     * Loop over the rules and apply them to the logical plan.
     *
     * @param plan     the analyzed logical plan to pre-optimize
     * @param listener the listener returning the pre-optimized plan when pre-optimization is complete
     */
    private void doPreOptimize(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        SubscribableListener<LogicalPlan> rulesListener = SubscribableListener.newSucceeded(plan);

        for (PreOptimizerRule rule : rules) {
            rulesListener = rulesListener.andThen((l, p) -> rule.apply(p, l));
        }

        rulesListener.addListener(listener);
    }
}
