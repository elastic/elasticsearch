/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

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

    private static final List<Rule> RULES = List.of();

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
        SubscribableListener<LogicalPlan> ruleChainListener = SubscribableListener.newSucceeded(plan);
        for (Rule rule : RULES) {
            ruleChainListener = ruleChainListener.andThen((l, p) -> rule.apply(p, l));
        }
        ruleChainListener.addListener(listener);
    }

    public interface Rule {
        void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener);
    }

    private static class FoldInferenceFunction implements Rule {
        private final InferenceFunctionEvaluator inferenceEvaluator;

        private FoldInferenceFunction(LogicalPreOptimizerContext preOptimizerContext) {
            this.inferenceEvaluator = new InferenceFunctionEvaluator(preOptimizerContext.foldCtx(), preOptimizerContext.inferenceService());
        }

        @Override
        public void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener) {

        }
    }
}
