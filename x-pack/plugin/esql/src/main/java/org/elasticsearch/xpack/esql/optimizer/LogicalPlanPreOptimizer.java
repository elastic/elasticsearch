/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class is responsible for invoking any steps that need to be applied to the logical plan,
 * before this is being optimized.
 * <p>
 * This is useful, especially if you need to execute some async tasks before the plan is optimized.
 * </p>
 */
public class LogicalPlanPreOptimizer {

    private final InferenceFunctionFolding inferenceFunctionFolding;

    public LogicalPlanPreOptimizer(TransportActionServices services, LogicalPreOptimizerContext preOptimizerContext) {
        this.inferenceFunctionFolding = new InferenceFunctionFolding(services.inferenceRunner(), preOptimizerContext.foldCtx());
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
        inferenceFunctionFolding.foldInferenceFunctions(plan, listener);
    }

    private static class InferenceFunctionFolding {
        private final InferenceRunner inferenceRunner;
        private final FoldContext foldContext;

        private InferenceFunctionFolding(InferenceRunner inferenceRunner, FoldContext foldContext) {
            this.inferenceRunner = inferenceRunner;
            this.foldContext = foldContext;
        }

        private void foldInferenceFunctions(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
            // First let's collect all the inference functions
            List<InferenceFunction<?>> inferenceFunctions = new ArrayList<>();
            plan.forEachExpressionUp(InferenceFunction.class, inferenceFunctions::add);

            if (inferenceFunctions.isEmpty()) {
                // No inference functions found. Return the original plan.
                listener.onResponse(plan);
                return;
            }

            // This is a map of inference functions to their results.
            // We will use this map to replace the inference functions in the plan.
            Map<InferenceFunction<?>, Expression> inferenceFunctionsToResults = new HashMap<>();

            // Prepare a listener that will be called when all inference functions are done.
            // This listener will replace the inference functions in the plan with their results.
            CountDownActionListener completionListener = new CountDownActionListener(
                inferenceFunctions.size(),
                listener.map(
                    ignored -> plan.transformExpressionsUp(InferenceFunction.class, f -> inferenceFunctionsToResults.getOrDefault(f, f))
                )
            );

            // Try to compute the result for each inference function.
            for (InferenceFunction<?> inferenceFunction : inferenceFunctions) {
                foldInferenceFunction(inferenceFunction, completionListener.map(e -> {
                    inferenceFunctionsToResults.put(inferenceFunction, e);
                    return null;
                }));
            }
        }

        private void foldInferenceFunction(InferenceFunction<?> inferenceFunction, ActionListener<Expression> listener) {
            InferenceFunctionEvaluator.get(inferenceFunction, inferenceRunner).eval(foldContext, listener);
        }
    }
}
