/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-optimizer rule that performs constant folding for inference functions in logical plans.
 * <p>
 * This rule identifies inference functions with constant parameters and evaluates them at optimization time,
 * replacing them with their computed results.
 * <p>
 * The folding process is recursive and handles nested inference functions by processing them in multiple
 * passes until no more foldable functions remain.
 * <p>
 * Example transformation:
 * {@code TEXT_EMBEDDING("hello world", "model1")} → {@code [0.1, 0.2, 0.3, ...]}
 */
public class FoldInferenceFunctions implements LogicalPlanPreOptimizerRule {

    private final InferenceFunctionEvaluator inferenceFunctionEvaluator;

    public FoldInferenceFunctions(LogicalPreOptimizerContext preOptimizerContext) {
        this(InferenceFunctionEvaluator.factory().create(preOptimizerContext.foldCtx(), preOptimizerContext.inferenceService()));
    }

    protected FoldInferenceFunctions(InferenceFunctionEvaluator inferenceFunctionEvaluator) {
        this.inferenceFunctionEvaluator = inferenceFunctionEvaluator;
    }

    @Override
    public void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        foldInferenceFunctions(plan, listener);
    }

    /**
     * Recursively folds inference functions in the logical plan.
     * <p>
     * This method collects all foldable inference functions, evaluates them in parallel,
     * and then replaces them with their computed results. If new foldable inference functions are remaining
     * after the first round of folding (due to nested function resolution), it recursively processes
     * them until no more foldable functions remain.
     * </p>
     *
     * @param plan     the logical plan to fold inference functions in
     * @param listener the listener to notify when the folding is complete
     */
    private void foldInferenceFunctions(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        // Collect all foldable inference functions from the current plan
        List<InferenceFunction<?>> inferenceFunctions = collectFoldableInferenceFunctions(plan);

        if (inferenceFunctions.isEmpty()) {
            // No foldable inference functions were found - return the original plan unchanged
            listener.onResponse(plan);
            return;
        }

        // Map to store the computed results for each inference function
        Map<InferenceFunction<?>, Expression> inferenceFunctionsToResults = new HashMap<>();

        // Create a countdown listener that will be triggered when all inference functions complete
        // Once all are done, replace the functions in the plan with their results and recursively
        // process any remaining foldable inference functions
        CountDownActionListener completionListener = new CountDownActionListener(
            inferenceFunctions.size(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> {
                // Transform the plan by replacing inference functions with their computed results
                LogicalPlan transformedPlan = plan.transformExpressionsUp(
                    InferenceFunction.class,
                    f -> inferenceFunctionsToResults.getOrDefault(f, f)
                );

                // Recursively process the transformed plan to handle any remaining inference functions
                foldInferenceFunctions(transformedPlan, l);
            })
        );

        // Evaluate each inference function asynchronously
        for (InferenceFunction<?> inferenceFunction : inferenceFunctions) {
            inferenceFunctionEvaluator.fold(inferenceFunction, completionListener.delegateFailureAndWrap((l, result) -> {
                inferenceFunctionsToResults.put(inferenceFunction, result);
                l.onResponse(null);
            }));
        }
    }

    /**
     * Collects all foldable inference functions from the logical plan.
     * <p>
     * A function is considered foldable if it meets all of the following criteria:
     * <ol>
     * <li>It's an instance of {@link InferenceFunction}</li>
     * <li>It's marked as foldable (all parameters are constants)</li>
     * <li>It doesn't contain nested inference functions (to avoid dependency issues)</li>
     * </ol>
     * <p>
     * Functions with nested inference functions are excluded to ensure proper evaluation order.
     * They will be considered for folding in subsequent recursive passes after their nested
     * functions have been resolved.
     *
     * @param plan the logical plan to collect inference functions from
     * @return a list of foldable inference functions, may be empty if none are found
     */
    private List<InferenceFunction<?>> collectFoldableInferenceFunctions(LogicalPlan plan) {
        List<InferenceFunction<?>> inferenceFunctions = new ArrayList<>();

        plan.forEachExpressionUp(InferenceFunction.class, f -> {
            if (f.foldable() && f.hasNestedInferenceFunction() == false) {
                inferenceFunctions.add(f);
            }
        });

        return inferenceFunctions;
    }
}
