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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.inference.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-optimizer rule that evaluates inference functions (like TEXT_EMBEDDING) into constant values.
 * <p>
 * This rule identifies foldable inference functions in the logical plan, executes them using the
 * inference runner, and replaces them with their computed results. This enables downstream
 * optimizations to work with the actual embedding values rather than the function calls.
 * <p>
 * The rule processes inference functions recursively, handling newly revealed functions that might
 * appear after the first round of folding.
 *
 * Example:
 * <pre>
 * // Before optimization:
 * FROM documents | WHERE KNN(field, TEXT_EMBEDDING('constant text', 'model-id'), 10)
 *
 * // After optimization:
 * // (The TEXT_EMBEDDING function is replaced with its computed dense vector)
 * FROM documents | WHERE KNN(field, [0.1, 0.2, ...], 10)
 * </pre>
 */
public class InferenceFunctionConstantFolding implements PreOptimizerRule {
    private final BulkInferenceRunner.Factory inferenceRunnerFactory;
    private final FoldContext foldContext;

    /**
     * Creates a new instance of the InferenceFunctionConstantFolding rule.
     *
     * @param inferenceRunnerFactory the factory for creating an inference runner
     * @param preOptimizerContext the pre-optimizer context containing folding configuration
     */
    public InferenceFunctionConstantFolding(
        BulkInferenceRunner.Factory inferenceRunnerFactory,
        LogicalPreOptimizerContext preOptimizerContext
    ) {
        this.inferenceRunnerFactory = inferenceRunnerFactory;
        this.foldContext = preOptimizerContext.foldCtx();
    }

    /**
     * Applies the InferenceFunctionConstantFolding rule to the given logical plan.
     *
     * @param plan     the logical plan to apply the rule to
     * @param listener the listener to notify when the rule has been applied
     */
    @Override
    public void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        foldInferenceFunctions(plan, listener);
    }

    /**
     * Recursively folds inference functions in the logical plan.
     * <p>
     * This method collects all foldable inference functions, evaluates them in parallel,
     * and then replaces them with their results. If new inference functions are revealed
     * after the first round of folding, it recursively processes them as well.
     *
     * @param plan     the logical plan to fold inference functions in
     * @param listener the listener to notify when the folding is complete
     */
    private void foldInferenceFunctions(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        // First let's collect all the inference foldable inference functions
        List<InferenceFunction<?>> inferenceFunctions = collectFoldableInferenceFunctions(plan);

        if (inferenceFunctions.isEmpty()) {
            // No inference functions that can be evaluated at this time found. Return the original plan.
            listener.onResponse(plan);
            return;
        }

        // Map each inference function to its computed result
        // We will use this map to replace the inference functions in the plan.
        Map<InferenceFunction<?>, Expression> inferenceFunctionsToResults = new HashMap<>();

        // Create a listener that will be notified when all inference functions have been evaluated
        // This listener will:
        // 1. Replace each inference function with its computed result in the plan
        // 2. Recursively process the transformed plan to handle any new constant expressions
        // that may have been revealed by the first round of folding
        CountDownActionListener completionListener = new CountDownActionListener(
            inferenceFunctions.size(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> {
                // Replace the inference functions in the plan with their results
                LogicalPlan next = plan.transformExpressionsUp(
                    InferenceFunction.class,
                    f -> inferenceFunctionsToResults.getOrDefault(f, f)
                );

                // Recursively fold the remaining inference functions
                foldInferenceFunctions(next, l);
            })
        );

        // Try to compute the result for each inference function.
        for (InferenceFunction<?> inferenceFunction : inferenceFunctions) {
            foldInferenceFunction(inferenceFunction, completionListener.map(e -> {
                inferenceFunctionsToResults.put(inferenceFunction, e);
                return null;
            }));
        }
    }

    /**
     * Collects all foldable inference functions from the logical plan.
     * <p>
     * A function is considered foldable if:
     * 1. It's an instance of InferenceFunction
     * 2. It's marked as foldable (all parameters are constants)
     * 3. It doesn't contain nested inference functions
     *
     * @param plan the logical plan to collect inference functions from
     * @return a list of foldable inference functions
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

    /**
     * Evaluates a single inference function asynchronously.
     * <p>
     * Uses the inference function's evaluator factory to create an evaluator
     * that can process the function with the given inference runner.
     *
     * @param inferenceFunction the inference function to evaluate
     * @param listener          the listener to notify when the evaluation is complete
     */
    private void foldInferenceFunction(InferenceFunction<?> inferenceFunction, ActionListener<Expression> listener) {
        inferenceFunction.inferenceEvaluatorFactory().get(inferenceRunnerFactory).eval(foldContext, listener);
    }
}
