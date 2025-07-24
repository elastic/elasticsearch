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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
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
 */
public class InferenceFunctionConstantFolding implements PreOptimizerRule {
    private final BulkInferenceRunner bulkInferenceRunner;
    private final FoldContext foldContext;

    /**
     * Creates a new instance of the InferenceFunctionConstantFolding rule.
     *
     * @param bulkInferenceRunner the inference runner to use for evaluating inference functions
     * @param foldContext         the fold context to use for evaluating inference functions
     */
    public InferenceFunctionConstantFolding(BulkInferenceRunner bulkInferenceRunner, FoldContext foldContext) {
        this.bulkInferenceRunner = bulkInferenceRunner;
        this.foldContext = foldContext;
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

        // This is a map of inference functions to their results.
        // We will use this map to replace the inference functions in the plan.
        Map<InferenceFunction<?>, Expression> inferenceFunctionsToResults = new HashMap<>();

        // Prepare a listener that will be called when all inference functions are done.
        // This listener will replace the inference functions in the plan with their results and then recursively fold the remaining
        // inference functions.
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
        inferenceFunction.inferenceEvaluatorFactory().get(bulkInferenceRunner).eval(foldContext, listener);
    }
}
