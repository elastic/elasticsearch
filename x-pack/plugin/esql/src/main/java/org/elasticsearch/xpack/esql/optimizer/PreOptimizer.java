/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;

/**
 * The class is responsible for invoking any steps that need to be applied to the logical plan,
 * before this is being optimized.
 * <p>
 * This is useful, especially if you need to execute some async tasks before the plan is optimized.
 * </p>
 */
public class PreOptimizer {

    private final InferencePreOptimizer inferencePreOptimizer;

    public PreOptimizer(TransportActionServices services, FoldContext foldContext) {
        this(services.inferenceRunner(), foldContext);
    }

    PreOptimizer(InferenceRunner inferenceRunner, FoldContext foldContext) {
        this.inferencePreOptimizer = new InferencePreOptimizer(inferenceRunner, foldContext);
    }

    public void preOptimize(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        inferencePreOptimizer.foldInferenceFunctions(plan, listener);
    }

    private static class InferencePreOptimizer {
        private final InferenceRunner inferenceRunner;
        private final FoldContext foldContext;

        private InferencePreOptimizer(InferenceRunner inferenceRunner, FoldContext foldContext) {
            this.inferenceRunner = inferenceRunner;
            this.foldContext = foldContext;
        }

        private void foldInferenceFunctions(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
            plan.transformExpressionsUp(InferenceFunction.class, this::foldInferenceFunction, listener);
        }

        private void foldInferenceFunction(InferenceFunction<?> inferenceFunction, ActionListener<Expression> listener) {
            InferenceFunctionEvaluator.get(inferenceFunction, inferenceRunner).eval(foldContext, listener);
        }
    }
}
