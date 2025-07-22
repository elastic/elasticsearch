/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;

/**
 * Interface for evaluating inference functions in ESQL.
 * <p>
 * Implementations of this interface handle the execution of specific inference functions,
 * such as text embedding generation, by delegating to the appropriate inference service.
 */
public interface InferenceFunctionEvaluator {

    /**
     * Evaluates the inference function asynchronously.
     *
     * @param foldContext The folding context containing expression information
     * @param listener The listener to be notified with the evaluation result
     */
    void eval(FoldContext foldContext, ActionListener<Expression> listener);

    /**
     * Factory interface for creating inference function evaluators.
     * <p>
     * Implementations should provide a way to create evaluators for specific
     * inference function types.
     */
    interface Factory {
        /**
         * Creates a new inference function evaluator.
         *
         * @param inferenceRunnerFactory Factory for creating inference runners
         * @return A new evaluator instance
         */
        InferenceFunctionEvaluator get(InferenceRunner.Factory inferenceRunnerFactory);
    }
}
