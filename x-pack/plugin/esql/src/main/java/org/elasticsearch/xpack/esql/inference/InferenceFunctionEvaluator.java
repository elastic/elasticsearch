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
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.textembedding.TextEmbeddingFunctionEvaluator;

public interface InferenceFunctionEvaluator {

    void eval(FoldContext foldContext, ActionListener<Expression> listener);

    static InferenceFunctionEvaluator get(InferenceFunction<?> inferenceFunction, InferenceRunner inferenceRunner) {
        return switch (inferenceFunction) {
            case TextEmbedding textEmbedding -> new TextEmbeddingFunctionEvaluator(textEmbedding, inferenceRunner);
            default -> throw new IllegalArgumentException("Unsupported inference function: " + inferenceFunction.getClass());
        };
    }
}
