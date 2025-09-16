/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.textembedding.TextEmbeddingOperator;

public class InferenceFunctionEvaluator {

    private final FoldContext foldContext;
    private final InferenceService inferenceService;

    public InferenceFunctionEvaluator(FoldContext foldContext, InferenceService inferenceService) {
        this.foldContext = foldContext;
        this.inferenceService = inferenceService;
    }

    public void fold(InferenceFunction<?> f, ActionListener<Object> listener) {
        assert f.foldable() : "Inference function must be foldable";


    }

    private Operator.OperatorFactory createInferenceOperatorFactory(InferenceFunction<?> f) {
        return switch (f) {
            case TextEmbedding textEmbedding -> new TextEmbeddingOperator.Factory(
                inferenceService,
                inferenceId(f),
                expressionEvaluatorFactory(textEmbedding.inputText())
            );
            default -> throw new IllegalArgumentException("Unknown inference function: " + f.getClass().getName());
        };
    }

    private String inferenceId(InferenceFunction<?> f) {
        return BytesRefs.toString(f.inferenceId().fold(foldContext));
    }

    private ExpressionEvaluator.Factory expressionEvaluatorFactory(Expression e) {
        assert e.foldable() : "Input expression must be foldable";
        return EvalMapper.toEvaluator(foldContext, Literal.of(foldContext, e), null);
    }
}
