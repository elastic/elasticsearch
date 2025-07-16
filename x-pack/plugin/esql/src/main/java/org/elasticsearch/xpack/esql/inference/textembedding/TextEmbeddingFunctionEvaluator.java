/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingBitResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.inference.TextEmbedding;
import org.elasticsearch.xpack.esql.inference.InferenceFunctionEvaluator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

import java.util.List;

public class TextEmbeddingFunctionEvaluator implements InferenceFunctionEvaluator {

    private final InferenceRunner inferenceRunner;

    private final TextEmbedding f;

    public TextEmbeddingFunctionEvaluator(TextEmbedding f, InferenceRunner inferenceRunner) {
        this.f = f;
        this.inferenceRunner = inferenceRunner;
    }

    @Override
    public void eval(FoldContext foldContext, ActionListener<Expression> listener) {
        assert f.inferenceId() != null && f.inferenceId().foldable() : "inferenceId should not be null and be foldable";
        assert f.inputText() != null && f.inputText().foldable() : "inputText should not be null and be foldable";

        String inferenceId = BytesRefs.toString(f.inferenceId().fold(foldContext));
        String inputText = BytesRefs.toString(f.inputText().fold(foldContext));

        inferenceRunner.execute(inferenceRequest(inferenceId, inputText), listener.map(this::parseInferenceResponse));
    }

    private InferenceAction.Request inferenceRequest(String inferenceId, String inputText) {
        return InferenceAction.Request.builder(inferenceId, TaskType.TEXT_EMBEDDING).setInput(List.of(inputText)).build();
    }

    private Literal parseInferenceResponse(InferenceAction.Response response) {
        float[] embeddingValues = switch (response.getResults()) {
            case TextEmbeddingFloatResults floatEmbeddingResults -> floatEmbeddingResults.embeddings().get(0).values();
            case TextEmbeddingByteResults bytesEmbeddingResults -> bytesEmbeddingResults.embeddings().get(0).toFloatArray();
            case TextEmbeddingBitResults bitsEmbeddingResults -> bitsEmbeddingResults.embeddings().get(0).toFloatArray();
            default -> throw new IllegalArgumentException("Inference response should be of type TextEmbeddingResults");
        };

        return new Literal(f.source(), embeddingValues, DataType.DENSE_VECTOR);
    }
}
