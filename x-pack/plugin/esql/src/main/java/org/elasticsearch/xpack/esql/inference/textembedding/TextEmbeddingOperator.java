/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.inference.embedding.EmbeddingOutputBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * {@link TextEmbeddingOperator} is an {@link InferenceOperator} that performs plain-text embedding inference.
 * It evaluates a text expression for each input row, constructs text embedding inference requests,
 * and emits the dense vector embeddings as output.
 * <p>
 * Dispatch is handled by the default {@code dispatchInferenceRequest} which calls
 * {@link InferenceService#executeInference}.
 * </p>
 */
public class TextEmbeddingOperator extends InferenceOperator {

    TextEmbeddingOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator inputEvaluator
    ) {
        super(
            driverContext,
            inferenceService,
            new TextEmbeddingRequestIterator.Factory(inferenceId, taskType, inputEvaluator),
            new EmbeddingOutputBuilder(driverContext.blockFactory())
        );
    }

    @Override
    public String toString() {
        return "TextEmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Factory for creating {@link TextEmbeddingOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator.Factory textEvaluatorFactory
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "TextEmbeddingOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TextEmbeddingOperator(
                driverContext,
                inferenceService,
                inferenceId,
                taskType,
                textEvaluatorFactory.get(driverContext)
            );
        }
    }
}
