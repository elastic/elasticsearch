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
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * {@link EmbeddingOperator} is an {@link InferenceOperator} that performs text embedding inference.
 * It evaluates a text expression for each input row, constructs text embedding inference requests,
 * and emits the dense vector embeddings as output.
 */
public class EmbeddingOperator extends InferenceOperator {

    private final String inferenceId;

    /**
     * Constructs a new {@code EmbeddingOperator}.
     *
     * @param driverContext    The driver context.
     * @param inferenceService The inference service to use for executing inference requests.
     * @param inferenceId      The ID of the embedding model to invoke.
     * @param taskType         The task type to use for inference requests.
     * @param inputEvaluator   Evaluator for computing input text from rows.
     */
    EmbeddingOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator inputEvaluator
    ) {
        super(
            driverContext,
            inferenceService,
            new EmbeddingRequestIterator.Factory(inferenceId, taskType, inputEvaluator),
            new TextEmbeddingOutputBuilder(driverContext.blockFactory())
        );

        this.inferenceId = inferenceId;
    }

    public String toString() {
        return "EmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Factory for creating {@link EmbeddingOperator} instances.
     */
    public record Factory(InferenceService inferenceService, String inferenceId, TaskType taskType, ExpressionEvaluator.Factory textEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "EmbeddingOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new EmbeddingOperator(driverContext, inferenceService, inferenceId, taskType, textEvaluatorFactory.get(driverContext));
        }
    }

}
