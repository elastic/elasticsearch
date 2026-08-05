/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * {@link EmbeddingOperator} is an {@link InferenceOperator} that performs multimodal embedding inference.
 * It evaluates a text expression for each input row, constructs typed embedding inference requests
 * using {@link EmbeddingAction.Request}, and emits the dense vector embeddings as output.
 * <p>
 * Dispatch routes to {@link InferenceService#executeEmbeddingInference} via an overridden
 * {@code dispatchInferenceRequest}.
 * </p>
 */
public class EmbeddingOperator extends InferenceOperator {

    EmbeddingOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        ExpressionEvaluator inputEvaluator,
        DataType dataType,
        TimeValue timeout
    ) {
        super(
            driverContext,
            inferenceService,
            new EmbeddingRequestIterator.Factory(inferenceId, TaskType.EMBEDDING, inputEvaluator, dataType, timeout),
            new EmbeddingOutputBuilder(driverContext.blockFactory())
        );
    }

    @Override
    protected void dispatchInferenceRequest(
        InferenceService inferenceService,
        BaseInferenceActionRequest request,
        ActionListener<InferenceAction.Response> listener
    ) {
        inferenceService.executeEmbeddingInference((EmbeddingAction.Request) request, listener);
    }

    @Override
    public String toString() {
        return "EmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Factory for creating {@link EmbeddingOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        ExpressionEvaluator.Factory textEvaluatorFactory,
        DataType dataType,
        TimeValue timeout
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "EmbeddingOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new EmbeddingOperator(
                driverContext,
                inferenceService,
                inferenceId,
                textEvaluatorFactory.get(driverContext),
                dataType,
                timeout
            );
        }
    }
}
