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
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

import java.util.Map;

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
     * @param inputOptions     Optional metadata for the input (e.g. type, format).
     */
    EmbeddingOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator inputEvaluator,
        Map<String, Object> inputOptions
    ) {
        super(
            driverContext,
            inferenceService,
            requestIteratorFactory(inferenceId, taskType, inputEvaluator, inputOptions),
            new TextEmbeddingOutputBuilder(driverContext.blockFactory())
        );

        this.inferenceId = inferenceId;
    }

    public String toString() {
        return "EmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    private static BulkInferenceRequestItemIterator.Factory requestIteratorFactory(
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator textEvaluator,
        Map<String, Object> inputOptions
    ) {
        Object typeValue = inputOptions.get("type");
        if (typeValue instanceof String typeStr) {
            DataType dataType = DataType.fromString(typeStr);
            DataFormat dataFormat = null;
            Object formatValue = inputOptions.get("format");
            if (formatValue instanceof String formatStr) {
                dataFormat = DataFormat.fromString(formatStr);
            }
            return new EmbeddingRequestIterator.Factory(inferenceId, taskType, textEvaluator, dataType, dataFormat);
        }
        return new TextEmbeddingRequestIterator.Factory(inferenceId, taskType, textEvaluator);
    }

    /**
     * Factory for creating {@link EmbeddingOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        TaskType taskType,
        ExpressionEvaluator.Factory textEvaluatorFactory,
        Map<String, Object> inputOptions
    ) implements OperatorFactory {

        /**
         * Convenience constructor for factories without input options.
         */
        public Factory(
            InferenceService inferenceService,
            String inferenceId,
            TaskType taskType,
            ExpressionEvaluator.Factory textEvaluatorFactory
        ) {
            this(inferenceService, inferenceId, taskType, textEvaluatorFactory, Map.of());
        }

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
                taskType,
                textEvaluatorFactory.get(driverContext),
                inputOptions
            );
        }
    }

}
