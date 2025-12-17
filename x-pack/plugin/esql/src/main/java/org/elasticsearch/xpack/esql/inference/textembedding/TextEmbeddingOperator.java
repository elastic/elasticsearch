/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * {@link TextEmbeddingOperator} is an {@link InferenceOperator} that performs text embedding inference.
 * It evaluates a text expression for each input row, constructs text embedding inference requests,
 * and emits the dense vector embeddings as output.
 */
public class TextEmbeddingOperator extends InferenceOperator {
    /**
     * Constructs a new {@code TextEmbeddingOperator}.
     *
     * @param driverContext                   The driver context.
     * @param inferenceService                The inference service to use for executing inference requests.
     * @param requestItemIteratorFactory      Factory for creating request iterators from input pages.
     * @param outputBuilder                   Builder for converting inference responses into output pages.
     * @param maxOutstandingPages             The maximum number of pages processed in parallel.
     * @param maxOutstandingInferenceRequests The maximum number of inference requests to be run in parallel.
     */
    TextEmbeddingOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        TextEmbeddingRequestIterator.Factory requestItemIteratorFactory,
        TextEmbeddingOutputBuilder outputBuilder,
        int maxOutstandingPages,
        int maxOutstandingInferenceRequests
    ) {
        super(
            driverContext,
            inferenceService,
            requestItemIteratorFactory,
            outputBuilder,
            maxOutstandingPages,
            maxOutstandingInferenceRequests
        );
    }

    /**
     * Factory for creating {@link TextEmbeddingOperator} instances.
     */
    public record Factory(InferenceService inferenceService, String inferenceId, ExpressionEvaluator.Factory textEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "TextEmbeddingOperator[]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TextEmbeddingOperator(
                driverContext,
                inferenceService,
                new TextEmbeddingRequestIterator.Factory(inferenceId, textEvaluatorFactory.get(driverContext)),
                new TextEmbeddingOutputBuilder(driverContext.blockFactory()),
                DEFAULT_MAX_OUTSTANDING_PAGES,
                DEFAULT_MAX_OUTSTANDING_REQUESTS
            );
        }
    }

}
