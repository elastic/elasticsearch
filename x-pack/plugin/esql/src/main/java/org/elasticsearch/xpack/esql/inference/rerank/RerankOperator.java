/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;

/**
 * {@link RerankOperator} is an {@link InferenceOperator} that computes relevance scores for rows using a reranking model.
 * It evaluates a row encoder expression for each input row, batches them together, and sends them to the reranking service
 * with a query text to obtain relevance scores.
 */
public class RerankOperator extends InferenceOperator {

    public static final int DEFAULT_BATCH_SIZE = 10;

    /**
     * Constructs a new {@code RerankOperator}.
     *
     * @param driverContext                   The driver context.
     * @param inferenceService                The inference service to use for executing inference requests.
     * @param requestItemIteratorFactory      Factory for creating request iterators from input pages.
     * @param outputBuilder                   Builder for converting inference responses into output pages.
     * @param maxOutstandingPages             The maximum number of pages processed in parallel.
     * @param maxOutstandingInferenceRequests The maximum number of inference requests to be run in parallel.
     */
    RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        RerankRequestIterator.Factory requestItemIteratorFactory,
        RerankOutputBuilder outputBuilder,
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
     * Factory for creating {@link RerankOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        ExpressionEvaluator.Factory rowEncoderFactory,
        int scoreChannel,
        int batchSize
    ) implements OperatorFactory {
        @Override
        public String describe() {
            return "RerankOperator[]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceService,
                new RerankRequestIterator.Factory(inferenceId, queryText, rowEncoderFactory.get(driverContext), batchSize),
                new RerankOutputBuilder(driverContext.blockFactory(), scoreChannel),
                DEFAULT_MAX_OUTSTANDING_PAGES,
                DEFAULT_MAX_OUTSTANDING_REQUESTS
            );
        }
    }

}
