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

import java.util.List;

/**
 * {@link RerankOperator} is an {@link InferenceOperator} that computes relevance scores for rows using a reranking model.
 * It evaluates a row encoder expression for each input row, batches them together, and sends them to the reranking service
 * with a query text to obtain relevance scores.
 */
public class RerankOperator extends InferenceOperator {

    public static final int DEFAULT_BATCH_SIZE = 10;

    private final String queryText;

    private final int scoreChannel;

    /**
     * Constructs a new {@code RerankOperator}.
     *
     * @param driverContext    The driver context.
     * @param inferenceService The inference service to use for executing inference requests.
     * @param inferenceId      The ID of the reranking model to invoke.
     * @param queryText        The query text to use for reranking.
     * @param inputEvaluators  Evaluator for computing reranked texts from input rows.
     * @param scoreChannel     The output channel where the relevance scores will be written.
     * @param batchSize        \The number of rows to include in each inference request batch.
     */
    RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        ExpressionEvaluator[] inputEvaluators,
        int scoreChannel,
        int batchSize
    ) {
        super(
            driverContext,
            inferenceService,
            new RerankRequestIterator.Factory(inferenceId, queryText, inputEvaluators, batchSize),
            new RerankOutputBuilder(driverContext.blockFactory(), scoreChannel)
        );
        this.queryText = queryText;
        this.scoreChannel = scoreChannel;
    }

    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    /**
     * Factory for creating {@link RerankOperator} instances.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        List<ExpressionEvaluator.Factory> inputEvaluatorFactories,
        int scoreChannel,
        int batchSize
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceService,
                inferenceId,
                queryText,
                inputEvaluators(driverContext),
                scoreChannel,
                batchSize
            );
        }

        protected ExpressionEvaluator[] inputEvaluators(DriverContext driverContext) {
            return inputEvaluatorFactories.stream()
                .map(evaluatorFactory -> evaluatorFactory.get(driverContext))
                .toArray(ExpressionEvaluator[]::new);
        }
    }
}
