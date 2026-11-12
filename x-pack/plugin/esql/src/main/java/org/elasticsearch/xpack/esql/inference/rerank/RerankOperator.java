/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.RerankAction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;

import java.util.List;
import java.util.Set;

/**
 * {@link RerankOperator} is an {@link InferenceOperator} that computes relevance scores for rows using a reranking model.
 * It evaluates a row encoder expression for each input row, batches them together, and sends them to the reranking service
 * with a query text to obtain relevance scores using {@link RerankAction.Request}.
 * <p>
 * Dispatch routes to {@link InferenceService#executeRerankInference} via an overridden
 * {@code dispatchInferenceRequest}.
 * </p>
 * <p>
 * Batches are formed within each input page and sent concurrently, so scores from different batches are only comparable
 * for pointwise rerankers. {@link Factory} therefore uses this operator only for endpoints known to be pointwise, and
 * {@link SingleRequestRerankOperator} for every other endpoint.
 * </p>
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
     * @param timeout          Timeout for each inference request.
     */
    RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        ExpressionEvaluator[] inputEvaluators,
        int scoreChannel,
        int batchSize,
        TimeValue timeout
    ) {
        super(
            driverContext,
            inferenceService,
            new RerankRequestIterator.Factory(inferenceId, queryText, inputEvaluators, batchSize, timeout),
            new RerankOutputBuilder(driverContext.blockFactory(), scoreChannel),
            Source.EMPTY,
            false
        );
        this.queryText = queryText;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void dispatchInferenceRequest(
        InferenceService inferenceService,
        BaseInferenceActionRequest request,
        ActionListener<InferenceAction.Response> listener
    ) {
        inferenceService.executeRerankInference((RerankAction.Request) request, listener);
    }

    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    /**
     * Inference endpoints known to use a pointwise reranker, which scores each document on its own. Scores from separate
     * requests are comparable for these, so they keep the batched, concurrent {@link RerankOperator}. Every other endpoint
     * may be listwise and uses {@link SingleRequestRerankOperator}.
     * <p>
     * Endpoints don't say whether their reranker is pointwise or listwise, so this lists them by ID until they do.
     * {@link Rerank#DEFAULT_INFERENCE_ID}, the default {@code RERANK} endpoint, runs Elastic's reranker, a cross-encoder.
     * </p>
     */
    private static final Set<String> KNOWN_POINTWISE_INFERENCE_IDS = Set.of(Rerank.DEFAULT_INFERENCE_ID);

    static boolean isKnownPointwise(String inferenceId) {
        return KNOWN_POINTWISE_INFERENCE_IDS.contains(inferenceId);
    }

    /**
     * Factory for the {@code RERANK} operator. Creates a batched {@link RerankOperator} for endpoints known to be pointwise
     * (see {@link #isKnownPointwise}), and a {@link SingleRequestRerankOperator} for every other endpoint.
     *
     * @param batchSize The number of documents per request for pointwise endpoints. Ignored for other endpoints.
     */
    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        List<ExpressionEvaluator.Factory> inputEvaluatorFactories,
        int scoreChannel,
        int batchSize,
        TimeValue timeout
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            if (isKnownPointwise(inferenceId) == false) {
                return new SingleRequestRerankOperator(
                    driverContext,
                    inferenceService,
                    inferenceId,
                    queryText,
                    inputEvaluators(driverContext),
                    scoreChannel,
                    timeout
                );
            }
            return new RerankOperator(
                driverContext,
                inferenceService,
                inferenceId,
                queryText,
                inputEvaluators(driverContext),
                scoreChannel,
                batchSize,
                timeout
            );
        }

        protected ExpressionEvaluator[] inputEvaluators(DriverContext driverContext) {
            return inputEvaluatorFactories.stream()
                .map(evaluatorFactory -> evaluatorFactory.get(driverContext))
                .toArray(ExpressionEvaluator[]::new);
        }
    }
}
