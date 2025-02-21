/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.inference.InferenceService;

import java.util.HashMap;
import java.util.Map;

public class RerankOperator extends AsyncOperator<Page> {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 10;

    private static final Logger logger = LogManager.getLogger(RerankOperator.class);

    public record Factory(
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluatorSuppliers,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id="
                + inferenceId
                + " query="
                + queryText
                + " rerank_fields="
                + rerankFieldsEvaluatorSuppliers.keySet()
                + " scoreChannel="
                + scoreChannel
                + "]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceService,
                inferenceId,
                queryText,
                buildRerankFieldEvaluator(rerankFieldsEvaluatorSuppliers, driverContext),
                scoreChannel
            );
        }

        private Map<String, EvalOperator.ExpressionEvaluator> buildRerankFieldEvaluator(
            Map<String, EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluatorSuppliers,
            DriverContext driverContext
        ) {
            Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluators = new HashMap<>();

            for (var entry : rerankFieldsEvaluatorSuppliers.entrySet()) {
                rerankFieldsEvaluators.put(entry.getKey(), entry.getValue().get(driverContext));
            }

            return rerankFieldsEvaluators;
        }
    }

    private final InferenceService inferenceService;
    private final BlockFactory blockFactory;
    private final String inferenceId;
    private final String queryText;
    private final Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluator;
    private final int scoreChannel;

    public RerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        Map<String, EvalOperator.ExpressionEvaluator> rerankFieldsEvaluator,
        int scoreChannel
    ) {
        super(driverContext, inferenceService.getThreadContext(), MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.inferenceService = inferenceService;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.rerankFieldsEvaluator = rerankFieldsEvaluator;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        listener.onResponse(inputPage);
    }

    @Override
    protected void doClose() {

    }

    @Override
    protected void releaseFetchedOnAnyThread(Page page) {
        releasePageOnAnyThread(page);
    }

    @Override
    public Page getOutput() {
        return fetchFromBuffer();
    }
}
