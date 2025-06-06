/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionConfig;

import java.util.stream.IntStream;

/**
 * {@link RerankOperator} is an inference operator that compute scores for rows using a reranking model.
 */
public class RerankOperator extends InferenceOperator {

    // Default number of rows to include per inference request
    private static final int DEFAULT_BATCH_SIZE = 20;
    private final String queryText;

    // Encodes each input row into a string representation for the model
    private final ExpressionEvaluator rowEncoder;
    private final int scoreChannel;

    // Batch size used to group rows into a single inference request (currently fixed)
    // TODO: make it configurable either in the command or as query pragmas
    private final int batchSize = DEFAULT_BATCH_SIZE;

    public RerankOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        ThreadPool threadPool,
        String inferenceId,
        String queryText,
        ExpressionEvaluator rowEncoder,
        int scoreChannel
    ) {
        super(driverContext, inferenceRunner, BulkInferenceExecutionConfig.DEFAULT, threadPool, inferenceId);
        this.queryText = queryText;
        this.rowEncoder = rowEncoder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    public void addInput(Page input) {
        try {
            Block inputBlock = rowEncoder.eval(input);
            super.addInput(input.appendBlock(inputBlock));
        } catch (Exception e) {
            releasePageOnAnyThread(input);
            throw e;
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(rowEncoder);
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId() + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    /**
     * Returns the request iterator responsible for batching and converting input rows into inference requests.
     */
    @Override
    protected RerankOperatorRequestIterator requests(Page inputPage) {
        int inputBlockChannel = inputPage.getBlockCount() - 1;
        return new RerankOperatorRequestIterator(inputPage.getBlock(inputBlockChannel), inferenceId(), queryText, batchSize);
    }

    /**
     * Returns the output builder responsible for collecting inference responses and building the output page.
     */
    @Override
    protected RerankOperatorOutputBuilder outputBuilder(Page input) {
        DoubleBlock.Builder outputBlockBuilder = blockFactory().newDoubleBlockBuilder(input.getPositionCount());
        return new RerankOperatorOutputBuilder(
            outputBlockBuilder,
            input.projectBlocks(IntStream.range(0, input.getBlockCount() - 1).toArray()),
            scoreChannel
        );
    }

    /**
     * Factory for creating {@link RerankOperator} instances
     */
    public record Factory(
        InferenceRunner inferenceRunner,
        String inferenceId,
        String queryText,
        ExpressionEvaluator.Factory rowEncoderFactory,
        int scoreChannel
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new RerankOperator(
                driverContext,
                inferenceRunner,
                inferenceRunner.threadPool(),
                inferenceId,
                queryText,
                rowEncoderFactory().get(driverContext),
                scoreChannel
            );
        }
    }
}
