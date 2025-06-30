/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.embedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
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
 * {@link DenseEmbeddingOperator} is an inference operator that compute vector embeddings from textual data .
 */
public class DenseEmbeddingOperator extends InferenceOperator {

    // Default number of rows to include per inference request
    private static final int DEFAULT_BATCH_SIZE = 20;

    // Encodes each input row into a string representation for the model
    private final ExpressionEvaluator inputEvaluator;

    // Numbers of dimensions for the vector
    private final int dimensions;

    // Batch size used to group rows into a single inference request (currently fixed)
    // TODO: make it configurable either in the command or as query pragmas
    private final int batchSize = DEFAULT_BATCH_SIZE;

    public DenseEmbeddingOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        ThreadPool threadPool,
        int dimensions,
        String inferenceId,
        ExpressionEvaluator inputEvaluator
    ) {
        super(driverContext, inferenceRunner, BulkInferenceExecutionConfig.DEFAULT, threadPool, inferenceId);
        this.dimensions = dimensions;
        this.inputEvaluator = inputEvaluator;
    }

    @Override
    public void addInput(Page input) {
        try {
            Block inputBlock = inputEvaluator.eval(input);
            super.addInput(input.appendBlock(inputBlock));
        } catch (Exception e) {
            releasePageOnAnyThread(input);
            throw e;
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(inputEvaluator);
    }

    @Override
    public String toString() {
        return "DenseEmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Returns the request iterator responsible for batching and converting input rows into inference requests.
     */
    @Override
    protected DenseEmbeddingRequestIterator requests(Page inputPage) {
        int inputBlockChannel = inputPage.getBlockCount() - 1;
        return new DenseEmbeddingRequestIterator(inputPage.getBlock(inputBlockChannel), inferenceId(), batchSize);
    }

    /**
     * Returns the output builder responsible for collecting inference responses and building the output page.
     */
    @Override
    protected DenseEmbeddingOperatorOutputBuilder outputBuilder(Page input) {
        FloatBlock.Builder outputBlockBuilder = blockFactory().newFloatBlockBuilder(input.getPositionCount() * dimensions);
        return new DenseEmbeddingOperatorOutputBuilder(
            outputBlockBuilder,
            input.projectBlocks(IntStream.range(0, input.getBlockCount() - 1).toArray()),
            dimensions
        );
    }

    /**
     * Factory for creating {@link DenseEmbeddingOperator} instances
     */
    public record Factory(InferenceRunner inferenceRunner, int dimensions, String inferenceId,
                          ExpressionEvaluator.Factory inputEvaluatorFactory) implements OperatorFactory {

        @Override
        public String describe() {
            return "DenseEmbeddingOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new DenseEmbeddingOperator(
                driverContext,
                inferenceRunner,
                inferenceRunner.threadPool(),
                dimensions,
                inferenceId,
                inputEvaluatorFactory().get(driverContext)
            );
        }
    }
}
