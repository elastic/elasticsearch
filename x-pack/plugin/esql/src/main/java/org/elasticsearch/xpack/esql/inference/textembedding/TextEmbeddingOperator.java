/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.inference.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunnerConfig;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

/**
 * {@link TextEmbeddingOperator} is an {@link InferenceOperator} that performs text embedding inference.
 * It evaluates a text expression for each input row, constructs inference requests, and emits the dense vector embeddings as output.
 */
public class TextEmbeddingOperator extends InferenceOperator {

    private final ExpressionEvaluator inputTextEvaluator;

    public TextEmbeddingOperator(
        DriverContext driverContext,
        BulkInferenceRunner bulkInferenceRunner,
        String inferenceId,
        ExpressionEvaluator inputTextEvaluator
    ) {
        super(driverContext, bulkInferenceRunner, inferenceId, InferenceRunnerConfig.DEFAULT.maxOutstandingRequests());
        this.inputTextEvaluator = inputTextEvaluator;
    }

    @Override
    protected void doClose() {
        Releasables.close(inputTextEvaluator);
    }

    @Override
    public String toString() {
        return "TextEmbeddingOperator[inferenceId=" + inferenceId() + "]";
    }

    @Override
    protected Page addOutputBlock(Page input, Block outputblock) {
        return input.appendBlock(outputblock);
    }

    @Override
    protected BulkInferenceRequestIterator requests(Page input) {
        return new TextEmbeddingOperatorRequestIterator((BytesRefBlock) inputTextEvaluator.eval(input), inferenceId());
    }

    @Override
    protected OutputBuilder outputBuilder(Page input) {
        // TODO: get dimensions from the function
        var outputBlockBuilder = blockFactory().newFloatBlockBuilder(input.getPositionCount());
        return new TextEmbeddingOperatorOutputBuilder(outputBlockBuilder);
    }

    public static class Factory implements Operator.OperatorFactory {
        private final BulkInferenceRunner.Factory inferenceRunnerFactory;
        private final String inferenceId;
        private final ExpressionEvaluator.Factory inputTextEvaluatorFactory;

        public Factory(
            BulkInferenceRunner.Factory inferenceRunnerFactory,
            String inferenceId,
            ExpressionEvaluator.Factory inputTextEvaluatorFactory
        ) {
            this.inferenceRunnerFactory = inferenceRunnerFactory;
            this.inferenceId = inferenceId;
            this.inputTextEvaluatorFactory = inputTextEvaluatorFactory;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TextEmbeddingOperator(
                driverContext,
                inferenceRunnerFactory.create(InferenceRunnerConfig.DEFAULT),
                inferenceId,
                inputTextEvaluatorFactory.get(driverContext)
            );
        }

        @Override
        public String describe() {
            return "TextEmbeddingOperator[inferenceId=" + inferenceId + "]";
        }
    }
}
