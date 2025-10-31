/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRunnerConfig;

/**
 * {@link TextEmbeddingOperator} is an {@link InferenceOperator} that performs text embedding inference.
 * It evaluates a text expression for each input row, constructs text embedding inference requests,
 * and emits the dense vector embeddings as output.
 */
public class TextEmbeddingOperator extends InferenceOperator {

    private final ExpressionEvaluator textEvaluator;

    public TextEmbeddingOperator(
        DriverContext driverContext,
        BulkInferenceRunner bulkInferenceRunner,
        String inferenceId,
        ExpressionEvaluator textEvaluator,
        int maxOutstandingPages
    ) {
        super(driverContext, bulkInferenceRunner, inferenceId, maxOutstandingPages);
        this.textEvaluator = textEvaluator;
    }

    @Override
    protected void doClose() {
        Releasables.close(textEvaluator);
    }

    @Override
    public String toString() {
        return "TextEmbeddingOperator[inference_id=[" + inferenceId() + "]]";
    }

    /**
     * Constructs the text embedding inference requests iterator for the given input page by evaluating the text expression.
     *
     * @param inputPage The input data page.
     */
    @Override
    protected BulkInferenceRequestIterator requests(Page inputPage) {
        return new TextEmbeddingOperatorRequestIterator((BytesRefBlock) textEvaluator.eval(inputPage), inferenceId());
    }

    /**
     * Creates a new {@link TextEmbeddingOperatorOutputBuilder} to collect and emit the text embedding results.
     *
     * @param input The input page for which results will be constructed.
     */
    @Override
    protected TextEmbeddingOperatorOutputBuilder outputBuilder(Page input) {
        FloatBlock.Builder outputBlockBuilder = blockFactory().newFloatBlockBuilder(input.getPositionCount());
        return new TextEmbeddingOperatorOutputBuilder(outputBlockBuilder, input);
    }

    /**
     * Factory for creating {@link TextEmbeddingOperator} instances.
     */
    public record Factory(InferenceService inferenceService, String inferenceId, ExpressionEvaluator.Factory textEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "TextEmbeddingOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new TextEmbeddingOperator(
                driverContext,
                inferenceService.bulkInferenceRunner(),
                inferenceId,
                textEvaluatorFactory.get(driverContext),
                BulkInferenceRunnerConfig.DEFAULT.maxOutstandingBulkRequests()
            );
        }
    }
}
