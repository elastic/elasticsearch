/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

public class CompletionOperator extends InferenceOperator<ChatCompletionResults> {

    public record Factory(InferenceRunner inferenceRunner, String inferenceId, ExpressionEvaluator.Factory promptEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "CompletionOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(
                driverContext,
                inferenceRunner,
                inferenceRunner.threadPool(),
                inferenceId,
                promptEvaluatorFactory.get(driverContext)
            );
        }
    }

    private final ExpressionEvaluator promptEvaluator;

    public CompletionOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        ThreadPool threadPool,
        String inferenceId,
        ExpressionEvaluator promptEvaluator
    ) {
        super(driverContext, inferenceRunner, threadPool, inferenceId);
        this.promptEvaluator = promptEvaluator;
    }

    @Override
    protected void doClose() {
        Releasables.close(promptEvaluator);
    }

    @Override
    public String toString() {
        return "CompletionOperator[inference_id=[" + inferenceId() + "]]";
    }

    @Override
    protected BulkInferenceRequestIterator requests(Page inputPage) {
        return new CompletionOperatorRequestIterator((BytesRefBlock) promptEvaluator.eval(inputPage), inferenceId());
    }

    @Override
    protected CompletionOperatorOutputBuilder outputBuilder(Page input) {
        return new CompletionOperatorOutputBuilder(blockFactory().newBytesRefBlockBuilder(input.getPositionCount()), input);
    }
}
