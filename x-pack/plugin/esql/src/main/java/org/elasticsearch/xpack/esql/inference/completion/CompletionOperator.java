/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

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
 * {@link CompletionOperator} is an {@link InferenceOperator} that performs inference using prompt-based model (e.g., text completion).
 * It evaluates a prompt expression for each input row, constructs inference requests, and emits the model responses as output.
 */
public class CompletionOperator extends InferenceOperator {

    private final ExpressionEvaluator promptEvaluator;

    public CompletionOperator(
        DriverContext driverContext,
        BulkInferenceRunner bulkInferenceRunner,
        String inferenceId,
        ExpressionEvaluator promptEvaluator,
        int maxOutstandingPages
    ) {
        super(driverContext, bulkInferenceRunner, inferenceId, maxOutstandingPages);
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
    protected synchronized Page addOutputBlock(Page input, Block outputblock) {
        return input.appendBlock(outputblock);
    }

    /**
     * Constructs the completion inference requests iterator for the given input page by evaluating the prompt expression.
     *
     * @param inputPage The input data page.
     */
    @Override
    protected BulkInferenceRequestIterator requests(Page inputPage) {
        return new CompletionOperatorRequestIterator((BytesRefBlock) promptEvaluator.eval(inputPage), inferenceId());
    }

    /**
     * Creates a new {@link CompletionOperatorOutputBuilder} to collect and emit the completion results.
     *
     * @param input The input page for which results will be constructed.
     */
    @Override
    protected CompletionOperatorOutputBuilder outputBuilder(Page input) {
        return new CompletionOperatorOutputBuilder(blockFactory().newBytesRefBlockBuilder(input.getPositionCount()));
    }

    /**
     * Factory for creating {@link CompletionOperator} instances.
     */
    public record Factory(
        BulkInferenceRunner.Factory inferenceRunnerFactory,
        String inferenceId,
        ExpressionEvaluator.Factory promptEvaluatorFactory
    ) implements OperatorFactory {
        @Override
        public String describe() {
            return "CompletionOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(
                driverContext,
                inferenceRunnerFactory.create(InferenceRunnerConfig.DEFAULT),
                inferenceId,
                promptEvaluatorFactory.get(driverContext),
                InferenceRunnerConfig.DEFAULT.maxOutstandingBulkRequests()
            );
        }
    }
}
