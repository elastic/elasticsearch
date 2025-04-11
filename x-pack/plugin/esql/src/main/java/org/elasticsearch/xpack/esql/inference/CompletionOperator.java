/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;

public class CompletionOperator extends InferenceOperator<Page> {

    public record Factory(InferenceRunner inferenceRunner, String inferenceId, ExpressionEvaluator.Factory promptEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "RerankOperator[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(driverContext, inferenceRunner, inferenceId, promptEvaluatorFactory.get(driverContext));
        }
    }

    private final ExpressionEvaluator promptEvaluator;

    public CompletionOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        ExpressionEvaluator promptEvaluator
    ) {
        super(driverContext, inferenceRunner.getThreadContext(), inferenceRunner, inferenceId);
        this.promptEvaluator = promptEvaluator;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        Page outputPage = inputPage.appendBlock(promptEvaluator.eval(inputPage));
        listener.onResponse(outputPage);
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

    @Override
    public String toString() {
        return "CompletionOperator[inference_id=[" + inferenceId() + "]]";
    }

}
