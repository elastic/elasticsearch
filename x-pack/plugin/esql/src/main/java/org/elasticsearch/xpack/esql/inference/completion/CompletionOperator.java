/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

public class CompletionOperator extends InferenceOperator<ChatCompletionResults> {

    public record Factory(InferenceRunner inferenceRunner, String inferenceId, ExpressionEvaluator.Factory promptEvaluatorFactory)
        implements
            OperatorFactory {
        @Override
        public String describe() {
            return "Completion[inference_id=[" + inferenceId + "]]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionOperator(driverContext, inferenceRunner, inferenceId, promptEvaluatorFactory.get(driverContext));
        }
    }

    private final ExpressionEvaluator promptEvaluator;
    private final BlockFactory blockFactory;

    public CompletionOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        ExpressionEvaluator promptEvaluator
    ) {
        super(driverContext, inferenceRunner, inferenceId);
        this.promptEvaluator = promptEvaluator;
        this.blockFactory = driverContext.blockFactory();
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(promptEvaluator);
    }

    @Override
    public String toString() {
        return "CompletionOperator[inference_id=[" + inferenceId() + "]]";
    }

    @Override
    protected TaskType taskType() {
        return TaskType.COMPLETION;
    }

    @Override
    protected BulkInferenceRequestIterator bulkInferenceRequestIterator(Page inputPage) {
        return new CompletionBulkInferenceRequestIterator((BytesRefBlock) promptEvaluator.eval(inputPage), this::inferenceRequestBuilder);
    }

    @Override
    protected BulkInferenceOutputBuilder<ChatCompletionResults, Page> bulkOutputBuilder(Page inputPage) {
        return new CompletionBulkInferenceOutputBuilder(inputPage, blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount()));
    }
}
