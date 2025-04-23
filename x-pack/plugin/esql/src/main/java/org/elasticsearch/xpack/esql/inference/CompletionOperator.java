/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;

import java.util.List;

public class CompletionOperator extends InferenceOperator<Page, ChatCompletionResults> {

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
    private final BlockFactory blockFactory;

    public CompletionOperator(
        DriverContext driverContext,
        InferenceRunner inferenceRunner,
        String inferenceId,
        ExpressionEvaluator promptEvaluator
    ) {
        super(driverContext, inferenceRunner.getThreadContext(), inferenceRunner, inferenceId, ChatCompletionResults.class);
        this.promptEvaluator = promptEvaluator;
        this.blockFactory = driverContext.blockFactory();
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        int pageSize = inputPage.getPositionCount();
        String[] responses = new String[pageSize];

        CountDownActionListener countDownListener = new CountDownActionListener(
            inputPage.getPositionCount(),
            listener.delegateFailureIgnoreResponseAndWrap(l -> {
                try (BytesRefBlock.Builder outputBlockBuilder = blockFactory.newBytesRefBlockBuilder(pageSize)) {
                    BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
                    for (int pos = 0; pos < pageSize; pos++) {
                        if (responses[pos] == null) {
                            outputBlockBuilder.appendNull();
                        } else {
                            bytesRefBuilder.copyChars(responses[pos]);
                            outputBlockBuilder.appendBytesRef(bytesRefBuilder.get());
                        }
                    }

                    l.onResponse(inputPage.appendBlock(outputBlockBuilder.build()));
                }
            })
        );

        try (BytesRefBlock promptBlock = (BytesRefBlock) promptEvaluator.eval(inputPage)) {
            BytesRef readBuffer = new BytesRef();
            for (int pos = 0; pos < pageSize; pos++) {
                final int currentPos = pos;
                if (promptBlock.isNull(pos)) {
                    countDownListener.onResponse(null);
                } else {
                    StringBuilder promptBuilder = new StringBuilder();
                    for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
                        readBuffer = promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
                        promptBuilder.append(readBuffer.utf8ToString()).append("\n");
                    }

                    InferenceAction.Request request = InferenceAction.Request.builder(inferenceId(), TaskType.COMPLETION)
                        .setInput(List.of(promptBuilder.toString()))
                        .build();

                    doInference(request, countDownListener.delegateFailureAndWrap((l, r) -> {
                        responses[currentPos] = r.results().getFirst().content();
                        l.onResponse(null);
                    }));
                }
            }
        }
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
