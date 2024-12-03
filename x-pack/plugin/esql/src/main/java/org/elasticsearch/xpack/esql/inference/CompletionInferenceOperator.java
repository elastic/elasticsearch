/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class CompletionInferenceOperator extends AsyncOperator {

    // Move to a setting.
    private static final int MAX_INFERENCE_WORKER = 1;

    public record Factory(
        InferenceExecutionService inferenceExecutionService,
        EvalOperator.ExpressionEvaluator.Factory promptEvaluatorFactory,
        String inferenceId
    ) implements OperatorFactory {
        public String describe() {
            return "CompletionInferenceOperator[]";
        }


        @Override
        public Operator get(DriverContext driverContext) {
            return new CompletionInferenceOperator(driverContext, inferenceExecutionService, promptEvaluatorFactory.get(driverContext), inferenceId);
        }
    }

    private final BlockFactory blockFactory;
    private final InferenceExecutionService inferenceExecutionService;
    private final EvalOperator.ExpressionEvaluator promptEvaluator;
    private final String inferenceId;

    public CompletionInferenceOperator(DriverContext driverContext, InferenceExecutionService inferenceExecutionService, EvalOperator.ExpressionEvaluator promptEvaluator, String inferenceId) {
        super(driverContext, MAX_INFERENCE_WORKER);
        this.blockFactory = driverContext.blockFactory();
        this.inferenceExecutionService = inferenceExecutionService;
        this.promptEvaluator = promptEvaluator;
        this.inferenceId = inferenceId;
    }


    @Override
    protected void performAsync(Page inputPage, ActionListener<Page> listener) {
        BytesRefBlock promptBlock = (BytesRefBlock) promptEvaluator.eval(inputPage);

        BytesRef promptValue = new BytesRef();

        CountDownLatch countDownLatch = new CountDownLatch(inputPage.getPositionCount());
        BytesRef[] inferredValues = new BytesRef[inputPage.getPositionCount()];

        ActionListener<Tuple<Integer, BytesRef>> inferenceResponseListener = ActionListener.wrap(
            response -> {
                countDownLatch.countDown();
                inferredValues[response.v1()] = response.v2();
                if (countDownLatch.getCount() == 0) {
                    BytesRefBlock.Builder inferenceBlock = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());

                    for (BytesRef inferredValue : inferredValues) {
                        if (inferredValue == null) {
                            inferenceBlock.appendNull();
                        } else {
                            inferenceBlock.appendBytesRef(inferredValue);
                        }
                    }
                    listener.onResponse(inputPage.appendBlock(inferenceBlock.build()));
                }
            },
            listener::onFailure
        );

        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            final int currentPosition = position;
            if (promptBlock.isNull(position) || promptBlock.getValueCount(position) != 1) {
                inferenceResponseListener.onResponse(Tuple.tuple(position, null));
                continue;
            }

            doCompletion(promptBlock.getBytesRef(promptBlock.getFirstValueIndex(position), promptValue).utf8ToString(), ActionListener.wrap(
                completionResponse -> inferenceResponseListener.onResponse(Tuple.tuple(currentPosition, completionResponse)),
                listener::onFailure
            ));
        }
    }


    @Override
    protected void doClose() {

    }

    @Override
    public String toString() {
        return "CompletionInferenceOperator[]";
    }

    private void doCompletion(String prompt, ActionListener<BytesRef> listener) {
        inferenceExecutionService.doInference(createInferenceRequest(prompt), ActionListener.wrap(
            inferenceResponse -> {
                if (inferenceResponse.getResults() instanceof ChatCompletionResults completionResults) {
                    listener.onResponse(new BytesRef(completionResults.getResults().getFirst().predictedValue().toString()));
                    return;
                }
                listener.onFailure(new Exception("WTF???"));
            },
            listener::onFailure
        ));
    }

    InferenceAction.Request createInferenceRequest(String prompt) {
        return InferenceAction.Request.builder(inferenceId, TaskType.COMPLETION)
            .setInput(List.of(prompt))
            .build();
    }
}
