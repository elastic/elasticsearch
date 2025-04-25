/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;

import java.util.List;
import java.util.NoSuchElementException;

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
    protected RequestIterator requests(Page inputPage) {
        return new InferenceOperator.RequestIterator() {
            private final BytesRefBlock promptBlock = (BytesRefBlock) promptEvaluator.eval(inputPage);
            private BytesRef readBuffer = new BytesRef();
            private int currentPos = 0;

            @Override
            public boolean hasNext() {
                return currentPos < promptBlock.getPositionCount();
            }

            @Override
            public InferenceAction.Request next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                int pos = currentPos++;

                if (promptBlock.isNull(pos)) {
                    return null;
                }

                StringBuilder promptBuilder = new StringBuilder();
                for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
                    readBuffer = promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
                    promptBuilder.append(readBuffer.utf8ToString()).append("\n");
                }

                return inferenceRequest(promptBuilder.toString());
            }

            @Override
            public void close() {
                promptBlock.allowPassingToDifferentDriver();
                Releasables.closeExpectNoException(promptBlock);
            }
        };
    }

    @Override
    protected OutputBuilder<ChatCompletionResults> outputBuilder(Page inputPage) {
        return new InferenceOperator.OutputBuilder<>() {
            private final BytesRefBlock.Builder outputBlockBuilder = blockFactory.newBytesRefBlockBuilder(inputPage.getPositionCount());
            private final BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();

            @Override
            public void close() {
                Releasables.closeExpectNoException(outputBlockBuilder);
            }

            @Override
            public void onInferenceResults(ChatCompletionResults completionResults) {
                if (completionResults == null || completionResults.getResults().isEmpty()) {
                    outputBlockBuilder.appendNull();
                } else {
                    outputBlockBuilder.beginPositionEntry();
                    for (ChatCompletionResults.Result rankedDocsResult : completionResults.getResults()) {
                        bytesRefBuilder.copyChars(rankedDocsResult.content());
                        outputBlockBuilder.appendBytesRef(bytesRefBuilder.get());
                        bytesRefBuilder.clear();
                    }
                    outputBlockBuilder.endPositionEntry();
                }
            }

            @Override
            protected Class<ChatCompletionResults> inferenceResultsClass() {
                return ChatCompletionResults.class;
            }

            @Override
            public Page buildOutput() {
                Block outputBlock = outputBlockBuilder.build();
                assert outputBlock.getPositionCount() == inputPage.getPositionCount();
                return inputPage.appendBlock(outputBlock);
            }
        };
    }

    private InferenceAction.Request inferenceRequest(String prompt) {
        return InferenceAction.Request.builder(inferenceId(), TaskType.COMPLETION).setInput(List.of(prompt)).build();
    }
}
