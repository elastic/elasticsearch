/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

import java.util.stream.IntStream;

public class CompletionOperatorOutputBuilder implements InferenceOperator.OutputBuilder {
    private final Page inputPage;
    private final BytesRefBlock.Builder outputBlockBuilder;
    private final BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();

    public CompletionOperatorOutputBuilder(BytesRefBlock.Builder outputBlockBuilder, Page inputPage) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
    }

    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        ChatCompletionResults completionResults = inferenceResults(inferenceResponse);

        if (completionResults == null) {
            outputBlockBuilder.appendNull();
        } else {
            outputBlockBuilder.beginPositionEntry();
            for (ChatCompletionResults.Result completionResult : completionResults.getResults()) {
                bytesRefBuilder.copyChars(completionResult.content());
                outputBlockBuilder.appendBytesRef(bytesRefBuilder.get());
                bytesRefBuilder.clear();
            }
            outputBlockBuilder.endPositionEntry();
        }
    }

    @Override
    public Page buildOutput() {
        Block outputBlock = outputBlockBuilder.build();
        assert outputBlock.getPositionCount() == inputPage.getPositionCount();
        return inputPage.projectBlocks(IntStream.range(0, inputPage.getBlockCount() - 1).toArray()).appendBlock(outputBlock);
    }

    private ChatCompletionResults inferenceResults(InferenceAction.Response inferenceResponse) {
        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, ChatCompletionResults.class);
    }
}
