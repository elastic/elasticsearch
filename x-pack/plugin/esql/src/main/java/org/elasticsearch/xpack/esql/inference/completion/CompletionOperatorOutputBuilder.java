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

/**
 * {@link CompletionOperatorOutputBuilder} builds the output page for {@link CompletionOperator} by converting {@link ChatCompletionResults}
 * into a {@link BytesRefBlock}.
 */
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
        releasePageOnAnyThread(inputPage);
    }

    /**
     * Adds an inference response to the output builder.
     *
     * <p>
     * If the response is null or not of type {@link ChatCompletionResults} an {@link IllegalStateException} is thrown.
     * Else, the result text is added to the output block.
     * </p>
     *
     * <p>
     * The responses must be added in the same order as the corresponding inference requests were generated.
     * Failing to preserve order may lead to incorrect or misaligned output rows.
     * </p>
     */
    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        if (inferenceResponse == null) {
            outputBlockBuilder.appendNull();
            return;
        }

        ChatCompletionResults completionResults = inferenceResults(inferenceResponse);

        if (completionResults == null) {
            throw new IllegalStateException("Received null inference result; expected a non-null result of type ChatCompletionResults");
        }

        outputBlockBuilder.beginPositionEntry();
        for (ChatCompletionResults.Result completionResult : completionResults.getResults()) {
            bytesRefBuilder.copyChars(completionResult.content());
            outputBlockBuilder.appendBytesRef(bytesRefBuilder.get());
            bytesRefBuilder.clear();
        }
        outputBlockBuilder.endPositionEntry();
    }

    /**
     * Builds the final output page by appending the completion output block to a shallow copy of the input page.
     */
    @Override
    public Page buildOutput() {
        Block outputBlock = outputBlockBuilder.build();
        assert outputBlock.getPositionCount() == inputPage.getPositionCount();
        return inputPage.shallowCopy().appendBlock(outputBlock);
    }

    private ChatCompletionResults inferenceResults(InferenceAction.Response inferenceResponse) {
        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, ChatCompletionResults.class);
    }
}
