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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceResponse;

import java.util.List;

/**
 * {@link CompletionOperatorOutputBuilder} builds the output page for {@link CompletionOperator} by converting {@link ChatCompletionResults}
 * into a {@link BytesRefBlock}.
 */
class CompletionOperatorOutputBuilder implements InferenceOperator.OutputBuilder {
    private final Page inputPage;
    private final BytesRefBlock.Builder outputBlockBuilder;
    private final BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();

    CompletionOperatorOutputBuilder(BytesRefBlock.Builder outputBlockBuilder, Page inputPage) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
    }


    @Override
    public void addInferenceResponse(BulkInferenceResponse bulkInferenceResponse) {
        List<ChatCompletionResults.Result> results = inferenceResults(bulkInferenceResponse.response());
        int currentIndex = 0;

        for (int valueCount : bulkInferenceResponse.shape()) {
            if (valueCount == 0) {
                outputBlockBuilder.appendNull();
                continue;
            }

            outputBlockBuilder.beginPositionEntry();
            for (int i = 0; i < valueCount; i++) {
                ChatCompletionResults.Result result = results.get(currentIndex++);
                bytesRefBuilder.copyChars(result.content());
                outputBlockBuilder.appendBytesRef(bytesRefBuilder.get());
                bytesRefBuilder.clear();
            }
            outputBlockBuilder.endPositionEntry();
        }
    }

    /**
     * Builds the final output page by appending the completion output block to the input page.
     */
    @Override
    public Page buildOutput() {
        Block outputBlock = outputBlockBuilder.build();
        assert outputBlock.getPositionCount() == inputPage.getPositionCount();
        return inputPage.appendBlock(outputBlock);
    }

    private List<ChatCompletionResults.Result> inferenceResults(InferenceAction.Response inferenceResponse) {
        if (inferenceResponse == null) {
            return List.of();
        }

        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, ChatCompletionResults.class).results();
    }
}
