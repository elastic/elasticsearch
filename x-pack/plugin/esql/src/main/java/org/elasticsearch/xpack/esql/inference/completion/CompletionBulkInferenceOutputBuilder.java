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
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;

public class CompletionBulkInferenceOutputBuilder extends BulkInferenceOutputBuilder<ChatCompletionResults, Page> {
    private final Page inputPage;
    private final BytesRefBlock.Builder outputBlockBuilder;
    private final BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();

    public CompletionBulkInferenceOutputBuilder(Page inputPage, BytesRefBlock.Builder outputBlockBuilder) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

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
}
