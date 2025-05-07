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
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

import java.util.concurrent.atomic.AtomicBoolean;

public class CompletionOperatorOutputBuilder extends InferenceOperator.OutputBuilder<ChatCompletionResults> {
    private final Page inputPage;
    private final BytesRefBlock.Builder outputBlockBuilder;
    private final BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
    private final AtomicBoolean isOutputBuilt = new AtomicBoolean(false);

    public CompletionOperatorOutputBuilder(BytesRefBlock.Builder outputBlockBuilder, Page inputPage) {
        this.inputPage = inputPage;
        this.outputBlockBuilder = outputBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(outputBlockBuilder);
        releasePageOnAnyThread(inputPage);
    }

    @Override
    public void addInferenceResult(ChatCompletionResults completionResults) {
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
    public Page buildOutput() {
        if (isOutputBuilt.compareAndSet(false, true)) {
            Block outputBlock = outputBlockBuilder.build();
            assert outputBlock.getPositionCount() == inputPage.getPositionCount();
            return inputPage.shallowCopy().appendBlock(outputBlock);
        }

        throw new IllegalStateException("buildOutput has already been called");
    }
}
