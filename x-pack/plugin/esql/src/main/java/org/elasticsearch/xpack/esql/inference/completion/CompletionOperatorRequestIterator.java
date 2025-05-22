/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

public class CompletionOperatorRequestIterator implements BulkInferenceRequestIterator {

    private final PromptReader promptReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    public CompletionOperatorRequestIterator(Page inputPage, String inferenceId) {
        assert inputPage.getBlockCount() > 0;
        BytesRefBlock promptBlock = inputPage.getBlock(inputPage.getBlockCount() - 1);
        this.promptReader = new PromptReader(promptBlock);
        this.size = promptBlock.getPositionCount();
        this.inferenceId = inferenceId;
    }

    @Override
    public boolean hasNext() {
        return currentPos < size;
    }

    @Override
    public InferenceAction.Request next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        return inferenceRequest(promptReader.readPrompt(currentPos++));
    }

    private InferenceAction.Request inferenceRequest(String prompt) {
        return InferenceAction.Request.builder(inferenceId, TaskType.COMPLETION).setInput(List.of(prompt)).build();
    }

    private static class PromptReader {
        private final BytesRefBlock promptBlock;
        private final StringBuilder strBuilder = new StringBuilder();
        private BytesRef readBuffer = new BytesRef();

        private PromptReader(BytesRefBlock promptBlock) {
            this.promptBlock = promptBlock;
        }

        public String readPrompt(int pos) {
            if (promptBlock.isNull(pos)) {
                return null;
            }

            strBuilder.setLength(0);

            for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
                readBuffer = promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
                strBuilder.append(readBuffer.utf8ToString());
                if (valueIndex != promptBlock.getValueCount(pos) -1) {
                    strBuilder.append("\n");
                }
            }

            return strBuilder.toString();
        }
    }
}
