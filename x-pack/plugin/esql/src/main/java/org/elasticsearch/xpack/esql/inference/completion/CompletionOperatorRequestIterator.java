/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

/**
 *  This iterator reads prompts from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 *  of type {@link TaskType#COMPLETION}.
 */
public class CompletionOperatorRequestIterator implements BulkInferenceRequestIterator {

    private final PromptReader promptReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of prompts.
     *
     * @param promptBlock The input block containing prompts.
     * @param inferenceId The ID of the inference model to invoke.
     */
    public CompletionOperatorRequestIterator(BytesRefBlock promptBlock, String inferenceId) {
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

    /**
     * Wraps a single prompt string into an {@link InferenceAction.Request}.
     */
    private InferenceAction.Request inferenceRequest(String prompt) {
        if (prompt == null) {
            return null;
        }

        return InferenceAction.Request.builder(inferenceId, TaskType.COMPLETION).setInput(List.of(prompt)).build();
    }

    @Override
    public int estimatedSize() {
        return promptReader.estimatedSize();
    }

    @Override
    public void close() {
        Releasables.close(promptReader);
    }

    /**
     * Helper class that reads prompts from a {@link BytesRefBlock}.
     */
    private static class PromptReader implements Releasable {
        private final BytesRefBlock promptBlock;
        private final StringBuilder strBuilder = new StringBuilder();
        private BytesRef readBuffer = new BytesRef();

        private PromptReader(BytesRefBlock promptBlock) {
            this.promptBlock = promptBlock;
        }

        /**
         * Reads the prompt string at the given position..
         *
         * @param pos the position index in the block
         */
        public String readPrompt(int pos) {
            if (promptBlock.isNull(pos)) {
                return null;
            }

            strBuilder.setLength(0);

            for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
                readBuffer = promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
                strBuilder.append(readBuffer.utf8ToString());
                if (valueIndex != promptBlock.getValueCount(pos) - 1) {
                    strBuilder.append("\n");
                }
            }

            return strBuilder.toString();
        }

        /**
         * Returns the total number of positions (prompts) in the block.
         */
        public int estimatedSize() {
            return promptBlock.getPositionCount();
        }

        @Override
        public void close() {

        }
    }
}
