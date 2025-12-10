/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InputTextReader;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItemIterator;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *  This iterator reads prompts from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 *  of type {@link TaskType#COMPLETION}.
 */
class CompletionInferenceRequestIterator implements BulkInferenceRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;

    private int[] shapeBuffer;
    private int shapeSize;

    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of prompts.
     *
     * @param promptBlock The input block containing prompts.
     * @param inferenceId The ID of the inference model to invoke.
     */
    CompletionInferenceRequestIterator(BytesRefBlock promptBlock, String inferenceId) {
        this.textReader = new InputTextReader(promptBlock);
        this.size = promptBlock.getPositionCount();
        this.inferenceId = inferenceId;
        // Start with reasonable capacity (most shapes are small)
        this.shapeBuffer = new int[16];
    }

    @Override
    public boolean hasNext() {
        return currentPos < size;
    }

    @Override
    public BulkInferenceRequestItem next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        shapeSize = 0;
        String nextPrompt = null;

        // Consume leading nulls and find first non-null
        while (Strings.isNullOrEmpty(nextPrompt) && currentPos < size) {
            nextPrompt = textReader.readText(currentPos);
            addToShape(Strings.isNullOrEmpty(nextPrompt) ? 0 : 1);
            currentPos++;
        }

        // Consume trailing nulls
        while (currentPos < size && Strings.isNullOrEmpty(textReader.readText(currentPos))) {
            addToShape(0);
            currentPos++;
        }

        // Create shape array of exact size
        int[] shape = Arrays.copyOf(shapeBuffer, shapeSize);
        return new BulkInferenceRequestItem(inferenceRequest(nextPrompt), shape);
    }

    private void addToShape(int value) {
        if (shapeSize >= shapeBuffer.length) {
            // Grow buffer if needed (rare case)
            shapeBuffer = Arrays.copyOf(shapeBuffer, shapeBuffer.length * 2);
        }
        shapeBuffer[shapeSize++] = value;
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
        return textReader.estimatedSize();
    }

    @Override
    public void close() {
        Releasables.close(textReader);
    }
}
