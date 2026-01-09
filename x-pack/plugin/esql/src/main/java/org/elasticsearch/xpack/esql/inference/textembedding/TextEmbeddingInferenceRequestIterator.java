/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

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
 * This iterator reads text inputs from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 * of type {@link TaskType#TEXT_EMBEDDING}.
 */
class TextEmbeddingInferenceRequestIterator implements BulkInferenceRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;

    private int[] shapeBuffer;
    private int shapeSize;

    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param textBlock   The input block containing text to embed.
     * @param inferenceId The ID of the inference model to invoke.
     */
    TextEmbeddingInferenceRequestIterator(BytesRefBlock textBlock, String inferenceId) {
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
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
        String nextText = null;

        // Consume positions until we find a non-null/non-empty value or exhaust the block
        // Keep only the first value in case of multi-valued fields.
        while (Strings.isNullOrEmpty(nextText) && currentPos < size) {
            nextText = textReader.readText(currentPos, 1);
            addToShape(Strings.isNullOrEmpty(nextText) ? 0 : 1);
            currentPos++;
        }

        // Consume trailing nulls/empty values
        while (currentPos < size && Strings.isNullOrEmpty(textReader.readText(currentPos, 1))) {
            addToShape(0);
            currentPos++;
        }

        // Create shape array of exact size
        int[] shape = Arrays.copyOf(shapeBuffer, shapeSize);
        return new BulkInferenceRequestItem(inferenceRequest(nextText), shape);
    }

    private void addToShape(int value) {
        if (shapeSize >= shapeBuffer.length) {
            // Grow buffer if needed (rare case)
            shapeBuffer = Arrays.copyOf(shapeBuffer, shapeBuffer.length * 2);
        }
        shapeBuffer[shapeSize++] = value;
    }

    /**
     * Wraps a single text string into an {@link InferenceAction.Request} for text embedding.
     */
    private InferenceAction.Request inferenceRequest(String text) {
        if (text == null) {
            return null;
        }

        return InferenceAction.Request.builder(inferenceId, TaskType.TEXT_EMBEDDING).setInput(List.of(text)).build();
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
