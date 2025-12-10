/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InputTextReader;
import org.elasticsearch.xpack.esql.inference.bulk.BulkRequestItem;
import org.elasticsearch.xpack.esql.inference.bulk.BulkRequestItemIterator;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * This iterator reads text inputs from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 * of type {@link TaskType#TEXT_EMBEDDING}.
 */
class TextEmbeddingOperatorRequestIterator implements BulkRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param textBlock   The input block containing text to embed.
     * @param inferenceId The ID of the inference model to invoke.
     */
    TextEmbeddingOperatorRequestIterator(BytesRefBlock textBlock, String inferenceId) {
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
        this.inferenceId = inferenceId;
    }

    @Override
    public boolean hasNext() {
        return currentPos < size;
    }

    @Override
    public BulkRequestItem next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        /*
         * Keep only the first value in case of multi-valued fields.
         * TODO: check if it is consistent with how the query vector builder is working.
         */
        return new BulkRequestItem(inferenceRequest(textReader.readText(currentPos++, 1)));
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
