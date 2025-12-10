/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

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
 *  This iterator reads prompts from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 *  of type {@link TaskType#COMPLETION}.
 */
class CompletionOperatorRequestIterator implements BulkRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of prompts.
     *
     * @param promptBlock The input block containing prompts.
     * @param inferenceId The ID of the inference model to invoke.
     */
    CompletionOperatorRequestIterator(BytesRefBlock promptBlock, String inferenceId) {
        this.textReader = new InputTextReader(promptBlock);
        this.size = promptBlock.getPositionCount();
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

        return new BulkRequestItem(inferenceRequest(textReader.readText(currentPos++)));
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
