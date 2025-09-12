/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * This iterator reads prompts from a {@link BytesRefBlock} and converts them into individual {@link InferenceAction.Request} instances
 * of type {@link TaskType#CHAT_COMPLETION}.
 */
public class ChatCompletionOperatorRequestIterator implements BulkInferenceRequestIterator {

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
    public ChatCompletionOperatorRequestIterator(BytesRefBlock promptBlock, String inferenceId) {
        this.promptReader = new PromptReader(promptBlock);
        this.size = promptBlock.getPositionCount();
        this.inferenceId = inferenceId;
    }

    @Override
    public boolean hasNext() {
        return currentPos < size;
    }

    @Override
    public BulkInferenceRequestItem.ChatCompletionRequestItem next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        UnifiedCompletionAction.Request inferenceRequest = inferenceRequest(promptReader.readPrompt(currentPos++));
        return BulkInferenceRequestItem.from(inferenceRequest);
    }

    /**
     * Wraps a single prompt string into an {@link UnifiedCompletionRequest}.
     */
    private UnifiedCompletionAction.Request inferenceRequest(String prompt) {
        if (prompt == null) {
            return null;
        }

        return new UnifiedCompletionAction.Request(
            inferenceId,
            TaskType.CHAT_COMPLETION,
            UnifiedCompletionRequest.of(
                List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(prompt), "user", null, null))
            ),
            TimeValue.THIRTY_SECONDS
        );
    }

    @Override
    public int estimatedSize() {
        return promptReader.estimatedSize();
    }

    @Override
    public void close() {
        Releasables.close(promptReader);
    }
}
