/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * This iterator reads text inputs from a {@link BytesRefBlock} and converts them into individual
 * {@link InferenceAction.Request} instances of type {@link TaskType#TEXT_EMBEDDING}.
 */
public class TextEmbeddingOperatorRequestIterator implements BulkInferenceRequestIterator {

    private final inputTextReader inputTextReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param inputTextBlock The input block containing text to embed.
     * @param inferenceId The ID of the inference model to invoke.
     */
    public TextEmbeddingOperatorRequestIterator(BytesRefBlock inputTextBlock, String inferenceId) {
        LogManager.getLogger(TextEmbeddingOperatorRequestIterator.class).info("inputTextBlock size {}", inputTextBlock.getPositionCount());
        this.inputTextReader = new inputTextReader(inputTextBlock);
        this.size = inputTextBlock.getPositionCount();
        this.inferenceId = inferenceId;
    }

    @Override
    public boolean hasNext() {
        return currentPos < size;
    }

    @Override
    public InferenceAction.Request next() {

        LogManager.getLogger(TextEmbeddingOperatorRequestIterator.class).info("has next {}", hasNext());
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        String inputText = inputTextReader.read(currentPos++);
        LogManager.getLogger(TextEmbeddingOperatorRequestIterator.class).info("Input text {}", inputText);
        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.of(inputText)).build();
    }

    @Override
    public void close() {
        Releasables.close(inputTextReader);
    }

    @Override
    public int estimatedSize() {
        return inputTextReader.inputTextBlock.getPositionCount();
    }

    /**
     * Helper class to read text values from a BytesRefBlock.
     */
    private static class inputTextReader implements Releasable {
        private final BytesRefBlock inputTextBlock;

        inputTextReader(BytesRefBlock inputTtextBlock) {
            this.inputTextBlock = inputTtextBlock;
        }

        String read(int position) {
            if (inputTextBlock.isNull(position)) {
                return "";
            }

            return inputTextBlock.getBytesRef(position, new BytesRef()).utf8ToString();
        }

        @Override
        public void close() {
            // BytesRefBlock is managed by the caller, no need to close here
        }
    }
}
