/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;
import org.elasticsearch.xpack.esql.inference.InputTextReader;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that converts a block of prompt strings into inference request items.
 * <p>
 * Each position in the prompt block is converted into a {@link InferenceAction.Request}
 * for completion inference. Null prompts are preserved as null requests.
 * </p>
 */
class CompletionRequestIterator implements BulkInferenceRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;
    private final BulkInferenceRequestItem.ShapeBuilder shapeBuilder = BulkInferenceRequestItem.shapeBuilder();

    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of prompts.
     *
     * @param inferenceId The ID of the inference model to invoke.
     * @param promptBlock The input block containing prompts.
     */
    CompletionRequestIterator(String inferenceId, BytesRefBlock promptBlock) {
        this.textReader = new InputTextReader(promptBlock);
        this.size = promptBlock.getPositionCount();
        this.inferenceId = inferenceId;
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

        shapeBuilder.reset();

        // Read rows until we find a non-null/non-blank value or reach the end of the page
        String currentPrompt = textReader.readText(currentPos++);
        while (hasNext() && Strings.isNullOrBlank(currentPrompt)) {
            shapeBuilder.addValue(0);
            currentPrompt = textReader.readText(currentPos++);
        }

        shapeBuilder.addValue(Strings.isNullOrBlank(currentPrompt) ? 0 : 1);

        if (Strings.hasText(currentPrompt)) {
            // Consume trailing null positions after the current row
            while (hasNext() && textReader.isNull(currentPos)) {
                shapeBuilder.addValue(0);
                currentPos++;
            }
        }

        return new BulkInferenceRequestItem(inferenceRequest(currentPrompt), shapeBuilder);
    }

    /**
     * Wraps a single prompt string into an {@link InferenceAction.Request}.
     */
    private InferenceAction.Request inferenceRequest(String prompt) {
        if (Strings.isNullOrBlank(prompt)) {
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
        Releasables.closeExpectNoException(textReader);
    }

    /**
     * Factory for creating {@link CompletionRequestIterator} instances.
     */
    record Factory(String inferenceId, ExpressionEvaluator promptEvaluator) implements BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new CompletionRequestIterator(inferenceId, (BytesRefBlock) promptEvaluator.eval(inputPage));
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(promptEvaluator);
        }
    }
}
