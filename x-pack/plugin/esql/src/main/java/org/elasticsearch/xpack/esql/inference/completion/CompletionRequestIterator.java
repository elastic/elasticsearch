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
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;
import org.elasticsearch.xpack.esql.inference.InputTextReader;

import java.util.List;
import java.util.Map;
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
    private final Map<String, Object> taskSettings;
    private final int size;
    private final PositionValueCountsBuilder positionValueCountsBuilder = BulkInferenceRequestItem.positionValueCountsBuilder();

    private int currentPos = 0;

    /**
     * Constructs a new iterator from the given block of prompts.
     *
     * @param inferenceId The ID of the inference model to invoke.
     * @param promptBlock The input block containing prompts.
     * @param taskSettings Task-specific settings to include in inference requests.
     */
    CompletionRequestIterator(String inferenceId, BytesRefBlock promptBlock, Map<String, Object> taskSettings) {
        this.textReader = new InputTextReader(promptBlock);
        this.size = promptBlock.getPositionCount();
        this.inferenceId = inferenceId;
        this.taskSettings = taskSettings;
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

        positionValueCountsBuilder.reset();

        // Read rows until we find a non-null/non-blank value or reach the end of the page
        String currentPrompt = textReader.readText(currentPos++);
        while (hasNext() && Strings.isNullOrBlank(currentPrompt)) {
            positionValueCountsBuilder.addValue(0);
            currentPrompt = textReader.readText(currentPos++);
        }

        positionValueCountsBuilder.addValue(Strings.isNullOrBlank(currentPrompt) ? 0 : 1);

        if (Strings.hasText(currentPrompt)) {
            // Consume trailing null positions after the current row
            while (hasNext() && textReader.isNull(currentPos)) {
                positionValueCountsBuilder.addValue(0);
                currentPos++;
            }
        }

        return new BulkInferenceRequestItem(inferenceRequest(currentPrompt), positionValueCountsBuilder);
    }

    /**
     * Wraps a single prompt string into an {@link InferenceAction.Request}.
     */
    private InferenceAction.Request inferenceRequest(String prompt) {
        if (Strings.isNullOrBlank(prompt)) {
            return null;
        }

        InferenceAction.Request.Builder builder = InferenceAction.Request.builder(inferenceId, TaskType.COMPLETION)
            .setInput(List.of(prompt));

        // Only set task settings if explicitly provided by the user.
        // This preserves backward compatibility and avoids sending empty
        // maps to the inference service, which could have unexpected behavior.
        if (taskSettings != null && taskSettings.isEmpty() == false) {
            builder.setTaskSettings(taskSettings);
        }

        return builder.build();
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
    record Factory(String inferenceId, ExpressionEvaluator promptEvaluator, Map<String, Object> taskSettings)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new CompletionRequestIterator(inferenceId, (BytesRefBlock) promptEvaluator.eval(inputPage), taskSettings);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(promptEvaluator);
        }
    }
}
