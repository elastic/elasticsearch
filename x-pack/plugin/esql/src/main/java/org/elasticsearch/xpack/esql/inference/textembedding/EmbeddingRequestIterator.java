/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;
import org.elasticsearch.xpack.esql.inference.InputTextReader;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterator that converts a block of text strings into inference request items for text embedding.
 * <p>
 * Each position in the text block is converted into a {@link InferenceAction.Request}
 * (or {@link EmbeddingAction.Request} when input options specify type/format)
 * for embedding inference. Null text inputs are preserved as null requests.
 * For multi-valued text fields, only the first value is used.
 * </p>
 */
class EmbeddingRequestIterator implements BulkInferenceRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final TaskType taskType;
    private final Map<String, Object> inputOptions;
    private final int size;
    private int currentPos = 0;

    private final PositionValueCountsBuilder positionValueCountsBuilder = BulkInferenceRequestItem.positionValueCountsBuilder();

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param inferenceId  The ID of the inference model to invoke.
     * @param taskType     The task type to use for inference requests.
     * @param textBlock    The input block containing text to embed.
     * @param inputOptions Optional metadata for the input value (e.g. type, format).
     */
    EmbeddingRequestIterator(String inferenceId, TaskType taskType, BytesRefBlock textBlock, Map<String, Object> inputOptions) {
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
        this.inferenceId = inferenceId;
        this.taskType = taskType;
        this.inputOptions = inputOptions;
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

        // Read rows until we find a non-null value or reach the end of the page.
        // For multi-valued fields, only the first value is considered to do the embedding.
        String currentText = textReader.readText(currentPos++, 1);
        while (hasNext() && currentText == null) {
            positionValueCountsBuilder.addValue(0);
            currentText = textReader.readText(currentPos++, 1);
        }

        positionValueCountsBuilder.addValue(currentText == null ? 0 : 1);

        if (currentText != null) {
            // Consume trailing null positions after the current row
            while (hasNext() && textReader.isNull(currentPos)) {
                positionValueCountsBuilder.addValue(0);
                currentPos++;
            }
        }

        return new BulkInferenceRequestItem(inferenceRequest(currentText), positionValueCountsBuilder);
    }

    /**
     * Wraps a single text string into an inference request for embedding.
     * When {@code inputOptions} specifies a {@code type} (and optionally a {@code format}),
     * builds an {@link EmbeddingAction.Request} with typed content via {@link InferenceStringGroup}.
     * Otherwise, falls back to a plain {@link InferenceAction.Request} with a string input.
     */
    private BaseInferenceActionRequest inferenceRequest(String text) {
        if (text == null) {
            return null;
        }

        Object typeValue = inputOptions.get("type");
        if (typeValue instanceof String typeStr) {
            DataType dataType = DataType.fromString(typeStr);
            DataFormat dataFormat = null;
            Object formatValue = inputOptions.get("format");
            if (formatValue instanceof String formatStr) {
                dataFormat = DataFormat.fromString(formatStr);
            }
            InferenceString inferenceString = dataFormat != null
                ? new InferenceString(dataType, dataFormat, text)
                : new InferenceString(dataType, text);
            InferenceStringGroup group = new InferenceStringGroup(inferenceString);
            EmbeddingRequest embeddingRequest = new EmbeddingRequest(List.of(group), InputType.UNSPECIFIED, Map.of());
            return new EmbeddingAction.Request(inferenceId, taskType, embeddingRequest, InferenceAction.Request.DEFAULT_TIMEOUT);
        }

        return InferenceAction.Request.builder(inferenceId, taskType).setInput(List.of(text)).build();
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
     * Factory for creating {@link EmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, TaskType taskType, ExpressionEvaluator textEvaluator, Map<String, Object> inputOptions)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new EmbeddingRequestIterator(
                inferenceId,
                taskType,
                (BytesRefBlock) textEvaluator.eval(inputPage),
                inputOptions
            );
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
