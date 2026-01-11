/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.textembedding;

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
 * Iterator that converts a block of text strings into inference request items for text embedding.
 * <p>
 * Each position in the text block is converted into a {@link InferenceAction.Request}
 * for text embedding inference. Null text inputs are preserved as null requests.
 * For multi-valued text fields, only the first value is used.
 * </p>
 */
class TextEmbeddingRequestIterator implements BulkInferenceRequestItemIterator {

    private final InputTextReader textReader;
    private final String inferenceId;
    private final int size;
    private int currentPos = 0;

    private final BulkInferenceRequestItem.ShapeBuilder shapeBuilder = BulkInferenceRequestItem.shapeBuilder();

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param inferenceId The ID of the inference model to invoke.
     * @param textBlock   The input block containing text to embed.
     */
    TextEmbeddingRequestIterator(String inferenceId, BytesRefBlock textBlock) {
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
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

        // Read rows until we find a non-null value or reach the end of the page.
        // For multi-valued fields, only the first value is considered to do the embedding.
        String currentText = textReader.readText(currentPos++, 1);
        while (hasNext() && currentText == null) {
            shapeBuilder.addValue(0);
            currentText = textReader.readText(currentPos++, 1);
        }

        shapeBuilder.addValue(currentText == null ? 0 : 1);

        if (currentText != null) {
            // Consume trailing null positions after the current row
            while (hasNext() && textReader.isNull(currentPos)) {
                shapeBuilder.addValue(0);
                currentPos++;
            }
        }

        return new BulkInferenceRequestItem(inferenceRequest(currentText), shapeBuilder);
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
        Releasables.closeExpectNoException(textReader);
    }

    /**
     * Factory for creating {@link TextEmbeddingRequestIterator} instances.
     */
    record Factory(String inferenceId, ExpressionEvaluator textEvaluator) implements BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new TextEmbeddingRequestIterator(inferenceId, (BytesRefBlock) textEvaluator.eval(inputPage));
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(textEvaluator);
        }
    }
}
