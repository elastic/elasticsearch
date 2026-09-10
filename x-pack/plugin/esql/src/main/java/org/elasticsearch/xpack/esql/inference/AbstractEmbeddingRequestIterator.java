/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.NoSuchElementException;

/**
 * Abstract base class for embedding request iterators.
 * <p>
 * Provides common iteration logic for converting a block of text strings into inference request items.
 * Subclasses implement {@link #buildRequestItem} to produce an inference typed requests
 * </p>
 */
public abstract class AbstractEmbeddingRequestIterator implements BulkInferenceRequestItemIterator {

    protected final String inferenceId;
    protected final TaskType taskType;
    private final InputTextReader textReader;
    private final int size;
    private int currentPos = 0;

    private final PositionValueCountsBuilder positionValueCountsBuilder = BulkInferenceRequestItem.positionValueCountsBuilder();

    protected AbstractEmbeddingRequestIterator(String inferenceId, TaskType taskType, BytesRefBlock textBlock) {
        this.inferenceId = inferenceId;
        this.taskType = taskType;
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
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

        return buildRequestItem(currentText, positionValueCountsBuilder);
    }

    /**
     * Subclasses produce a typed inference request
     *
     * @param text the text to embed, or null for a null-input position
     * @param pvcs the position value counts builder for this batch
     * @return the constructed request item
     */
    protected abstract BulkInferenceRequestItem buildRequestItem(String text, PositionValueCountsBuilder pvcs);

    @Override
    public int estimatedSize() {
        return textReader.estimatedSize();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(textReader);
    }
}
