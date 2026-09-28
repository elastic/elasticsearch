/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem.PositionValueCountsBuilder;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Abstract base class for embedding request iterators.
 * <p>
 * Converts a block of texts into inference request items, coalescing up to {@code batchSize} embeddable texts into a
 * single request; the receiving {@code EmbeddingOutputBuilder} redistributes the embeddings back across rows via the
 * per-position value counts. Subclasses implement {@link #buildRequestItem} to produce the typed request for a batch.
 * </p>
 */
public abstract class AbstractEmbeddingRequestIterator implements BulkInferenceRequestItemIterator {

    /**
     * Emitted once per query when any input position holds more than one value. Constant because
     * {@link Warnings#registerWarning(String)} caches on the message to emit it a single time; naming the field or the number of
     * discarded values would defeat that and produce one warning per distinct field or count.
     */
    static final String MULTIVALUE_NOTICE = "embedding input is multi-valued; only the first value is embedded. "
        + "Reduce the field first, for example with MV_FIRST, to choose the value explicitly.";

    protected final String inferenceId;
    protected final TaskType taskType;
    private final InputTextReader textReader;
    private final int size;
    private final int batchSize;
    private int currentPos = 0;

    /**
     * Buffer accumulating the embeddable texts for the current batch (reused across {@link #next()} calls).
     */
    private final List<String> inputBuffer;

    private final PositionValueCountsBuilder positionValueCountsBuilder;

    /** Collects the notice emitted when a multi-valued position is reduced to its first value. */
    private final Warnings warnings;

    protected AbstractEmbeddingRequestIterator(
        String inferenceId,
        TaskType taskType,
        BytesRefBlock textBlock,
        int batchSize,
        Warnings warnings
    ) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1 but was [" + batchSize + "]");
        }
        this.inferenceId = inferenceId;
        this.taskType = taskType;
        this.warnings = warnings;
        this.textReader = new InputTextReader(textBlock);
        this.size = textBlock.getPositionCount();
        this.batchSize = batchSize;
        this.inputBuffer = new ArrayList<>(batchSize);
        this.positionValueCountsBuilder = BulkInferenceRequestItem.positionValueCountsBuilder(batchSize);
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

        inputBuffer.clear();
        positionValueCountsBuilder.reset();

        fillBatchUpToBatchSize();
        consumeTrailingNullPositions();

        // Hand off an immutable snapshot so reusing inputBuffer on the next call can never mutate an in-flight request.
        return buildRequestItem(List.copyOf(inputBuffer), positionValueCountsBuilder);
    }

    /**
     * Fills the current batch with up to {@link #batchSize} embeddable texts.
     * <p>
     * Each input position contributes at most one value: for multi-valued fields only the first value is embedded, and a
     * warning records that the remaining values were discarded. Which value is "first" depends on how the field was loaded
     * (doc values return sorted order, stored fields return source order), so the discarded values are not predictable and
     * the caller has to reduce the field explicitly to get a defined result.
     * A null position contributes no text but is still recorded as a zero count so the results can be realigned back to
     * the original rows.
     * </p>
     */
    private void fillBatchUpToBatchSize() {
        while (inputBuffer.size() < batchSize && hasNext()) {
            if (textReader.isNull(currentPos) == false && textReader.valueCount(currentPos) > 1) {
                warnings.registerWarning(MULTIVALUE_NOTICE);
            }
            // For multi-valued fields, only the first value is considered to do the embedding.
            String text = textReader.readText(currentPos++, 1);
            if (text == null) {
                positionValueCountsBuilder.addValue(0);
            } else {
                inputBuffer.add(text);
                positionValueCountsBuilder.addValue(1);
            }
        }
    }

    /**
     * After the batch is full, bundles any trailing null positions into the current request so they don't force an
     * additional (empty) request. Stops at the first non-null position.
     */
    private void consumeTrailingNullPositions() {
        while (hasNext() && textReader.isNull(currentPos)) {
            positionValueCountsBuilder.addValue(0);
            currentPos++;
        }
    }

    /**
     * Subclasses produce a typed inference request for a batch of texts.
     *
     * @param texts the texts to embed in this request, in row order; empty when the batch contains only null inputs
     * @param pvcs  the position value counts builder for this batch
     * @return the constructed request item
     */
    protected abstract BulkInferenceRequestItem buildRequestItem(List<String> texts, PositionValueCountsBuilder pvcs);

    @Override
    public int estimatedSize() {
        // Each request packs up to batchSize non-null texts; null positions ride along without taking a slot. So the real
        // request count is at most positions / batchSize (rounded up) — exact when there are no nulls, fewer when there are.
        // This is only a sizing hint for the caller's response list, so an upper bound is fine.
        return Math.ceilDiv(textReader.estimatedSize(), batchSize);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(textReader);
    }
}
