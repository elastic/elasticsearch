/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.InferenceOperator.BulkInferenceRequestItemIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that converts a block of text strings into batched inference request items for reranking.
 * <p>
 * This iterator groups multiple positions into a single inference request (up to {@code batchSize} documents
 * per request) for efficient batch processing. It supports:
 * <ul>
 *   <li>Multi-valued positions: Each position can contribute zero or more documents</li>
 *   <li>Null/empty filtering: Positions with null or empty text are tracked but don't generate documents</li>
 *   <li>Trailing null bundling: Null positions after reaching batch size are bundled with the current batch</li>
 * </ul>
 * <p>
 * The shape array tracks how many documents each position contributes, enabling proper result alignment
 * when scores are returned. For example, a shape of [1, 0, 2, 1] indicates the first position contributed
 * 1 document, the second contributed none (null/empty), the third contributed 2 documents, and the fourth
 * contributed 1 document.
 */
class RerankRequestIterator implements BulkInferenceRequestItemIterator {

    private final BytesRefBlock textBlock;
    private final String inferenceId;
    private final String queryText;
    private final int batchSize;
    private final int totalPositions;

    /**
     * Current position being processed in the input block.
     */
    private int currentPos = 0;

    private final BulkInferenceRequestItem.ShapeBuilder shapeBuilder;

    /**
     * Reusable buffer for reading BytesRef values from the block.
     */
    private BytesRef scratch = new BytesRef();

    /**
     * Buffer accumulating document texts for the current batch.
     */
    private final List<String> inputBuffer;

    /**
     * Constructs a new iterator from the given block of text inputs.
     *
     * @param inferenceId The ID of the inference model to invoke.
     * @param queryText   The query text to use for reranking.
     * @param textBlock   The input block containing documents to rerank.
     * @param batchSize   The maximum number of documents to include in a single inference request.
     */
    RerankRequestIterator(String inferenceId, String queryText, BytesRefBlock textBlock, int batchSize) {
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.textBlock = textBlock;
        this.batchSize = batchSize;
        this.totalPositions = textBlock.getPositionCount();
        this.inputBuffer = new ArrayList<>(batchSize);
        this.shapeBuilder = BulkInferenceRequestItem.shapeBuilder(batchSize);
    }

    @Override
    public boolean hasNext() {
        return currentPos < totalPositions;
    }

    @Override
    public BulkInferenceRequestItem next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        resetBatchState();
        fillBatchUpToBatchSize();
        consumeTrailingNullOrEmptyPositions();

        return new BulkInferenceRequestItem(inferenceRequest(inputBuffer), shapeBuilder);
    }

    /**
     * Resets the state for building a new batch.
     */
    private void resetBatchState() {
        inputBuffer.clear();
        shapeBuilder.reset();
    }

    /**
     * Fills the batch with documents up to the configured batch size.
     * Stops when either the batch size is reached or no more positions are available.
     */
    private void fillBatchUpToBatchSize() {
        while (inputBuffer.size() < batchSize && currentPos < totalPositions) {
            List<String> inputs = readInputText(currentPos);
            inputBuffer.addAll(inputs);
            shapeBuilder.addValue(inputs.size());
            currentPos++;
        }
    }

    /**
     * After the batch size is met, continues consuming any trailing null or empty positions.
     * This reduces the number of inference requests by bundling null positions with the preceding batch.
     * Stops when a non-empty position is encountered.
     */
    private void consumeTrailingNullOrEmptyPositions() {
        while (currentPos < totalPositions) {
            List<String> inputs = readInputText(currentPos);
            if (inputs.isEmpty()) {
                shapeBuilder.addValue(0);
                currentPos++;
            } else {
                break;
            }
        }
    }

    /**
     * Reads all non-empty text values from the specified position in the input block.
     * <p>
     * Supports multi-valued positions by reading all values at the given position.
     * Filters out empty or whitespace-only strings using {@link Strings#hasText}.
     * </p>
     *
     * @param position The position in the block to read from.
     * @return A list of non-empty, non-whitespace text strings. Returns an empty list if the
     *         position is null or contains only empty/whitespace values.
     */
    private List<String> readInputText(int position) {
        if (textBlock.isNull(position)) {
            return List.of();
        }

        int valueCount = textBlock.getValueCount(position);
        List<String> inputs = new ArrayList<>(valueCount);

        int firstValueIndex = textBlock.getFirstValueIndex(position);
        for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
            scratch = textBlock.getBytesRef(firstValueIndex + valueIndex, scratch);
            String inputText = scratch.utf8ToString();

            // Filter out empty and whitespace-only strings
            if (Strings.hasText(inputText)) {
                inputs.add(inputText);
            }
        }

        return inputs;
    }

    /**
     * Creates an inference request for reranking the given documents.
     *
     * @param inputs The list of document texts to rerank.
     * @return An inference request, or null if there are no inputs.
     */
    private InferenceAction.Request inferenceRequest(List<String> inputs) {
        if (inputs.isEmpty()) {
            return null;
        }

        // Create a defensive copy since the inputBuffer is reused for subsequent batches
        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(List.copyOf(inputs)).setQuery(queryText).build();
    }

    @Override
    public int estimatedSize() {
        return totalPositions;
    }

    @Override
    public void close() {
        textBlock.allowPassingToDifferentDriver();
        Releasables.closeExpectNoException(textBlock);
    }

    /**
     * Factory for creating {@link RerankRequestIterator} instances.
     */
    record Factory(String inferenceId, String queryText, ExpressionEvaluator rowEncoder, int batchSize)
        implements
            BulkInferenceRequestItemIterator.Factory {

        @Override
        public BulkInferenceRequestItemIterator create(Page inputPage) {
            return new RerankRequestIterator(inferenceId, queryText, (BytesRefBlock) rowEncoder.eval(inputPage), batchSize);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(rowEncoder);
        }
    }
}
