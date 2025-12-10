/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItem;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestItemIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator over input data blocks to create batched inference requests for the Rerank task.
 *
 * <p>This iterator reads from a {@link BytesRefBlock} containing input documents or items to be reranked. It slices the input into batches
 * of configurable size and converts each batch into an {@link InferenceAction.Request} with the task type {@link TaskType#RERANK}.
 */
class RerankInferenceRequestIterator implements BulkInferenceRequestItemIterator {
    private final BytesRefBlock inputBlock;
    private final String inferenceId;
    private final String queryText;
    private final int batchSize;
    private int remainingPositions;

    private int[] shapeBuffer;
    private int shapeSize;

    RerankInferenceRequestIterator(BytesRefBlock inputBlock, String inferenceId, String queryText, int batchSize) {
        this.inputBlock = inputBlock;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.batchSize = batchSize;
        this.remainingPositions = inputBlock.getPositionCount();
        // Start with reasonable capacity
        this.shapeBuffer = new int[Math.min(batchSize, 16)];
    }

    @Override
    public boolean hasNext() {
        return remainingPositions > 0;
    }

    @Override
    public BulkInferenceRequestItem next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        shapeSize = 0;
        final int maxInputSize = Math.min(remainingPositions, batchSize);
        final List<String> inputs = new ArrayList<>(maxInputSize);
        BytesRef scratch = new BytesRef();

        int startIndex = inputBlock.getPositionCount() - remainingPositions;

        // Process batch of positions (including nulls)
        for (int i = 0; i < maxInputSize; i++) {
            int pos = startIndex + i;
            if (inputBlock.isNull(pos)) {
                // Null position - record 0 in shape and continue
                addToShape(0);
            } else {
                // Count how many values at this position
                int valueCount = inputBlock.getValueCount(pos);
                addToShape(valueCount);

                // Add all values from this position to inputs
                int firstValueIndex = inputBlock.getFirstValueIndex(pos);
                for (int v = 0; v < valueCount; v++) {
                    scratch = inputBlock.getBytesRef(firstValueIndex + v, scratch);
                    inputs.add(BytesRefs.toString(scratch));
                }
            }
        }

        remainingPositions -= shapeSize;

        // If all positions were null, return null request
        if (inputs.isEmpty()) {
            int[] shape = Arrays.copyOf(shapeBuffer, shapeSize);
            return new BulkInferenceRequestItem(null, shape);
        }

        int[] shape = Arrays.copyOf(shapeBuffer, shapeSize);
        return new BulkInferenceRequestItem(inferenceRequest(inputs), shape);
    }

    private void addToShape(int value) {
        if (shapeSize >= shapeBuffer.length) {
            // Grow buffer if needed (rare case)
            shapeBuffer = Arrays.copyOf(shapeBuffer, shapeBuffer.length * 2);
        }
        shapeBuffer[shapeSize++] = value;
    }

    @Override
    public int estimatedSize() {
        return inputBlock.getPositionCount();
    }

    private InferenceAction.Request inferenceRequest(List<String> inputs) {
        return InferenceAction.Request.builder(inferenceId, TaskType.RERANK).setInput(inputs).setQuery(queryText).build();
    }

    @Override
    public void close() {
        inputBlock.allowPassingToDifferentDriver();
        Releasables.close(inputBlock);
    }
}
