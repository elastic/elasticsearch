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
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class RerankOperatorRequestIterator implements BulkInferenceRequestIterator {
    private final BytesRefBlock inputBlock;
    private final String inferenceId;
    private final String queryText;
    private final int batchSize;
    private int remainingPositions;

    public RerankOperatorRequestIterator(BytesRefBlock inputBlock, String inferenceId, String queryText, int batchSize) {
        this.inputBlock = inputBlock;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.batchSize = batchSize;
        this.remainingPositions = inputBlock.getPositionCount();
    }

    @Override
    public boolean hasNext() {
        return remainingPositions > 0;
    }

    @Override
    public InferenceAction.Request next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        final int inputSize = Math.min(remainingPositions, batchSize);
        final List<String> inputs = new ArrayList<>(inputSize);
        BytesRef scratch = new BytesRef();

        int startIndex = inputBlock.getPositionCount() - remainingPositions;
        for (int i = 0; i < inputSize; i++) {
            int pos = startIndex + i;
            if (inputBlock.isNull(pos)) {
                inputs.add("");
            } else {
                scratch = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(pos), scratch);
                inputs.add(BytesRefs.toString(scratch));
            }
        }

        remainingPositions -= inputSize;
        return inferenceRequest(inputs);
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
