/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.completion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRequestBuilderSupplier;
import org.elasticsearch.xpack.esql.inference.InferenceRequestSupplier;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

public class CompletionBulkInferenceRequestIterator implements BulkInferenceRequestIterator {
    private final InferenceRequestBuilderSupplier requestBuilderSupplier;
    private final BytesRefBlock promptBlock;
    private final BytesRef readBuffer = new BytesRef();
    private int currentPos = 0;

    public CompletionBulkInferenceRequestIterator(BytesRefBlock promptBlock, InferenceRequestBuilderSupplier requestBuilderSupplier) {
        this.promptBlock = promptBlock;
        this.requestBuilderSupplier = requestBuilderSupplier;
    }

    @Override
    public boolean hasNext() {
        return currentPos < promptBlock.getPositionCount();
    }

    @Override
    public InferenceRequestSupplier next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        int pos = currentPos++;

        return () -> {
            if (promptBlock.isNull(pos)) {
                return null;
            }

            StringBuilder promptBuilder = new StringBuilder();
            for (int valueIndex = 0; valueIndex < promptBlock.getValueCount(pos); valueIndex++) {
                promptBlock.getBytesRef(promptBlock.getFirstValueIndex(pos) + valueIndex, readBuffer);
                promptBuilder.append(readBuffer.utf8ToString()).append("\n");
            }

            return inferenceRequest(promptBuilder.toString());
        };
    }

    private InferenceAction.Request inferenceRequest(String prompt) {
        return requestBuilderSupplier.get().setInput(List.of(prompt)).build();
    }

    @Override
    public void close() {
        promptBlock.allowPassingToDifferentDriver();
        Releasables.closeExpectNoException(promptBlock);
    }
}
