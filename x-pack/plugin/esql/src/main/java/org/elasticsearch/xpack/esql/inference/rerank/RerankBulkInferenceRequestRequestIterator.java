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
import org.elasticsearch.xpack.esql.inference.InferenceRequestBuilderSupplier;
import org.elasticsearch.xpack.esql.inference.InferenceRequestSupplier;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;

import java.util.List;
import java.util.NoSuchElementException;

public class RerankBulkInferenceRequestRequestIterator implements BulkInferenceRequestIterator {

    private final BytesRefBlock inputBlock;
    private final InferenceRequestBuilderSupplier requestBuilderSupplier;

    boolean done = false;

    public RerankBulkInferenceRequestRequestIterator(BytesRefBlock inputBlock, InferenceRequestBuilderSupplier requestBuilderSupplier) {
        this.inputBlock = inputBlock;
        this.requestBuilderSupplier = requestBuilderSupplier;
    }

    @Override
    public void close() {
        inputBlock.allowPassingToDifferentDriver();
        Releasables.closeExpectNoException(inputBlock);
    }

    @Override
    public boolean hasNext() {
        return done == false;
    }

    @Override
    public InferenceRequestSupplier next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }

        done = true;

        return () -> {
            String[] inputs = new String[inputBlock.getPositionCount()];
            BytesRef buffer = new BytesRef();

            for (int pos = 0; pos < inputBlock.getPositionCount(); pos++) {
                if (inputBlock.isNull(pos)) {
                    inputs[pos] = "";
                } else {
                    buffer = inputBlock.getBytesRef(inputBlock.getFirstValueIndex(pos), buffer);
                    inputs[pos] = BytesRefs.toString(buffer);
                }
            }

            return requestBuilderSupplier.get().setInput(List.of(inputs)).build();
        };
    }
}
