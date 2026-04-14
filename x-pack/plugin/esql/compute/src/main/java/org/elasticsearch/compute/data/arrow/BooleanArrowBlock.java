/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;

/**
 * Converts Arrow BIT vectors to ESQL BooleanBlocks.
 * Flat vectors delegate to zero-copy {@link BooleanArrowBufBlock}.
 * {@link ListVector} (multi-valued) inputs are converted by copying values into block builders.
 */
public final class BooleanArrowBlock {

    private BooleanArrowBlock() {}

    public static Block of(ValueVector vector, BlockFactory blockFactory) {
        if (vector instanceof ListVector listVector) {
            return ofList(listVector, blockFactory);
        }
        return BooleanArrowBufBlock.of((BitVector) vector, blockFactory);
    }

    private static Block ofList(ListVector listVector, BlockFactory blockFactory) {
        int rowCount = listVector.getValueCount();
        FieldVector child = listVector.getDataVector();
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (listVector.isNull(i)) {
                    builder.appendNull();
                } else {
                    int start = listVector.getElementStartIndex(i);
                    int end = listVector.getElementEndIndex(i);
                    if (start == end) {
                        builder.appendNull();
                    } else {
                        builder.beginPositionEntry();
                        for (int j = start; j < end; j++) {
                            builder.appendBoolean((Boolean) child.getObject(j));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }
}
