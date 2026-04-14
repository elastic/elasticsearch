/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;

/**
 * Converts Arrow FLOAT4 vectors to ESQL DoubleBlocks by copying and widening each float value.
 * Handles both flat vectors and {@link ListVector} (multi-valued) inputs.
 */
public final class FloatToDoubleArrowBlock {

    private FloatToDoubleArrowBlock() {}

    public static Block of(ValueVector vector, BlockFactory blockFactory) {
        if (vector instanceof ListVector listVector) {
            return ofList(listVector, blockFactory);
        }
        int rowCount = vector.getValueCount();
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (vector.isNull(i)) {
                    builder.appendNull();
                } else {
                    builder.appendDouble(((Number) vector.getObject(i)).doubleValue());
                }
            }
            return builder.build();
        }
    }

    private static Block ofList(ListVector listVector, BlockFactory blockFactory) {
        int rowCount = listVector.getValueCount();
        FieldVector child = listVector.getDataVector();
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
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
                            builder.appendDouble(((Number) child.getObject(j)).doubleValue());
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }
}
