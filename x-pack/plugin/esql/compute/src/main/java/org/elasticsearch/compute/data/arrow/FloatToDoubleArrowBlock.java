/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;

/**
 * Converts Arrow FLOAT4 vectors to ESQL DoubleBlocks by copying and widening each float value.
 * Handles both flat vectors and {@link ListVector} (multi-valued) inputs.
 *
 * See {@link ArrowListSupport} for the Arrow list -> ESQL multi-value mapping rules
 * (null lists, empty lists, and null children are all collapsed to an ESQL null
 * position; mixed lists drop their null elements).
 */
public final class FloatToDoubleArrowBlock {

    private FloatToDoubleArrowBlock() {}

    public static Block of(ValueVector vector, BlockFactory blockFactory) {
        if (vector instanceof ListVector listVector) {
            return ofList(listVector, blockFactory);
        }
        Float4Vector floatVector = (Float4Vector) vector;
        int rowCount = floatVector.getValueCount();
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (floatVector.isNull(i)) {
                    builder.appendNull();
                } else {
                    builder.appendDouble(floatVector.get(i));
                }
            }
            return builder.build();
        }
    }

    private static Block ofList(ListVector listVector, BlockFactory blockFactory) {
        int rowCount = listVector.getValueCount();
        Float4Vector child = (Float4Vector) listVector.getDataVector();
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
            for (int i = 0; i < rowCount; i++) {
                if (listVector.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                int start = listVector.getElementStartIndex(i);
                int end = listVector.getElementEndIndex(i);
                int nonNullCount = ArrowListSupport.countNonNull(child, start, end);
                if (nonNullCount == 0) {
                    builder.appendNull();
                } else if (nonNullCount == 1) {
                    for (int j = start; j < end; j++) {
                        if (child.isNull(j) == false) {
                            builder.appendDouble(child.get(j));
                            break;
                        }
                    }
                } else {
                    boolean hasNulls = nonNullCount != (end - start);
                    builder.beginPositionEntry();
                    if (hasNulls) {
                        for (int j = start; j < end; j++) {
                            if (child.isNull(j) == false) {
                                builder.appendDouble(child.get(j));
                            }
                        }
                    } else {
                        for (int j = start; j < end; j++) {
                            builder.appendDouble(child.get(j));
                        }
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
