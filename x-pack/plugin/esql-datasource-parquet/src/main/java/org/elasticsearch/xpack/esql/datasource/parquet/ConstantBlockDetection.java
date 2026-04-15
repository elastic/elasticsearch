/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

import java.util.BitSet;

/**
 * Post-decode constant detection for Parquet column batches. After values are decoded into arrays,
 * these methods check whether all non-null values are identical and, if so, return a constant
 * {@link Block} instead of an array-backed block. This turns O(n) downstream consumers into O(1)
 * for partition columns and other repeated-value scenarios common in Parquet files.
 *
 * <p>Returns {@code null} when the values are not constant, signaling the caller to fall through
 * to the normal array-block path.
 */
final class ConstantBlockDetection {

    private ConstantBlockDetection() {}

    /**
     * Returns a constant boolean block if all values are identical, or null otherwise.
     * Only called for the non-nullable (no nulls) path.
     */
    static Block tryConstantBoolean(boolean[] values, int rows, BlockFactory blockFactory) {
        if (rows <= 1) {
            return blockFactory.newConstantBooleanBlockWith(rows == 0 ? false : values[0], rows);
        }
        boolean first = values[0];
        for (int i = 1; i < rows; i++) {
            if (values[i] != first) {
                return null;
            }
        }
        return blockFactory.newConstantBooleanBlockWith(first, rows);
    }

    /**
     * Returns a constant int block if all values are identical, or null otherwise.
     * Only called for the non-nullable (no nulls) path.
     */
    static Block tryConstantInt(int[] values, int rows, BlockFactory blockFactory) {
        if (rows <= 1) {
            return blockFactory.newConstantIntBlockWith(rows == 0 ? 0 : values[0], rows);
        }
        int first = values[0];
        for (int i = 1; i < rows; i++) {
            if (values[i] != first) {
                return null;
            }
        }
        return blockFactory.newConstantIntBlockWith(first, rows);
    }

    /**
     * Returns a constant long block if all values are identical, or null otherwise.
     * Only called for the non-nullable (no nulls) path.
     */
    static Block tryConstantLong(long[] values, int rows, BlockFactory blockFactory) {
        if (rows <= 1) {
            return blockFactory.newConstantLongBlockWith(rows == 0 ? 0L : values[0], rows);
        }
        long first = values[0];
        for (int i = 1; i < rows; i++) {
            if (values[i] != first) {
                return null;
            }
        }
        return blockFactory.newConstantLongBlockWith(first, rows);
    }

    /**
     * Returns a constant double block if all values are identical, or null otherwise.
     * Only called for the non-nullable (no nulls) path.
     * Uses {@code Double.doubleToRawLongBits} for bitwise comparison to handle NaN correctly
     * (all NaN bit patterns are treated as distinct unless bitwise identical).
     */
    static Block tryConstantDouble(double[] values, int rows, BlockFactory blockFactory) {
        if (rows <= 1) {
            return blockFactory.newConstantDoubleBlockWith(rows == 0 ? 0.0 : values[0], rows);
        }
        long firstBits = Double.doubleToRawLongBits(values[0]);
        for (int i = 1; i < rows; i++) {
            if (Double.doubleToRawLongBits(values[i]) != firstBits) {
                return null;
            }
        }
        return blockFactory.newConstantDoubleBlockWith(values[0], rows);
    }

    /**
     * Returns a constant BytesRef block if all values in the builder are identical, or null.
     * This is checked by examining the first and subsequent BytesRef values for equality.
     * Only called for the non-nullable (no nulls) path; the values array must be fully populated.
     */
    static Block tryConstantBytesRef(BytesRef[] values, int rows, BlockFactory blockFactory) {
        if (rows <= 1) {
            return blockFactory.newConstantBytesRefBlockWith(rows == 0 ? new BytesRef() : values[0], rows);
        }
        BytesRef first = values[0];
        for (int i = 1; i < rows; i++) {
            if (first.bytesEquals(values[i]) == false) {
                return null;
            }
        }
        return blockFactory.newConstantBytesRefBlockWith(new BytesRef(first.bytes, first.offset, first.length), rows);
    }

    /**
     * Checks if all rows in a nullable batch are null. When {@code nullCount == rows},
     * returns a constant null block; otherwise returns null to signal fall-through.
     */
    static Block tryAllNull(BitSet nulls, int rows, BlockFactory blockFactory) {
        if (nulls.cardinality() == rows) {
            return blockFactory.newConstantNullBlock(rows);
        }
        return null;
    }
}
