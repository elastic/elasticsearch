/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Converts columnar primitive arrays to ESQL {@link Block}s, avoiding the per-element overhead
 * of Block.Builder when the source data is already in columnar form.
 * <p>
 * Used by format readers (ORC, and future Parquet ColumnReader) that produce data as typed arrays.
 * Three fast paths are provided for each type:
 * <ul>
 *   <li><b>Repeating</b>: a single value broadcast to all positions via constant block.</li>
 *   <li><b>No nulls</b>: bulk array copy + direct vector wrap (no builder, no per-element dispatch).</li>
 *   <li><b>With nulls</b>: bulk array copy + null bitmap conversion + array block.</li>
 * </ul>
 * <p>
 * All methods copy the input arrays because callers such as ORC reuse their column buffers
 * across batches. When a caller owns the array exclusively, a future overload can wrap
 * without copying.
 */
public final class ColumnBlockConversions {

    private ColumnBlockConversions() {}

    /**
     * Converts a {@code long[]} column to a {@link Block} (LONG data type).
     */
    public static Block longColumn(
        BlockFactory blockFactory,
        long[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        boolean[] isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull[0]) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantLongBlockWith(values[0], rowCount);
        }
        if (noNulls) {
            return blockFactory.newLongArrayVector(Arrays.copyOf(values, rowCount), rowCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(
            Arrays.copyOf(values, rowCount),
            rowCount,
            null,
            toBitSet(isNull, rowCount),
            Block.MvOrdering.UNORDERED
        );
    }

    /**
     * Converts a {@code long[]} column to a {@link Block} (INTEGER data type) by downcasting.
     */
    public static Block intColumnFromLongs(
        BlockFactory blockFactory,
        long[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        boolean[] isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull[0]) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantIntBlockWith((int) values[0], rowCount);
        }
        int[] ints = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            ints[i] = (int) values[i];
        }
        if (noNulls) {
            return blockFactory.newIntArrayVector(ints, rowCount).asBlock();
        }
        return blockFactory.newIntArrayBlock(ints, rowCount, null, toBitSet(isNull, rowCount), Block.MvOrdering.UNORDERED);
    }

    /**
     * Converts a {@code double[]} column to a {@link Block} (DOUBLE data type).
     */
    public static Block doubleColumn(
        BlockFactory blockFactory,
        double[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        boolean[] isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull[0]) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantDoubleBlockWith(values[0], rowCount);
        }
        if (noNulls) {
            return blockFactory.newDoubleArrayVector(Arrays.copyOf(values, rowCount), rowCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(
            Arrays.copyOf(values, rowCount),
            rowCount,
            null,
            toBitSet(isNull, rowCount),
            Block.MvOrdering.UNORDERED
        );
    }

    /**
     * Converts a {@code long[]} column to a {@link Block} (BOOLEAN data type).
     * ORC stores booleans as longs where 0 = false.
     */
    public static Block booleanColumnFromLongs(
        BlockFactory blockFactory,
        long[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        boolean[] isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull[0]) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantBooleanBlockWith(values[0] != 0, rowCount);
        }
        boolean[] bools = new boolean[rowCount];
        for (int i = 0; i < rowCount; i++) {
            bools[i] = values[i] != 0;
        }
        if (noNulls) {
            return blockFactory.newBooleanArrayVector(bools, rowCount).asBlock();
        }
        return blockFactory.newBooleanArrayBlock(bools, rowCount, null, toBitSet(isNull, rowCount), Block.MvOrdering.UNORDERED);
    }

    /**
     * Converts a {@code long[]} column to a {@link Block} (DOUBLE data type) by widening.
     * Used for ORC DECIMAL types that arrive as {@code LongColumnVector}.
     */
    public static Block doubleColumnFromLongs(
        BlockFactory blockFactory,
        long[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        boolean[] isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull[0]) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantDoubleBlockWith(values[0], rowCount);
        }
        double[] doubles = new double[rowCount];
        for (int i = 0; i < rowCount; i++) {
            doubles[i] = values[i];
        }
        if (noNulls) {
            return blockFactory.newDoubleArrayVector(doubles, rowCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(doubles, rowCount, null, toBitSet(isNull, rowCount), Block.MvOrdering.UNORDERED);
    }

    private static BitSet toBitSet(boolean[] isNull, int length) {
        BitSet bits = new BitSet(length);
        for (int i = 0; i < length; i++) {
            if (isNull[i]) {
                bits.set(i);
            }
        }
        return bits;
    }
}
