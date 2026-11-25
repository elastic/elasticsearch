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
 * The {@code longColumn} and {@code doubleColumn} entry points expose a {@code copyValues}
 * parameter: callers such as ORC that recycle their column buffers across batches pass
 * {@code true}, while callers that transfer ownership of a freshly allocated array (e.g. the
 * Parquet reader) pass {@code false} to avoid the extra copy. The remaining entry points
 * convert between primitive types and always allocate a fresh target array.
 */
public final class ColumnBlockConversions {

    private ColumnBlockConversions() {}

    /**
     * Converts a {@code long[]} column to a {@link Block} (LONG data type).
     * <p>
     * Pass {@code copyValues = true} when the caller reuses {@code values} across batches
     * (e.g. ORC's {@code ColumnVector}), and {@code false} when the caller transfers ownership
     * of a freshly allocated array.
     */
    public static Block longColumn(
        BlockFactory blockFactory,
        long[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        BitSet isNull,
        boolean copyValues
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull.get(0)) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantLongBlockWith(values[0], rowCount);
        }
        long[] data = copyValues ? Arrays.copyOf(values, rowCount) : values;
        if (noNulls) {
            return blockFactory.newLongArrayVector(data, rowCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(data, rowCount, null, isNull, Block.MvOrdering.UNORDERED);
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
        BitSet isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull.get(0)) {
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
        return blockFactory.newIntArrayBlock(ints, rowCount, null, isNull, Block.MvOrdering.UNORDERED);
    }

    /**
     * Converts a {@code double[]} column to a {@link Block} (DOUBLE data type).
     * <p>
     * Pass {@code copyValues = true} when the caller reuses {@code values} across batches
     * (e.g. ORC's {@code ColumnVector}), and {@code false} when the caller transfers ownership
     * of a freshly allocated array.
     */
    public static Block doubleColumn(
        BlockFactory blockFactory,
        double[] values,
        int rowCount,
        boolean noNulls,
        boolean isRepeating,
        BitSet isNull,
        boolean copyValues
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull.get(0)) {
                return blockFactory.newConstantNullBlock(rowCount);
            }
            return blockFactory.newConstantDoubleBlockWith(values[0], rowCount);
        }
        double[] data = copyValues ? Arrays.copyOf(values, rowCount) : values;
        if (noNulls) {
            return blockFactory.newDoubleArrayVector(data, rowCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(data, rowCount, null, isNull, Block.MvOrdering.UNORDERED);
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
        BitSet isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull.get(0)) {
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
        return blockFactory.newBooleanArrayBlock(bools, rowCount, null, isNull, Block.MvOrdering.UNORDERED);
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
        BitSet isNull
    ) {
        if (isRepeating) {
            if (noNulls == false && isNull != null && isNull.get(0)) {
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
        return blockFactory.newDoubleArrayBlock(doubles, rowCount, null, isNull, Block.MvOrdering.UNORDERED);
    }

    /**
     * Converts a {@code boolean[]} null-flag array to a {@link BitSet}.
     * Returns {@code null} when the input is {@code null}, so callers can forward an absent
     * null mask through {@code ColumnBlockConversions} entry points without a separate guard.
     */
    public static BitSet toBitSet(boolean[] isNull, int length) {
        if (isNull == null) {
            return null;
        }
        BitSet bits = new BitSet(length);
        for (int i = 0; i < length; i++) {
            if (isNull[i]) {
                bits.set(i);
            }
        }
        return bits;
    }
}
