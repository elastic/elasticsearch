/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block.MvOrdering;

import java.util.BitSet;

public class BlockFactory {

    private static final BlockFactory NON_BREAKING = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop-esql-breaker"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private final CircuitBreaker breaker;

    private final BigArrays bigArrays;

    public BlockFactory(CircuitBreaker breaker, BigArrays bigArrays) {
        this.breaker = breaker;
        this.bigArrays = bigArrays;
    }

    /**
     * Returns the Non-Breaking block factory.
     */
    public static BlockFactory getNonBreakingInstance() {
        return NON_BREAKING;
    }

    public static BlockFactory getInstance(CircuitBreaker breaker, BigArrays bigArrays) {
        return new BlockFactory(breaker, bigArrays);
    }

    // For testing
    public CircuitBreaker breaker() {
        return breaker;
    }

    // For testing
    public BigArrays bigArrays() {
        return bigArrays;
    }

    /**
     * Adjust the circuit breaker with the given delta, if the delta is negative, the breaker will
     * be adjusted without tripping.  If the data was already created before calling this method,
     * and the breaker trips, we add the delta without breaking to account for the created data.
     * If the data has not been created yet, we do not add the delta to the breaker if it trips.
     */
    void adjustBreaker(final long delta, final boolean isDataAlreadyCreated) {
        // checking breaker means potentially tripping, but it doesn't
        // have to if the delta is negative
        if (delta > 0) {
            try {
                breaker.addEstimateBytesAndMaybeBreak(delta, "<esql_block_factory>");
            } catch (CircuitBreakingException e) {
                // if (isDataAlreadyCreated) { // TODO: remove isDataAlreadyCreated
                // since we've already created the data, we need to
                // add it so closing the stream re-adjusts properly
                // breaker.addWithoutBreaking(delta);
                // }
                // re-throw the original exception
                throw e;
            }
        } else {
            breaker.addWithoutBreaking(delta);
        }
    }

    /** Pre-adjusts the breaker for the given position count and element type. Returns the pre-adjusted amount. */
    public long preAdjustBreakerForBoolean(int positionCount) {
        long bytes = (long) positionCount * Byte.BYTES;
        adjustBreaker(bytes, false);
        return bytes;
    }

    public long preAdjustBreakerForInt(int positionCount) {
        long bytes = (long) positionCount * Integer.BYTES;
        adjustBreaker(bytes, false);
        return bytes;
    }

    public long preAdjustBreakerForLong(int positionCount) {
        long bytes = (long) positionCount * Long.BYTES;
        adjustBreaker(bytes, false);
        return bytes;
    }

    public long preAdjustBreakerForDouble(int positionCount) {
        long bytes = (long) positionCount * Double.BYTES;
        adjustBreaker(bytes, false);
        return bytes;
    }

    public BooleanBlock.Builder newBooleanBlockBuilder(int estimatedSize) {
        return new BooleanBlockBuilder(estimatedSize, this);
    }

    /**
     * Build a {@link BooleanVector.FixedBuilder} that never grows.
     */
    public BooleanVector.FixedBuilder newBooleanVectorFixedBuilder(int size) {
        return new BooleanVectorFixedBuilder(size, this);
    }

    public final BooleanBlock newBooleanArrayBlock(boolean[] values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return newBooleanArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, 0L);
    }

    public BooleanBlock newBooleanArrayBlock(boolean[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrder, long preAdjustedBytes) {
        var b = new BooleanArrayBlock(values, pc, fvi, nulls, mvOrder, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public BooleanVector.Builder newBooleanVectorBuilder(int estimatedSize) {
        return new BooleanVectorBuilder(estimatedSize, this);
    }

    public final BooleanVector newBooleanArrayVector(boolean[] values, int positionCount) {
        return newBooleanArrayVector(values, positionCount, 0L);
    }

    public BooleanVector newBooleanArrayVector(boolean[] values, int positionCount, long preAdjustedBytes) {
        var b = new BooleanArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public final BooleanBlock newConstantBooleanBlockWith(boolean value, int positions) {
        return newConstantBooleanBlockWith(value, positions, 0L);
    }

    public BooleanBlock newConstantBooleanBlockWith(boolean value, int positions, long preAdjustedBytes) {
        var b = new ConstantBooleanVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public BooleanVector newConstantBooleanVector(boolean value, int positions) {
        adjustBreaker(ConstantBooleanVector.RAM_BYTES_USED, false);
        var v = new ConstantBooleanVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantBooleanVector.RAM_BYTES_USED;
        return v;
    }

    public IntBlock.Builder newIntBlockBuilder(int estimatedSize) {
        return new IntBlockBuilder(estimatedSize, this);
    }

    public final IntBlock newIntArrayBlock(int[] values, int positionCount, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return newIntArrayBlock(values, positionCount, firstValueIndexes, nulls, mvOrdering, 0L);
    }

    public IntBlock newIntArrayBlock(int[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = new IntArrayBlock(values, pc, fvi, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public IntVector.Builder newIntVectorBuilder(int estimatedSize) {
        return new IntVectorBuilder(estimatedSize, this);
    }

    /**
     * Build a {@link IntVector.FixedBuilder} that never grows.
     */
    public IntVector.FixedBuilder newIntVectorFixedBuilder(int size) {
        return new IntVectorFixedBuilder(size, this);
    }

    /**
     * Creates a new Vector with the given values and positionCount. Equivalent to:
     *   newIntArrayVector(values, positionCount, 0L); // with zero pre-adjusted bytes
     */
    public final IntVector newIntArrayVector(int[] values, int positionCount) {
        return newIntArrayVector(values, positionCount, 0L);
    }

    /**
     * Creates a new Vector with the given values and positionCount, where the caller has already
     * pre-adjusted a number of bytes with the factory's breaker.
     *
     * long preAdjustedBytes = blockFactory.preAdjustBreakerForInt(positionCount);
     * int[] values = new int[positionCount];
     * for (int i = 0; i &lt; positionCount; i++) {
     *   values[i] = doWhateverStuff
     * }
     * var vector = blockFactory.newIntArrayVector(values, positionCount, preAdjustedBytes);
     */
    public IntVector newIntArrayVector(int[] values, int positionCount, long preAdjustedBytes) {
        var b = new IntArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public final IntBlock newConstantIntBlockWith(int value, int positions) {
        return newConstantIntBlockWith(value, positions, 0L);
    }

    public IntBlock newConstantIntBlockWith(int value, int positions, long preAdjustedBytes) {
        var b = new ConstantIntVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public IntVector newConstantIntVector(int value, int positions) {
        adjustBreaker(ConstantIntVector.RAM_BYTES_USED, false);
        var v = new ConstantIntVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantIntVector.RAM_BYTES_USED;
        return v;
    }

    public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
        return new LongBlockBuilder(estimatedSize, this);
    }

    public final LongBlock newLongArrayBlock(long[] values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return newLongArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, 0L);
    }

    public LongBlock newLongArrayBlock(long[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = new LongArrayBlock(values, pc, fvi, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public LongVector.Builder newLongVectorBuilder(int estimatedSize) {
        return new LongVectorBuilder(estimatedSize, this);
    }

    /**
     * Build a {@link LongVector.FixedBuilder} that never grows.
     */
    public LongVector.FixedBuilder newLongVectorFixedBuilder(int size) {
        return new LongVectorFixedBuilder(size, this);
    }

    public final LongVector newLongArrayVector(long[] values, int positionCount) {
        return newLongArrayVector(values, positionCount, 0L);
    }

    public LongVector newLongArrayVector(long[] values, int positionCount, long preAdjustedBytes) {
        var b = new LongArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public final LongBlock newConstantLongBlockWith(long value, int positions) {
        return newConstantLongBlockWith(value, positions, 0L);
    }

    public LongBlock newConstantLongBlockWith(long value, int positions, long preAdjustedBytes) {
        var b = new ConstantLongVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public LongVector newConstantLongVector(long value, int positions) {
        adjustBreaker(ConstantLongVector.RAM_BYTES_USED, false);
        var v = new ConstantLongVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantLongVector.RAM_BYTES_USED;
        return v;
    }

    public DoubleBlock.Builder newDoubleBlockBuilder(int estimatedSize) {
        return new DoubleBlockBuilder(estimatedSize, this);
    }

    public final DoubleBlock newDoubleArrayBlock(double[] values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return newDoubleArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, 0L);

    }

    public DoubleBlock newDoubleArrayBlock(double[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = new DoubleArrayBlock(values, pc, fvi, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public DoubleVector.Builder newDoubleVectorBuilder(int estimatedSize) {
        return new DoubleVectorBuilder(estimatedSize, this);
    }

    /**
     * Build a {@link DoubleVector.FixedBuilder} that never grows.
     */
    public DoubleVector.FixedBuilder newDoubleVectorFixedBuilder(int size) {
        return new DoubleVectorFixedBuilder(size, this);
    }

    public final DoubleVector newDoubleArrayVector(double[] values, int positionCount) {
        return newDoubleArrayVector(values, positionCount, 0L);
    }

    public DoubleVector newDoubleArrayVector(double[] values, int positionCount, long preAdjustedBytes) {
        var b = new DoubleArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public final DoubleBlock newConstantDoubleBlockWith(double value, int positions) {
        return newConstantDoubleBlockWith(value, positions, 0L);
    }

    public DoubleBlock newConstantDoubleBlockWith(double value, int positions, long preAdjustedBytes) {
        var b = new ConstantDoubleVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes, true);
        return b;
    }

    public DoubleVector newConstantDoubleVector(double value, int positions) {
        adjustBreaker(ConstantDoubleVector.RAM_BYTES_USED, false);
        var v = new ConstantDoubleVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantDoubleVector.RAM_BYTES_USED;
        return v;
    }

    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize) {
        return new BytesRefBlockBuilder(estimatedSize, bigArrays, this);
    }

    public BytesRefBlock newBytesRefArrayBlock(BytesRefArray values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        var b = new BytesRefArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - values.bigArraysRamBytesUsed(), true);
        return b;
    }

    public BytesRefVector.Builder newBytesRefVectorBuilder(int estimatedSize) {
        return new BytesRefVectorBuilder(estimatedSize, bigArrays, this);
    }

    public BytesRefVector newBytesRefArrayVector(BytesRefArray values, int positionCount) {
        var b = new BytesRefArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - values.bigArraysRamBytesUsed(), true);
        return b;
    }

    public BytesRefBlock newConstantBytesRefBlockWith(BytesRef value, int positions) {
        var b = new ConstantBytesRefVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed(), true);
        return b;
    }

    public BytesRefVector newConstantBytesRefVector(BytesRef value, int positions) {
        long preadjusted = ConstantBytesRefVector.ramBytesUsed(value);
        adjustBreaker(preadjusted, false);
        var v = new ConstantBytesRefVector(value, positions, this);
        assert v.ramBytesUsed() == preadjusted;
        return v;
    }

    public Block newConstantNullBlock(int positions) {
        var b = new ConstantNullBlock(positions, this);
        adjustBreaker(b.ramBytesUsed(), true);
        return b;
    }
}
