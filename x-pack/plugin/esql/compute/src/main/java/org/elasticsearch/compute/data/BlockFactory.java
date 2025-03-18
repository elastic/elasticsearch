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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block.MvOrdering;

import java.util.BitSet;

public class BlockFactory {
    public static final String LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING = "esql.block_factory.local_breaker.over_reserved";
    public static final ByteSizeValue LOCAL_BREAKER_OVER_RESERVED_DEFAULT_SIZE = ByteSizeValue.ofKb(4);

    public static final String LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING = "esql.block_factory.local_breaker.max_over_reserved";
    public static final ByteSizeValue LOCAL_BREAKER_OVER_RESERVED_DEFAULT_MAX_SIZE = ByteSizeValue.ofKb(16);

    public static final String MAX_BLOCK_PRIMITIVE_ARRAY_SIZE_SETTING = "esql.block_factory.max_block_primitive_array_size";
    public static final ByteSizeValue DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE = ByteSizeValue.ofKb(512);

    private final CircuitBreaker breaker;

    private final BigArrays bigArrays;
    private final long maxPrimitiveArrayBytes;
    private final BlockFactory parent;

    public BlockFactory(CircuitBreaker breaker, BigArrays bigArrays) {
        this(breaker, bigArrays, DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE);
    }

    public BlockFactory(CircuitBreaker breaker, BigArrays bigArrays, ByteSizeValue maxPrimitiveArraySize) {
        this(breaker, bigArrays, maxPrimitiveArraySize, null);
    }

    protected BlockFactory(CircuitBreaker breaker, BigArrays bigArrays, ByteSizeValue maxPrimitiveArraySize, BlockFactory parent) {
        assert breaker instanceof LocalCircuitBreaker == false
            || (parent != null && ((LocalCircuitBreaker) breaker).parentBreaker() == parent.breaker)
            : "use local breaker without parent block factory";
        this.breaker = breaker;
        this.bigArrays = bigArrays;
        this.parent = parent;
        this.maxPrimitiveArrayBytes = maxPrimitiveArraySize.getBytes();
    }

    public static BlockFactory getInstance(CircuitBreaker breaker, BigArrays bigArrays) {
        return new BlockFactory(breaker, bigArrays, DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE, null);
    }

    // For testing
    public CircuitBreaker breaker() {
        return breaker;
    }

    // For testing
    public BigArrays bigArrays() {
        return bigArrays;
    }

    protected BlockFactory parent() {
        return parent != null ? parent : this;
    }

    public BlockFactory newChildFactory(LocalCircuitBreaker childBreaker) {
        if (childBreaker.parentBreaker() != breaker) {
            throw new IllegalStateException("Different parent breaker");
        }
        return new BlockFactory(childBreaker, bigArrays, ByteSizeValue.ofBytes(maxPrimitiveArrayBytes), this);
    }

    /**
     * Adjust the circuit breaker with the given delta, if the delta is negative, the breaker will
     * be adjusted without tripping.
     * @throws CircuitBreakingException if the breaker was put above its limit
     */
    public void adjustBreaker(final long delta) throws CircuitBreakingException {
        // checking breaker means potentially tripping, but it doesn't
        // have to if the delta is negative
        if (delta > 0) {
            breaker.addEstimateBytesAndMaybeBreak(delta, "<esql_block_factory>");
        } else {
            breaker.addWithoutBreaking(delta);
        }
    }

    /** Pre-adjusts the breaker for the given position count and element type. Returns the pre-adjusted amount. */
    public long preAdjustBreakerForBoolean(int positionCount) {
        long bytes = (long) positionCount * Byte.BYTES;
        adjustBreaker(bytes);
        return bytes;
    }

    public long preAdjustBreakerForInt(int positionCount) {
        long bytes = (long) positionCount * Integer.BYTES;
        adjustBreaker(bytes);
        return bytes;
    }

    public long preAdjustBreakerForLong(int positionCount) {
        long bytes = (long) positionCount * Long.BYTES;
        adjustBreaker(bytes);
        return bytes;
    }

    public long preAdjustBreakerForDouble(int positionCount) {
        long bytes = (long) positionCount * Double.BYTES;
        adjustBreaker(bytes);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public final BooleanBlock newConstantBooleanBlockWith(boolean value, int positions) {
        return newConstantBooleanBlockWith(value, positions, 0L);
    }

    public BooleanBlock newConstantBooleanBlockWith(boolean value, int positions, long preAdjustedBytes) {
        var b = new ConstantBooleanVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public BooleanVector newConstantBooleanVector(boolean value, int positions) {
        adjustBreaker(ConstantBooleanVector.RAM_BYTES_USED);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public final IntBlock newConstantIntBlockWith(int value, int positions) {
        return newConstantIntBlockWith(value, positions, 0L);
    }

    public IntBlock newConstantIntBlockWith(int value, int positions, long preAdjustedBytes) {
        var b = new ConstantIntVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public IntVector newConstantIntVector(int value, int positions) {
        adjustBreaker(ConstantIntVector.RAM_BYTES_USED);
        var v = new ConstantIntVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantIntVector.RAM_BYTES_USED;
        return v;
    }

    public FloatBlock.Builder newFloatBlockBuilder(int estimatedSize) {
        return new FloatBlockBuilder(estimatedSize, this);
    }

    public final FloatBlock newFloatArrayBlock(float[] values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        return newFloatArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, 0L);
    }

    public FloatBlock newFloatArrayBlock(float[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = new FloatArrayBlock(values, pc, fvi, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public FloatVector.Builder newFloatVectorBuilder(int estimatedSize) {
        return new FloatVectorBuilder(estimatedSize, this);
    }

    /**
     * Build a {@link FloatVector.FixedBuilder} that never grows.
     */
    public FloatVector.FixedBuilder newFloatVectorFixedBuilder(int size) {
        return new FloatVectorFixedBuilder(size, this);
    }

    public final FloatVector newFloatArrayVector(float[] values, int positionCount) {
        return newFloatArrayVector(values, positionCount, 0L);
    }

    public FloatVector newFloatArrayVector(float[] values, int positionCount, long preAdjustedBytes) {
        var b = new FloatArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public final FloatBlock newConstantFloatBlockWith(float value, int positions) {
        return newConstantFloatBlockWith(value, positions, 0L);
    }

    public FloatBlock newConstantFloatBlockWith(float value, int positions, long preAdjustedBytes) {
        var b = new ConstantFloatVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public FloatVector newConstantFloatVector(float value, int positions) {
        adjustBreaker(ConstantFloatVector.RAM_BYTES_USED);
        var v = new ConstantFloatVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantFloatVector.RAM_BYTES_USED;
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public final LongBlock newConstantLongBlockWith(long value, int positions) {
        return newConstantLongBlockWith(value, positions, 0L);
    }

    public LongBlock newConstantLongBlockWith(long value, int positions, long preAdjustedBytes) {
        var b = new ConstantLongVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public LongVector newConstantLongVector(long value, int positions) {
        adjustBreaker(ConstantLongVector.RAM_BYTES_USED);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
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
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public final DoubleBlock newConstantDoubleBlockWith(double value, int positions) {
        return newConstantDoubleBlockWith(value, positions, 0L);
    }

    public DoubleBlock newConstantDoubleBlockWith(double value, int positions, long preAdjustedBytes) {
        var b = new ConstantDoubleVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed() - preAdjustedBytes);
        return b;
    }

    public DoubleVector newConstantDoubleVector(double value, int positions) {
        adjustBreaker(ConstantDoubleVector.RAM_BYTES_USED);
        var v = new ConstantDoubleVector(value, positions, this);
        assert v.ramBytesUsed() == ConstantDoubleVector.RAM_BYTES_USED;
        return v;
    }

    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize) {
        return new BytesRefBlockBuilder(estimatedSize, bigArrays, this);
    }

    public BytesRefBlock newBytesRefArrayBlock(BytesRefArray values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        var b = new BytesRefArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering, this);
        adjustBreaker(b.ramBytesUsed() - values.bigArraysRamBytesUsed());
        return b;
    }

    public BytesRefVector.Builder newBytesRefVectorBuilder(int estimatedSize) {
        return new BytesRefVectorBuilder(estimatedSize, bigArrays, this);
    }

    public BytesRefVector newBytesRefArrayVector(BytesRefArray values, int positionCount) {
        var b = new BytesRefArrayVector(values, positionCount, this);
        adjustBreaker(b.ramBytesUsed() - values.bigArraysRamBytesUsed());
        return b;
    }

    public BytesRefBlock newConstantBytesRefBlockWith(BytesRef value, int positions) {
        var b = new ConstantBytesRefVector(value, positions, this).asBlock();
        adjustBreaker(b.ramBytesUsed());
        return b;
    }

    public BytesRefVector newConstantBytesRefVector(BytesRef value, int positions) {
        long preadjusted = ConstantBytesRefVector.ramBytesUsed(value);
        adjustBreaker(preadjusted);
        var v = new ConstantBytesRefVector(value, positions, this);
        assert v.ramBytesUsed() == preadjusted;
        return v;
    }

    public Block newConstantNullBlock(int positions) {
        var b = new ConstantNullBlock(positions, this);
        adjustBreaker(b.ramBytesUsed());
        return b;
    }

    public AggregateMetricDoubleBlockBuilder newAggregateMetricDoubleBlockBuilder(int estimatedSize) {
        return new AggregateMetricDoubleBlockBuilder(estimatedSize, this);
    }

    public final Block newConstantAggregateMetricDoubleBlock(
        AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral value,
        int positions
    ) {
        try (AggregateMetricDoubleBlockBuilder builder = newAggregateMetricDoubleBlockBuilder(positions)) {
            if (value.min() != null) {
                builder.min().appendDouble(value.min());
            } else {
                builder.min().appendNull();
            }
            if (value.max() != null) {
                builder.max().appendDouble(value.max());
            } else {
                builder.max().appendNull();
            }
            if (value.sum() != null) {
                builder.sum().appendDouble(value.sum());
            } else {
                builder.sum().appendNull();
            }
            if (value.count() != null) {
                builder.count().appendInt(value.count());
            } else {
                builder.count().appendNull();
            }
            return builder.build();
        }
    }

    /**
     * Returns the maximum number of bytes that a Block should be backed by a primitive array before switching to using BigArrays.
     */
    public long maxPrimitiveArrayBytes() {
        return maxPrimitiveArrayBytes;
    }
}
