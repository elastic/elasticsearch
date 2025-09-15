/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.AbstractBlockBuilder;
import org.elasticsearch.compute.data.AbstractVectorBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Block.MvOrdering;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BooleanVectorFixedBuilder;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.DoubleVectorFixedBuilder;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.IntVectorFixedBuilder;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.LongVectorFixedBuilder;
import org.elasticsearch.compute.data.Vector;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A block factory that tracks the creation of blocks, vectors, builders that it creates. The
 * factory allows to ensure that the blocks are released, as well as inspect the source of the
 * creation.
 */
public class MockBlockFactory extends BlockFactory {

    static final boolean TRACK_ALLOCATIONS = true;

    static Object trackDetail() {
        return TRACK_ALLOCATIONS
            ? new RuntimeException("Releasable allocated from test: " + LuceneTestCase.getTestClass().getName())
            : true;
    }

    final ConcurrentMap<Object, Object> TRACKED_BLOCKS = new ConcurrentHashMap<>();

    public void ensureAllBlocksAreReleased() {
        purgeTrackBlocks();
        final Map<Object, Object> copy = new HashMap<>(TRACKED_BLOCKS);
        // we should really assert this, but not just yet, see comment below
        // assert breaker().getUsed() > 0 : "Expected some used in breaker if tracked blocks is not empty";
        if (breaker().getUsed() == 0) {
            TRACKED_BLOCKS.clear();
            return; // this is a hack until we get better a mock tracking.
        }
        if (copy.isEmpty() == false) {
            Iterator<Object> causes = copy.values().iterator();
            Object firstCause = causes.next();
            RuntimeException exception = new RuntimeException(
                copy.size() + " releasables have not been released",
                firstCause instanceof Throwable ? (Throwable) firstCause : null
            );
            while (causes.hasNext()) {
                Object cause = causes.next();
                if (cause instanceof Throwable) {
                    exception.addSuppressed((Throwable) cause);
                }
            }
            throw exception;
        }
    }

    public MockBlockFactory(CircuitBreaker breaker, BigArrays bigArrays) {
        this(breaker, bigArrays, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE);
    }

    public MockBlockFactory(CircuitBreaker breaker, BigArrays bigArrays, ByteSizeValue maxPrimitiveArraySize) {
        this(breaker, bigArrays, maxPrimitiveArraySize, null);
    }

    private MockBlockFactory(CircuitBreaker breaker, BigArrays bigArrays, ByteSizeValue maxPrimitiveArraySize, BlockFactory parent) {
        super(breaker, bigArrays, maxPrimitiveArraySize, parent);
    }

    @Override
    public BlockFactory newChildFactory(LocalCircuitBreaker childBreaker) {
        if (childBreaker.parentBreaker() != breaker()) {
            throw new IllegalStateException("Different parent breaker");
        }
        return new MockBlockFactory(childBreaker, bigArrays(), ByteSizeValue.ofBytes(maxPrimitiveArrayBytes()), this);
    }

    @Override
    public void adjustBreaker(final long delta) {
        purgeTrackBlocks();
        super.adjustBreaker(delta);
    }

    void purgeTrackBlocks() {
        for (var entries : TRACKED_BLOCKS.entrySet()) {
            var b = entries.getKey();
            if (b instanceof Block block) {
                if (block.isReleased()) {
                    TRACKED_BLOCKS.remove(block);
                }
            } else if (b instanceof AbstractBlockBuilder blockBuilder) {
                if (blockBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(blockBuilder);
                }
            } else if (b instanceof IntVectorFixedBuilder vecBuilder) {
                if (vecBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(vecBuilder);
                }
            } else if (b instanceof LongVectorFixedBuilder vecBuilder) {
                if (vecBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(vecBuilder);
                }
            } else if (b instanceof DoubleVectorFixedBuilder vecBuilder) {
                if (vecBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(vecBuilder);
                }
            } else if (b instanceof BooleanVectorFixedBuilder vecBuilder) {
                if (vecBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(vecBuilder);
                }
            } else if (b instanceof AbstractVectorBuilder vecBuilder) {
                if (vecBuilder.isReleased()) {
                    TRACKED_BLOCKS.remove(vecBuilder);
                }
            } else if (b instanceof Vector vector) {
                if (vector.isReleased()) {
                    TRACKED_BLOCKS.remove(vector);
                }
            } else {
                throw new RuntimeException("Unexpected: " + b);
            }
        }
    }

    void track(Object obj, Object trackDetail) {
        purgeTrackBlocks();
        TRACKED_BLOCKS.put(obj, trackDetail);
    }

    @Override
    public long preAdjustBreakerForBoolean(int positionCount) {
        // TODO: track these preadjusters?
        return super.preAdjustBreakerForBoolean(positionCount);
    }

    @Override
    public long preAdjustBreakerForInt(int positionCount) {
        return super.preAdjustBreakerForInt(positionCount);
    }

    @Override
    public long preAdjustBreakerForLong(int positionCount) {
        return super.preAdjustBreakerForLong(positionCount);
    }

    @Override
    public long preAdjustBreakerForDouble(int positionCount) {
        return super.preAdjustBreakerForDouble(positionCount);
    }

    @Override
    public BooleanBlock.Builder newBooleanBlockBuilder(int estimatedSize) {
        var b = super.newBooleanBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BooleanVector.FixedBuilder newBooleanVectorFixedBuilder(int size) {
        var b = super.newBooleanVectorFixedBuilder(size);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BooleanBlock newBooleanArrayBlock(boolean[] values, int pc, int[] fvi, BitSet nulls, MvOrdering order, long preAdjustedBytes) {
        var b = super.newBooleanArrayBlock(values, pc, fvi, nulls, order, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BooleanVector.Builder newBooleanVectorBuilder(int estimatedSize) {
        var b = super.newBooleanVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BooleanVector newBooleanArrayVector(boolean[] values, int positionCount, long preAdjustedBytes) {
        var b = super.newBooleanArrayVector(values, positionCount, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BooleanBlock newConstantBooleanBlockWith(boolean value, int positions, long preAdjustedBytes) {
        var b = super.newConstantBooleanBlockWith(value, positions, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntBlock.Builder newIntBlockBuilder(int estimatedSize) {
        // var b = new MockIntBlockBuilder(estimatedSize, super);
        var b = super.newIntBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntBlock newIntArrayBlock(int[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = super.newIntArrayBlock(values, pc, fvi, nulls, mvOrdering, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntVector.Builder newIntVectorBuilder(int estimatedSize) {
        var b = super.newIntVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntVector.FixedBuilder newIntVectorFixedBuilder(int size) {
        var b = super.newIntVectorFixedBuilder(size);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntVector newIntArrayVector(int[] values, int positionCount, long preAdjustedBytes) {
        var b = super.newIntArrayVector(values, positionCount, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public IntBlock newConstantIntBlockWith(int value, int positions, long preAdjustedBytes) {
        var b = super.newConstantIntBlockWith(value, positions, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
        var b = super.newLongBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongBlock newLongArrayBlock(long[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = super.newLongArrayBlock(values, pc, fvi, nulls, mvOrdering, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongVector.Builder newLongVectorBuilder(int estimatedSize) {
        var b = super.newLongVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongVector.FixedBuilder newLongVectorFixedBuilder(int size) {
        var b = super.newLongVectorFixedBuilder(size);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongVector newLongArrayVector(long[] values, int positionCount, long preAdjustedBytes) {
        var b = super.newLongArrayVector(values, positionCount, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongBlock newConstantLongBlockWith(long value, int positions, long preAdjustedBytes) {
        var b = super.newConstantLongBlockWith(value, positions, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleBlock.Builder newDoubleBlockBuilder(int estimatedSize) {
        var b = super.newDoubleBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleBlock newDoubleArrayBlock(double[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = super.newDoubleArrayBlock(values, pc, fvi, nulls, mvOrdering, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleVector.Builder newDoubleVectorBuilder(int estimatedSize) {
        var b = super.newDoubleVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleVector.FixedBuilder newDoubleVectorFixedBuilder(int size) {
        var b = super.newDoubleVectorFixedBuilder(size);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleVector newDoubleArrayVector(double[] values, int positionCount, long preAdjustedBytes) {
        var b = super.newDoubleArrayVector(values, positionCount, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleBlock newConstantDoubleBlockWith(double value, int positions, long preAdjustedBytes) {
        var b = super.newConstantDoubleBlockWith(value, positions, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize) {
        var b = super.newBytesRefBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefBlock newBytesRefArrayBlock(BytesRefArray values, int pc, int[] firstValueIndexes, BitSet nulls, MvOrdering mvOrdering) {
        var b = super.newBytesRefArrayBlock(values, pc, firstValueIndexes, nulls, mvOrdering);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefVector.Builder newBytesRefVectorBuilder(int estimatedSize) {
        var b = super.newBytesRefVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefVector newBytesRefArrayVector(BytesRefArray values, int positionCount) {
        var b = super.newBytesRefArrayVector(values, positionCount);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefBlock newConstantBytesRefBlockWith(BytesRef value, int positions) {
        var b = super.newConstantBytesRefBlockWith(value, positions);
        track(b, trackDetail());
        return b;
    }

    @Override
    public Block newConstantNullBlock(int positions) {
        var b = super.newConstantNullBlock(positions);
        track(b, trackDetail());
        return b;
    }
}
