/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.AbstractBlockBuilder;
import org.elasticsearch.compute.data.AbstractDelegatingCompoundBlock;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlock;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Block.MvOrdering;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.data.ExponentialHistogramBlockBuilder;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.data.arrow.CircuitBreakerAllocationListener;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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

    private record IdentityKey(Object object) {
        @Override
        public boolean equals(Object o) {
            return o instanceof IdentityKey that && that.object == object;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(object);
        }
    }

    final ConcurrentMap<IdentityKey, Object> TRACKED_BLOCKS = new ConcurrentHashMap<>();

    private static final long PURGE_RELEASED_BYTES = 1024 * 1024;
    private volatile int nextPurgeSize = 128;
    private final AtomicLong releasedBytes = new AtomicLong();

    public void ensureAllBlocksAreReleased() {

        if (arrowAllocator != null) {
            arrowAllocator.close();
        }

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

    public MockBlockFactory(BlockFactoryBuilder builder) {
        this(builder, null);
    }

    protected MockBlockFactory(BlockFactoryBuilder builder, BlockFactory parent) {
        super(builder, parent);
    }

    @Override
    protected BufferAllocator childFactoryAllocator() {
        var listener = new CircuitBreakerAllocationListener(this.breaker());
        return super.arrowAllocator().newChildAllocator("mock-factory", listener, 0, Long.MAX_VALUE);
    }

    @Override
    public BlockFactory newChildFactory(LocalCircuitBreaker childBreaker) {
        if (childBreaker.parentBreaker() != breaker()) {
            throw new IllegalStateException("Different parent breaker");
        }
        return new MockBlockFactory(builder(bigArrays()).breaker(childBreaker).maxPrimitiveArraySize(maxPrimitiveArrayBytes()), this);
    }

    @Override
    public void adjustBreaker(final long delta) {
        if (delta < 0 && releasedBytes.addAndGet(-delta) >= PURGE_RELEASED_BYTES) {
            purgeTrackBlocks();
        }
        super.adjustBreaker(delta);
    }

    static boolean isKeyReleased(IdentityKey key) {
        Object b = key.object;
        return switch (b) {
            case Block block -> block.isReleased();
            case Vector vector -> vector.isReleased();
            case AbstractBlockBuilder builder -> builder.isReleased();
            case Vector.Builder builder -> builder.isReleased();
            case AbstractDelegatingCompoundBlock.AbstractCompositeBlockBuilder<?> builder -> builder.isReleased();
            default -> throw new RuntimeException("Unexpected: " + b);
        };
    }

    void purgeTrackBlocks() {
        releasedBytes.set(0);
        for (var entries : TRACKED_BLOCKS.entrySet()) {
            var key = entries.getKey();
            if (isKeyReleased(key)) {
                TRACKED_BLOCKS.remove(key);
            }
        }
        nextPurgeSize = TRACKED_BLOCKS.size() + Math.clamp(TRACKED_BLOCKS.size(), 128, 1024);
    }

    void track(Object obj, Object trackDetail) {
        if (TRACKED_BLOCKS.size() >= nextPurgeSize) {
            purgeTrackBlocks();
        }
        TRACKED_BLOCKS.put(new IdentityKey(obj), trackDetail);
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
    public BooleanVector newConstantBooleanVector(boolean value, int positions, long preAdjustedBytes) {
        var v = super.newConstantBooleanVector(value, positions, preAdjustedBytes);
        track(v, trackDetail());
        return v;
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
    public IntVector newConstantIntVector(int value, int positions, long preAdjustedBytes) {
        var v = super.newConstantIntVector(value, positions, preAdjustedBytes);
        track(v, trackDetail());
        return v;
    }

    @Override
    public IntVector newIntRangeVector(int startInclusive, int endExclusive) {
        var v = super.newIntRangeVector(startInclusive, endExclusive);
        track(v, trackDetail());
        return v;
    }

    @Override
    public FloatBlock.Builder newFloatBlockBuilder(int estimatedSize) {
        var b = super.newFloatBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatBlock newFloatArrayBlock(float[] values, int pc, int[] fvi, BitSet nulls, MvOrdering mvOrdering, long preAdjustedBytes) {
        var b = super.newFloatArrayBlock(values, pc, fvi, nulls, mvOrdering, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatVector.Builder newFloatVectorBuilder(int estimatedSize) {
        var b = super.newFloatVectorBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatVector.FixedBuilder newFloatVectorFixedBuilder(int size) {
        var b = super.newFloatVectorFixedBuilder(size);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatVector newFloatArrayVector(float[] values, int positionCount, long preAdjustedBytes) {
        var b = super.newFloatArrayVector(values, positionCount, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatBlock newConstantFloatBlockWith(float value, int positions, long preAdjustedBytes) {
        var b = super.newConstantFloatBlockWith(value, positions, preAdjustedBytes);
        track(b, trackDetail());
        return b;
    }

    @Override
    public FloatVector newConstantFloatVector(float value, int positions, long preAdjustedBytes) {
        var v = super.newConstantFloatVector(value, positions, preAdjustedBytes);
        track(v, trackDetail());
        return v;
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
    public LongVector newConstantLongVector(long value, int positions, long preAdjustedBytes) {
        var v = super.newConstantLongVector(value, positions, preAdjustedBytes);
        track(v, trackDetail());
        return v;
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
    public DoubleVector newConstantDoubleVector(double value, int positions, long preAdjustedBytes) {
        var v = super.newConstantDoubleVector(value, positions, preAdjustedBytes);
        track(v, trackDetail());
        return v;
    }

    @Override
    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize) {
        var b = super.newBytesRefBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefBlock.Builder newBytesRefBlockBuilder(int estimatedSize, long byteHint) {
        var b = super.newBytesRefBlockBuilder(estimatedSize, byteHint);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BytesRefVector newConstantBytesRefVector(BytesRef value, int positions) {
        var v = super.newConstantBytesRefVector(value, positions);
        track(v, trackDetail());
        return v;
    }

    @Override
    public BytesRefVector newDirectBytesRefVector(byte[] bytes, int[] startOffsets, int positionCount) {
        var v = super.newDirectBytesRefVector(bytes, startOffsets, positionCount);
        track(v, trackDetail());
        return v;
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

    @Override
    public AggregateMetricDoubleBlockBuilder newAggregateMetricDoubleBlockBuilder(int estimatedSize) {
        var b = super.newAggregateMetricDoubleBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public AggregateMetricDoubleBlock newAggregateMetricDoubleBlock(Block minBlock, Block maxBlock, Block sumBlock, Block countBlock) {
        var b = super.newAggregateMetricDoubleBlock(minBlock, maxBlock, sumBlock, countBlock);
        track(b, trackDetail());
        return b;
    }

    @Override
    public AggregateMetricDoubleBlock newAggregateMetricDoubleBlock(
        double[] minValues,
        double[] maxValues,
        double[] sumValues,
        int[] countValues,
        int positions
    ) {
        var b = super.newAggregateMetricDoubleBlock(minValues, maxValues, sumValues, countValues, positions);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BlockLoader.Block newAggregateMetricDoubleBlockFromDocValues(
        DoubleBlock minBlock,
        DoubleBlock maxBlock,
        DoubleBlock sumBlock,
        IntBlock countBlock
    ) {
        var b = super.newAggregateMetricDoubleBlockFromDocValues(minBlock, maxBlock, sumBlock, countBlock);
        track(b, trackDetail());
        return b;
    }

    @Override
    public ExponentialHistogramBlockBuilder newExponentialHistogramBlockBuilder(int estimatedSize) {
        var b = super.newExponentialHistogramBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public ExponentialHistogramBlock newConstantExponentialHistogramBlock(ExponentialHistogram value, int positionCount) {
        var b = super.newConstantExponentialHistogramBlock(value, positionCount);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BlockLoader.Block newExponentialHistogramBlockFromDocValues(
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        DoubleBlock valueCounts,
        DoubleBlock zeroThresholds,
        BytesRefBlock encodedHistograms
    ) {
        var b = super.newExponentialHistogramBlockFromDocValues(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
        track(b, trackDetail());
        return b;
    }

    @Override
    public TDigestBlockBuilder newTDigestBlockBuilder(int estimatedSize) {
        var b = super.newTDigestBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public TDigestBlock newConstantTDigestBlock(TDigestHolder value, int positions) {
        var b = super.newConstantTDigestBlock(value, positions);
        track(b, trackDetail());
        return b;
    }

    @Override
    public TDigestBlock newConstantTDigestBlockWith(TDigestHolder value, int positions) {
        var b = super.newConstantTDigestBlockWith(value, positions);
        track(b, trackDetail());
        return b;
    }

    @Override
    public BlockLoader.Block newTDigestBlockFromDocValues(
        BytesRefBlock encodedDigests,
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        LongBlock counts
    ) {
        var b = super.newTDigestBlockFromDocValues(encodedDigests, minima, maxima, sums, counts);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongRangeBlockBuilder newLongRangeBlockBuilder(int estimatedSize) {
        var b = super.newLongRangeBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public LongRangeBlock newConstantLongRangeBlockWith(LongRangeBlockBuilder.LongRange value, int positions) {
        var b = super.newConstantLongRangeBlockWith(value, positions);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleRangeBlockBuilder newDoubleRangeBlockBuilder(int estimatedSize) {
        var b = super.newDoubleRangeBlockBuilder(estimatedSize);
        track(b, trackDetail());
        return b;
    }

    @Override
    public DoubleRangeBlock newConstantDoubleRangeBlockWith(DoubleRangeBlockBuilder.DoubleRange value, int positions) {
        var b = super.newConstantDoubleRangeBlockWith(value, positions);
        track(b, trackDetail());
        return b;
    }
}
