/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Abstract base class for composite block types that are implemented by delegating to several concrete sub-blocks.
 * Supports multi-valued fields via {@code firstValueIndexes}.
 * <p>
 * The key insight is that value indices in the composite block correspond to positions in the sub-blocks.
 * Sub-blocks are always single-valued per position. For example, a multi-valued exponential histogram block
 * with three histograms at one position would have three corresponding positions in each sub-block.
 * </p>
 * <p>
 * Null handling is delegated to the first sub-block returned by {@link #getSubBlocks()} which must be non-null
 * for non-null composite values.
 * </p>
 */
public abstract class AbstractDelegatingCompoundBlock<T extends Block> extends AbstractNonThreadSafeRefCounted implements Block {

    /**
     * Transport version when multi-value support was introduced for {@link TDigestBlock} and {@link ExponentialHistogramBlock}.
     */
    static final TransportVersion MULTIVALUE_SUPPORT = TransportVersion.fromName("histogram_blocks_multivalue_support");

    private final int positionCount;
    @Nullable
    private final int[] firstValueIndexes;

    /**
     * @param positionCount the number of positions in this block
     * @param firstValueIndexes maps positions to ranges of value indices in sub-blocks. Null if single-valued.
     */
    protected AbstractDelegatingCompoundBlock(int positionCount, @Nullable int[] firstValueIndexes) {
        this.positionCount = positionCount;
        this.firstValueIndexes = firstValueIndexes;
    }

    /**
     * @return a list of the sub-blocks composing this compound block. The first block must be non-null for
     *         non-null composite values (used for null detection). The order should match the order expected
     *         by {@link #buildFromSubBlocks(List, int, int[])}.
     */
    protected abstract List<Block> getSubBlocks();

    /**
     * Construct a new instance of the block, based on the given list of sub-blocks.
     * @param subBlocks List of sub-blocks, in the same order as {@link #getSubBlocks()}
     * @param positionCount the number of positions in the new block
     * @param firstValueIndexes the firstValueIndexes array for the new block (may be null for single-valued)
     * @return a new instance based on the given blocks.
     */
    protected abstract T buildFromSubBlocks(List<Block> subBlocks, int positionCount, @Nullable int[] firstValueIndexes);

    @Override
    public final int getPositionCount() {
        return positionCount;
    }

    @Override
    public final int getFirstValueIndex(int position) {
        return firstValueIndexes == null ? position : firstValueIndexes[position];
    }

    @Override
    public final int getValueCount(int position) {
        if (isNull(position)) {
            return 0;
        }
        if (firstValueIndexes == null) {
            return 1;
        }
        return firstValueIndexes[position + 1] - firstValueIndexes[position];
    }

    @Override
    public final int getTotalValueCount() {
        int subBlockCount = subBlockPositionCount();
        int nullCount = 0;
        for (int p = 0; p < positionCount; p++) {
            if (isNull(p)) {
                nullCount++;
            }
        }
        return subBlockCount - nullCount;
    }

    @Override
    public final boolean isNull(int position) {
        return getSubBlocks().getFirst().isNull(getFirstValueIndex(position));
    }

    @Override
    public final boolean mayHaveNulls() {
        return getSubBlocks().getFirst().mayHaveNulls();
    }

    @Override
    public final boolean areAllValuesNull() {
        return getSubBlocks().getFirst().areAllValuesNull();
    }

    @Override
    public final boolean mayHaveMultivaluedFields() {
        return firstValueIndexes != null;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        if (false == mayHaveMultivaluedFields()) {
            return false;
        }
        for (int p = 0; p < getPositionCount(); p++) {
            if (getValueCount(p) > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    /**
     * Returns the number of positions in the sub-blocks (the total number of individual values including nulls).
     */
    final int subBlockPositionCount() {
        return firstValueIndexes == null ? positionCount : firstValueIndexes[positionCount];
    }

    /**
     * Compares positionCount and firstValueIndexes with another block for equality.
     */
    protected final boolean layoutEquals(AbstractDelegatingCompoundBlock<?> that) {
        if (positionCount != that.positionCount) {
            return false;
        }
        if (firstValueIndexes == null) {
            return that.firstValueIndexes == null;
        }
        if (that.firstValueIndexes == null) {
            return false;
        }
        return Arrays.equals(firstValueIndexes, 0, positionCount + 1, that.firstValueIndexes, 0, positionCount + 1);
    }

    protected boolean assertInvariants() {
        if (firstValueIndexes != null) {
            assert firstValueIndexes.length >= getPositionCount() + 1 : firstValueIndexes.length + " < " + positionCount;
            for (int i = 0; i < getPositionCount(); i++) {
                assert firstValueIndexes[i + 1] >= firstValueIndexes[i] : firstValueIndexes[i + 1] + " < " + firstValueIndexes[i];
            }
        }
        return true;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        getSubBlocks().forEach(Block::allowPassingToDifferentDriver);
    }

    @Override
    public BlockFactory blockFactory() {
        return getSubBlocks().getFirst().blockFactory();
    }

    @Override
    public int valueMaxByteSize() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not have a single value byte size");
    }

    @Override
    public long ramBytesUsed() {
        long bytes = firstValueIndexes != null ? RamUsageEstimator.sizeOf(firstValueIndexes) : 0;
        for (Block b : getSubBlocks()) {
            bytes += b.ramBytesUsed();
        }
        return bytes;
    }

    @Override
    protected void closeInternal() {
        if (firstValueIndexes != null) {
            blockFactory().preAdjustBreakerForInt(-firstValueIndexes.length);
        }
        Releasables.close(getSubBlocks());
    }

    @Override
    public T filter(boolean mayContainDuplicates, int... positions) {
        if (firstValueIndexes == null) {
            return applyToSubBlocks(b -> b.filter(mayContainDuplicates, positions), -1, null);
        }
        throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
    }

    @Override
    public T slice(int beginInclusive, int endExclusive) {
        int newPositionCount = endExclusive - beginInclusive;
        if (firstValueIndexes == null) {
            return applyToSubBlocks(b -> b.slice(beginInclusive, endExclusive), -1, null);
        }
        throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
    }

    @Override
    public T keepMask(BooleanVector mask) {
        if (firstValueIndexes == null) {
            return applyToSubBlocks(b -> b.keepMask(mask), -1, null);
        }
        throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
    }

    @Override
    public T deepCopy(BlockFactory blockFactory) {
        if (firstValueIndexes == null) {
            return applyToSubBlocks(b -> b.deepCopy(blockFactory), -1, null);
        }
        long newFviBytes = blockFactory.preAdjustBreakerForInt(firstValueIndexes.length);
        boolean success = false;
        try {
            int[] copiedFirstValueIndices = firstValueIndexes.clone();
            T result = applyToSubBlocks(b -> b.deepCopy(blockFactory), positionCount, copiedFirstValueIndices);
            success = true;
            return result;
        } finally {
            if (success == false) {
                blockFactory.adjustBreaker(-newFviBytes);
            }
        }
    }

    private T applyToSubBlocks(
        java.util.function.Function<Block, Block> operation,
        int newPositionCount,
        @Nullable int[] newFirstValueIndexes
    ) {
        List<Block> newSubBlocks = new ArrayList<>(getSubBlocks().size());
        boolean success = false;
        try {
            for (Block b : getSubBlocks()) {
                newSubBlocks.add(operation.apply(b));
            }
            assert newFirstValueIndexes == null || newPositionCount >= 0
                : "If a multi-valued block is provided it should have a position count";
            int actualPositionCount = newFirstValueIndexes != null ? newPositionCount : newSubBlocks.getFirst().getPositionCount();
            T result = buildFromSubBlocks(newSubBlocks, actualPositionCount, newFirstValueIndexes);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(newSubBlocks);
            }
        }
    }

    /**
     * Writes the multivalue metadata (positionCount, firstValueIndexes) to a stream.
     */
    protected void writeMultiValueMetadata(StreamOutput out) throws IOException {
        out.writeVInt(positionCount);
        out.writeBoolean(firstValueIndexes != null);
        if (firstValueIndexes != null) {
            out.writeVInt(firstValueIndexes[0]);
            for (int i = 1; i <= positionCount; i++) {
                out.writeVInt(firstValueIndexes[i] - firstValueIndexes[i - 1]);
            }
        }
    }

    protected interface Deserializer<T extends Block> {
        T deserialize(BlockStreamInput in, int positionCount, @Nullable int[] firstValueIndexes) throws IOException;
    }

    protected static <T extends Block> T readFrom(BlockStreamInput in, Deserializer<T> deserializer) throws IOException {
        int positionCount = in.readVInt();
        long bytesReserved = 0;
        boolean success = false;
        try {
            int[] firstValueIndexes = null;
            if (in.readBoolean()) {
                bytesReserved += in.blockFactory().preAdjustBreakerForInt(positionCount + 1);
                firstValueIndexes = new int[positionCount + 1];
                firstValueIndexes[0] = in.readVInt();
                for (int i = 1; i <= positionCount; i++) {
                    firstValueIndexes[i] = firstValueIndexes[i - 1] + in.readVInt();
                }
            }
            T result = deserializer.deserialize(in, positionCount, firstValueIndexes);
            success = true;
            return result;
        } finally {
            if (success == false) {
                in.blockFactory().adjustBreaker(-bytesReserved);
            }
        }
    }

    /**
     * Manages firstValueIndexes and positionCount for multi-value support.
     */
    protected abstract static class AbstractCompositeBlockBuilder<T extends Block> implements Block.Builder {
        private int positionCount;
        private int[] firstValueIndexes;
        private int openPositionEntryStartValue = -1;

        /**
         * The number of values added, including null entries.
         */
        private int valueCount;

        protected final BlockFactory blockFactory;
        private boolean closed = false;
        private boolean built = false;

        protected AbstractCompositeBlockBuilder(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        public final Block.Builder beginPositionEntry() {
            assert closed == false;
            assert built == false;
            if (isPositionEntryOpen()) {
                endPositionEntry();
            }
            openPositionEntryStartValue = valueCount;
            return this;
        }

        private void setFirstValue(int position, int value) {
            assert closed == false;
            assert built == false;
            ensureFirstValueIndexCapacity(position + 1);
            firstValueIndexes[position] = value;
        }

        private void ensureFirstValueIndexCapacity(int minCapacity) {
            if (firstValueIndexes == null || firstValueIndexes.length < minCapacity) {
                int newLength = ArrayUtil.oversize(minCapacity, Integer.BYTES);
                blockFactory.preAdjustBreakerForInt(newLength);

                if (firstValueIndexes != null) {
                    int oldLength = firstValueIndexes.length;
                    firstValueIndexes = ArrayUtil.growExact(firstValueIndexes, newLength);
                    blockFactory.preAdjustBreakerForInt(-oldLength);
                } else {
                    firstValueIndexes = new int[newLength];
                    IntStream.range(0, positionCount).forEach(i -> firstValueIndexes[i] = i);
                }
            }
        }

        protected boolean isPositionEntryOpen() {
            return openPositionEntryStartValue != -1;
        }

        /**
         * Must be called after a new value (null or non-null) has been appended.
         */
        protected final void valueAppended() {
            assert closed == false;
            assert built == false;
            valueCount++;
            if (isPositionEntryOpen() == false) {
                if (firstValueIndexes != null) {
                    setFirstValue(positionCount, valueCount - 1);
                }
                positionCount++;
            }
        }

        @Override
        public final Block.Builder endPositionEntry() {
            assert closed == false;
            assert built == false;
            assert isPositionEntryOpen() : "must call beginPositionEntry() first";
            int numValues = valueCount - openPositionEntryStartValue;
            assert numValues > 0 : "use appendNull to build an empty position";

            if (numValues > 1 || firstValueIndexes != null) {
                setFirstValue(positionCount, valueCount - numValues);
            }
            openPositionEntryStartValue = -1;
            positionCount++;
            return this;
        }

        @Override
        public final T build() {
            if (closed || built) {
                throw new IllegalStateException("already closed");
            }
            if (isPositionEntryOpen()) {
                endPositionEntry();
            }
            if (firstValueIndexes != null) {
                // Add the terminating first value index
                setFirstValue(positionCount, valueCount);
            }
            T result = doBuild(positionCount, firstValueIndexes);
            // the block now owns the memory and takes care of releasing it from the CB
            firstValueIndexes = null;
            built = true;
            return result;
        }

        protected abstract T doBuild(int positionCount, @Nullable int[] firstValueIndexes);

        @Override
        public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
            assert closed == false;
            assert built == false;
            assert isPositionEntryOpen() == false;
            if (block.areAllValuesNull()) {
                for (int i = beginInclusive; i < endExclusive; i++) {
                    appendNull();
                }
            } else {
                AbstractDelegatingCompoundBlock<?> from = (AbstractDelegatingCompoundBlock<?>) block;
                int numCopiedPositions = endExclusive - beginInclusive;
                int startSubBlockPos = from.firstValueIndexes == null ? beginInclusive : from.firstValueIndexes[beginInclusive];
                int endSubBlockPos = from.firstValueIndexes == null ? endExclusive : from.firstValueIndexes[endExclusive];
                // Remember that both null values and non-multivalued, non-null entries take up exactly one position in the subblocks
                int numCopiedSubBlockPositions = endSubBlockPos - startSubBlockPos;
                boolean hasNoMultiValues = numCopiedSubBlockPositions == numCopiedPositions;
                if (hasNoMultiValues) {
                    if (firstValueIndexes != null) {
                        ensureFirstValueIndexCapacity(positionCount + numCopiedPositions + 1);
                        for (int i = 0; i < numCopiedPositions; i++) {
                            firstValueIndexes[positionCount + i] = valueCount + i;
                        }
                    }
                } else {
                    assert from.firstValueIndexes != null : "Expected to be non-null for multi-valued block";
                    ensureFirstValueIndexCapacity(positionCount + numCopiedPositions + 1);
                    int offset = valueCount - from.firstValueIndexes[beginInclusive];
                    for (int i = 0; i < numCopiedPositions; i++) {
                        firstValueIndexes[positionCount + i] = from.firstValueIndexes[beginInclusive + i] + offset;
                    }
                }
                copySubBlockPositions(from, startSubBlockPos, endSubBlockPos);
                this.valueCount += numCopiedSubBlockPositions;
                this.positionCount += numCopiedPositions;
            }
            return this;
        }

        /**
         * Copy the range of sub-block positions from the provided block into this builder.
         */
        protected abstract void copySubBlockPositions(AbstractDelegatingCompoundBlock<?> block, int startSubBlockPos, int endSubBlockPos);

        @Override
        public long estimatedBytes() {
            return firstValueIndexes != null ? RamUsageEstimator.sizeOf(firstValueIndexes) : 0;
        }

        @Override
        public final void close() {
            if (closed == false) {
                closed = true;
                if (firstValueIndexes != null) {
                    blockFactory.preAdjustBreakerForInt(-firstValueIndexes.length);
                }
                extraClose();
            }
        }

        /**
         * Called when {@link #close()} was invoked.
         */
        protected void extraClose() {}
    }
}
