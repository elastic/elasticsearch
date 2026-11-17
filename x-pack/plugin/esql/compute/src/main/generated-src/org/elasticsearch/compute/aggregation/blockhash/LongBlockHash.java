/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.compute.aggregation.GroupRuns;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeLong;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.swisshash.LongSwissHash;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import java.util.BitSet;
// end generated imports

/**
 * Maps a {@link LongBlock} column to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class LongBlockHash extends PartitionedBlockHash {
    private final int channel;
    final LongHashTable hash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    /**
     * When {@code true}, the input to {@link #add(Page, GroupingAggregatorFunction.AddInput)} is known to arrive from Lucene
     * ordered by this field (it is the shard's primary {@code index.sort.field}), so equal values are adjacent. Pages without
     * nulls or multivalues are then split into {@link GroupRuns runs} of equal values that are hashed once each.
     */
    private final boolean primarySorted;

    /**
     * Scratch for the runs of the page being added, reused for every page. Only created for a {@link #primarySorted} hash.
     */
    private GroupRuns runs;

    /**
     * How many pages took the runs path, so a profile can confirm that the sorted path was really used.
     */
    private long sortedPages;

    private static final int PREFETCH_BATCH = 128;
    private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
    private final int[] batchHashes = new int[PREFETCH_BATCH];

    LongBlockHash(int channel, BlockFactory blockFactory, boolean primarySorted) {
        super(blockFactory);
        this.channel = channel;
        this.hash = HashImplFactory.newLongHash(blockFactory);
        this.primarySorted = primarySorted;
    }

    LongBlockHash(int channel, BlockFactory blockFactory) {
        this(channel, blockFactory, false);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        // TODO track raw counts and which implementation we pick for the profiler - #114008
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            seenNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
            return;
        }
        LongBlock castBlock = (LongBlock) block;
        LongVector vector = castBlock.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(castBlock)) {
                addInput.add(0, groupIds);
            }
            return;
        }
        if (primarySorted && vector.getPositionCount() > 0) {
            addRuns(vector, addInput);
            return;
        }
        try (IntVector groupIds = add(vector)) {
            addInput.add(0, groupIds);
        }
    }

    /**
     *  Adds the vector values to the hash, and returns a new vector with the group IDs for those positions.
     */
    IntVector add(LongVector vector) {
        if (hash instanceof LongSwissHash swiss && swiss.shouldPrefetch()) {
            return addWithPrefetch(vector, swiss);
        }
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long v = vector.getLong(i);
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
            }
            return builder.build();
        }
    }

    /**
     * Path of {@link #add(Page, GroupingAggregatorFunction.AddInput)} for a {@link #primarySorted} vector page. First finds the
     * runs of equal adjacent values with a plain comparison per row, then hashes one value per run and hands the runs
     * to the aggregators, which then do not have to look for the same boundaries again.
     * <p>
     *     Splitting the work in two keeps the prefetching hash lookups available: even when the key is nearly unique and
     *     every run has one row, the run values are still hashed in prefetched batches.
     * </p>
     */
    private void addRuns(LongVector vector, GroupingAggregatorFunction.AddInput addInput) {
        if (runs == null) {
            runs = new GroupRuns(blockFactory);
        }
        sortedPages++;
        runs.reset();
        int positions = vector.getPositionCount();
        long prev = vector.getLong(0);
        for (int i = 1; i < positions; i++) {
            long v = vector.getLong(i);
            if (v != prev) {
                runs.appendRunEnd(i);
                prev = v;
            }
        }
        runs.appendRunEnd(positions);
        int runCount = runs.runCount();
        if (hash instanceof LongSwissHash swiss && swiss.shouldPrefetch()) {
            int dummy = 0;
            for (int offset = 0; offset < runCount; offset += PREFETCH_BATCH) {
                int batchSize = Math.min(PREFETCH_BATCH, runCount - offset);
                for (int i = 0; i < batchSize; i++) {
                    batchHashes[i] = LongSwissHash.hash(vector.getLong(runs.start(offset + i)));
                    dummy ^= swiss.prefetch(batchHashes[i]);
                }
                for (int i = 0; i < batchSize; i++) {
                    final long id = swiss.addWithHash(vector.getLong(runs.start(offset + i)), batchHashes[i]);
                    runs.setGroupId(offset + i, Math.toIntExact(hashOrdToGroupNullReserved(id)));
                }
            }
            prefetchBarrier.consume(dummy);
            addInput.addRuns(0, runs);
            return;
        }
        for (int run = 0; run < runCount; run++) {
            runs.setGroupId(run, Math.toIntExact(hashOrdToGroupNullReserved(hash.add(vector.getLong(runs.start(run))))));
        }
        addInput.addRuns(0, runs);
    }

    private IntVector addWithPrefetch(LongVector vector, LongSwissHash swiss) {
        int positions = vector.getPositionCount();
        int dummy = 0;
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int offset = 0; offset < positions; offset += PREFETCH_BATCH) {
                int batchSize = Math.min(PREFETCH_BATCH, positions - offset);
                for (int i = 0; i < batchSize; i++) {
                    batchHashes[i] = LongSwissHash.hash(vector.getLong(offset + i));
                    dummy ^= swiss.prefetch(batchHashes[i]);
                }
                for (int i = 0; i < batchSize; i++) {
                    final long id = swiss.addWithHash(vector.getLong(offset + i), batchHashes[i]);
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(id)));
                }
            }
            prefetchBarrier.consume(dummy);
            return builder.build();
        }
    }

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(LongBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeLong(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        LongBlock castBlock = (LongBlock) block;
        LongVector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(LongVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long v = vector.getLong(i);
                long found = hash.find(v);
                if (found < 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(found)));
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(LongBlock block) {
        return new MultivalueDedupeLong(block).hashLookup(blockFactory, hash);
    }

    @Override
    public LongBlock[] getKeys(IntVector selected) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (groupId == 0) {
                    builder.appendNull();
                } else {
                    builder.appendLong(hash.get(groupId - 1));
                }
            }
            return new LongBlock[] { builder.build() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1));
    }

    @Override
    public int numKeys() {
        if (seenNull) {
            return Math.toIntExact(hash.size() + 1);
        } else {
            return Math.toIntExact(hash.size());
        }
    }

    @Override
    public long estimatedBytesForPartitioning() {
        return (long) numKeys() * Long.BYTES;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1)).seenGroupIds(bigArrays);
    }

    /**
     * Carries the {@code seenNull} flag through the split/combine round-trip alongside the delegate keys.
     * <p>
     * This hash reserves group ordinal 0 for null and stores null outside its Swiss hash; Swiss ordinals are
     * therefore offset by 1 from group IDs. When {@code seenNull} is {@code true}, null's aggregation state
     * was emitted as one extra entry in partition 0 of the aggregation split, and {@link #keysInPartition(int)}
     * returns +1 for partition 0 so the combiner allocates space for it.
     */
    private record PartitionedHashKeysWithSeenNull(PartitionedHashTable.PartitionedHashKeys delegate, boolean seenNull)
        implements
            PartitionedHashTable.PartitionedHashKeys {

        @Override
        public int keysInPartition(int partition) {
            return delegate.keysInPartition(partition) + (seenNull && partition == 0 ? 1 : 0);
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            delegate.releasePartition(breaker, partition);
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            delegate.releaseAll(breaker);
        }
    }

    @Override
    public PartitionedHashTable.PartitionedHashKeys splitPartition(
        CircuitBreaker breaker,
        PartitionedHashTable.PartitionSplitter partitionSplitter
    ) {
        if (hash instanceof LongSwissHash swiss) {
            // Swiss ordinals are 0-indexed but group IDs reserve 0 for null, so non-null keys start at 1.
            // withOffset(+1) shifts firstId so the aggregation splitter reads from the correct group-ID slots.
            var groupIdSplitter = PartitionedHashTable.PartitionSplitter.withOffset(partitionSplitter, 1);
            PartitionedHashTable.PartitionedHashKeys keys = swiss.splitPartition(breaker, groupIdSplitter);
            boolean success = false;
            try {
                if (seenNull) {
                    // Emit null's aggregation state (group ID 0) as one extra entry appended to partition 0.
                    int nullOffset = keys.keysInPartition(0);
                    int[] singleNullCounts = new int[PartitionedHashTable.NUM_PARTITIONS];
                    singleNullCounts[0] = 1;
                    int[] singleNullOffsets = new int[PartitionedHashTable.NUM_PARTITIONS];
                    singleNullOffsets[0] = nullOffset;
                    // shiftedIds needs only 1 element: null is the sole entry in partition 0 (shifted ID = 0)
                    partitionSplitter.split(0, new short[1], 1, singleNullCounts, singleNullOffsets);
                }
                success = true;
                return new PartitionedHashKeysWithSeenNull(keys, seenNull);
            } finally {
                if (success == false) {
                    keys.releaseAll(breaker);
                }
            }
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public boolean combinePartition(PartitionedHashTable.PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        if (hash instanceof LongSwissHash swiss) {
            PartitionedHashKeysWithSeenNull withSeenNull = (PartitionedHashKeysWithSeenNull) keys;
            int numNonNullKeys = withSeenNull.delegate().keysInPartition(partitionIndex);
            boolean appendOnly = swiss.combinePartition(withSeenNull.delegate(), partitionIndex, resultIds);
            // Shift all result IDs by 1: Swiss ordinals (0-indexed) → group IDs (1-indexed, null reserved at 0)
            for (int i = 0; i < numNonNullKeys; i++) {
                resultIds[i]++;
            }
            if (withSeenNull.seenNull() && partitionIndex == 0) {
                // Only mark seenNull on the target hash for the one partition that carries null's state.
                seenNull = true;
                resultIds[numNonNullKeys] = 0; // null's group ID
                return false; // can't use appendOnly with the null entry mixed in
            }
            return appendOnly;
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public void ensureCapacity(int size) {
        if (hash instanceof LongSwissHash swiss) {
            swiss.ensureCapacity(size);
        }
    }

    @Override
    public void clear() {
        seenNull = false;
        hash.clear();
    }

    @Override
    public void close() {
        prefetchBarrier.flush();
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("LongBlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
        b.append(", seenNull=").append(seenNull);
        if (primarySorted) {
            b.append(", primarySorted=true, sortedPages=").append(sortedPages);
        }
        return b.append('}').toString();
    }
}
