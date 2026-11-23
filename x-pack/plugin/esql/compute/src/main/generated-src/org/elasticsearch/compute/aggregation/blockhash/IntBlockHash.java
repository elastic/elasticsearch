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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.swisshash.LongSwissHash;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import java.util.BitSet;
// end generated imports

/**
 * Maps a {@link IntBlock} column to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class IntBlockHash extends PartitionedBlockHash {
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
     * Scratch for the runs of the page being added, reused for every page.
     */
    private GroupRuns runs;

    /**
     * How many pages took the runs path, so a profile can confirm that it paid off.
     */
    private long runPages;

    /**
     * How many pages looked for runs and gave up because their keys repeat too little, so a profile shows that the page
     * was hashed row by row.
     */
    private long rowByRowPages;

    /**
     * The smallest average run length at which a page takes the runs path. Finding the runs costs a comparison per row and
     * saves one hash probe per row that repeats the previous key, and the aggregators that cannot combine a run still pay
     * to expand it, so it only pays when keys repeat several times in a row. Measured with {@code COUNT}, {@code AVG},
     * {@code MIN}, {@code MAX} and filtered aggregations: at 2 the aggregators that expand runs lose up to 40% on keys whose
     * runs average 2 to 3 rows, at 4 and above every measured shape stays within noise of the row by row path.
     */
    private static final int MIN_AVERAGE_RUN_LENGTH = 4;

    /**
     * How many rows are scanned before the average run length is first checked. A few short runs at the start of a page
     * must not decide for the whole page, but a page of unique keys should stop being scanned after a few dozen rows.
     */
    private static final int PROBE_ROWS = 64;

    IntBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.hash = HashImplFactory.newLongHash(blockFactory);
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
        IntBlock castBlock = (IntBlock) block;
        IntVector vector = castBlock.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(castBlock)) {
                addInput.add(0, groupIds);
            }
            return;
        }
        if (vector.getPositionCount() > 0) {
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
    IntVector add(IntVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                int v = vector.getInt(i);
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
            }
            return builder.build();
        }
    }

    /**
     * Path of {@link #add(Page, GroupingAggregatorFunction.AddInput)} for a vector page. Keys often arrive in runs of equal
     * adjacent values: the primary {@code index.sort.field} of the shard, a time series, or just a dominant value. This
     * finds the runs with a plain comparison per row, hashes one value per run and hands the runs to the aggregators once,
     * so that none of them has to look for the same boundaries again. The result is the same as for the row by row path,
     * only the cost differs.
     * <p>
     *     The search stops, and the page is hashed row by row, as soon as the completed runs average fewer than
     *     {@link #MIN_AVERAGE_RUN_LENGTH} rows, after at least {@link #PROBE_ROWS} rows. So a page of unique keys costs a
     *     few dozen comparisons, not a whole scan. The choice is made for every page on its own.
     * </p>
     */
    private void addRuns(IntVector vector, GroupingAggregatorFunction.AddInput addInput) {
        if (runs == null) {
            runs = new GroupRuns(blockFactory);
        }
        runs.reset();
        int positions = vector.getPositionCount();
        int prev = vector.getInt(0);
        for (int i = 1; i < positions; i++) {
            int v = vector.getInt(i);
            if (v != prev) {
                runs.appendRunEnd(i);
                if (i >= PROBE_ROWS && (long) runs.runCount() * MIN_AVERAGE_RUN_LENGTH > i) {
                    addRowByRow(vector, addInput);
                    return;
                }
                prev = v;
            }
        }
        runs.appendRunEnd(positions);
        int runCount = runs.runCount();
        if ((long) runCount * MIN_AVERAGE_RUN_LENGTH > positions) {
            addRowByRow(vector, addInput);
            return;
        }
        runPages++;
        for (int run = 0; run < runCount; run++) {
            runs.setGroupId(run, Math.toIntExact(hashOrdToGroupNullReserved(hash.add(vector.getInt(runs.start(run))))));
        }
        try {
            addInput.addRuns(0, runs);
        } finally {
            runs.release();
        }
    }

    private void addRowByRow(IntVector vector, GroupingAggregatorFunction.AddInput addInput) {
        rowByRowPages++;
        try (IntVector groupIds = add(vector)) {
            addInput.add(0, groupIds);
        }
    }

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(IntBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeInt(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        IntBlock castBlock = (IntBlock) block;
        IntVector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(IntVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                int v = vector.getInt(i);
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

    private IntBlock lookup(IntBlock block) {
        return new MultivalueDedupeInt(block).hashLookup(blockFactory, hash);
    }

    @Override
    public IntBlock[] getKeys(IntVector selected) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (groupId == 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt((int) hash.get(groupId - 1));
                }
            }
            return new IntBlock[] { builder.build() };
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
        return (long) numKeys() * Integer.BYTES;
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
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("IntBlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
        b.append(", seenNull=").append(seenNull);
        if (runPages + rowByRowPages > 0) {
            b.append(", runPages=").append(runPages).append(", rowByRowPages=").append(rowByRowPages);
        }
        return b.append('}').toString();
    }
}
