/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

// begin generated imports
$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.$Hash$Table;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.BlockFactory;
$if(int)$
$else$
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
$endif$
$if(BytesRef)$
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
$endif$
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe$Type$;
$if(BytesRef)$
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
$endif$
import org.elasticsearch.core.ReleasableIterator;
$if(BytesRef)$
import org.elasticsearch.swisshash.BytesRefSwissHash;
import java.util.Arrays;
$endif$
// end generated imports

/**
 * Maps a {@link $Type$Block} column to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class $Type$BlockHash extends BlockHash {
    private final int channel;
    final $Hash$Table hash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

$if(BytesRef)$
    private static final int PREFETCH_BATCH = 128;
    private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
    private final long[] batchHashes = new long[PREFETCH_BATCH];
    private final BytesRef[] batchKeys = new BytesRef[PREFETCH_BATCH];
    {
        for (int i = 0; i < PREFETCH_BATCH; i++) {
            batchKeys[i] = new BytesRef();
        }
    }
$endif$

    $Type$BlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.hash = HashImplFactory.new$Hash$(blockFactory);
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
        $Type$Block castBlock = ($Type$Block) block;
        $Type$Vector vector = castBlock.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(castBlock)) {
                addInput.add(0, groupIds);
            }
            return;
        }
        try (IntVector groupIds = add(vector)) {
            addInput.add(0, groupIds);
        }
    }

    /**
     *  Adds the vector values to the hash, and returns a new vector with the group IDs for those positions.
     */
    IntVector add($Type$Vector vector) {
$if(BytesRef)$
        if (vector.isConstant()) {
            BytesRef v = vector.getBytesRef(0, new BytesRef());
            int groupId = Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v)));
            return blockFactory.newConstantIntVector(groupId, vector.getPositionCount());
        }
        var ordinals = vector.asOrdinals();
        if (ordinals != null) {
            return addOrdinalsVector(ordinals);
        }
        if (hash instanceof BytesRefSwissHash swiss && swiss.shouldPrefetch()) {
            return addWithPrefetch(vector, swiss);
        }
        BytesRef scratch = new BytesRef();
$endif$
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
$if(double)$
                long v = Double.doubleToLongBits(vector.getDouble(i));
$elseif(BytesRef)$
                BytesRef v = vector.getBytesRef(i, scratch);
$else$
                $type$ v = vector.get$Type$(i);
$endif$
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
            }
            return builder.build();
        }
    }

$if(BytesRef)$
    private IntVector addWithPrefetch(BytesRefVector vector, BytesRefSwissHash swiss) {
        int positions = vector.getPositionCount();
        int dummy = 0;
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int offset = 0; offset < positions; offset += PREFETCH_BATCH) {
                int batchSize = Math.min(PREFETCH_BATCH, positions - offset);
                for (int i = 0; i < batchSize; i++) {
                    vector.getBytesRef(offset + i, batchKeys[i]);
                    batchHashes[i] = BytesRefSwissHash.hash64(batchKeys[i]);
                    dummy ^= swiss.prefetch(batchHashes[i]);
                }
                for (int i = 0; i < batchSize; i++) {
                    final long id = swiss.addWithHash(batchKeys[i], batchHashes[i]);
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(id)));
                }
            }
            for (int i = 0; i < PREFETCH_BATCH; i++) {
                batchKeys[i].bytes = BytesRef.EMPTY_BYTES;
                batchKeys[i].offset = 0;
                batchKeys[i].length = 0;
            }
            prefetchBarrier.consume(dummy);
            return builder.build();
        }
    }
$endif$

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add($Type$Block block) {
$if(BytesRef)$
        var ordinals = block.asOrdinals();
        if (ordinals != null) {
            return addOrdinalsBlock(ordinals);
        }
$endif$
        MultivalueDedupe.HashResult result = new MultivalueDedupe$Type$(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        $Type$Block castBlock = ($Type$Block) block;
        $Type$Vector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

$if(BytesRef)$
    /**
     * Sentinel for "this dictionary ord has not yet been hashed into the global hash on this page".
     * Stored in the per-page {@code localToGlobal} array; replaced with the real group id (always
     * non-negative because of {@link BlockHash#hashOrdToGroupNullReserved}) on first touch.
     */
    private static final int UNSEEN = -1;

    /**
     * Lazily hashes the dictionary entry at {@code ord} into the global hash on first visit and
     * returns the cached group id thereafter. Dictionary entries that no rows reference are never
     * looked up, so phantom entries left over by {@code filter()}/{@code slice()}/{@code keepMask()}
     * (which share the dictionary across the filtered block) are not promoted to groups.
     */
    private int groupForOrd(int ord, BytesRefVector dict, int[] localToGlobal, BytesRef scratch) {
        int g = localToGlobal[ord];
        if (g == UNSEEN) {
            g = Math.toIntExact(hashOrdToGroupNullReserved(hash.add(dict.getBytesRef(ord, scratch))));
            localToGlobal[ord] = g;
        }
        return g;
    }

    private IntVector addOrdinalsVector(OrdinalBytesRefVector inputBlock) {
        IntVector inputOrds = inputBlock.getOrdinalsVector();
        BytesRefVector dict = inputBlock.getDictionaryVector();
        if (inputOrds.isConstant()) {
            BytesRef scratch = new BytesRef();
            int ord = inputOrds.getInt(0);
            int groupId = Math.toIntExact(hashOrdToGroupNullReserved(hash.add(dict.getBytesRef(ord, scratch))));
            return blockFactory.newConstantIntVector(groupId, inputOrds.getPositionCount());
        }
        // The lazy path costs a measurable branch+load per row on dense Parquet pages where every dict
        // entry is referenced (clickbench q16/q17/q18/q34); only take it when phantoms are actually
        // possible (filter()/slice()/keepMask() flagged the block as needing compaction).
        return inputBlock.needsCompaction() ? addOrdinalsVectorLazy(inputOrds, dict) : addOrdinalsVectorEager(inputOrds, dict);
    }

    private IntVector addOrdinalsVectorEager(IntVector inputOrds, BytesRefVector dict) {
        try (var hashOrds = add(dict); var builder = blockFactory.newIntVectorBuilder(inputOrds.getPositionCount())) {
            for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                builder.appendInt(hashOrds.getInt(inputOrds.getInt(p)));
            }
            return builder.build();
        }
    }

    private IntVector addOrdinalsVectorLazy(IntVector inputOrds, BytesRefVector dict) {
        BytesRef scratch = new BytesRef();
        long acquiredBytes = (long) Integer.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "BytesRefBlockHash");
        try {
            int[] localToGlobal = new int[dict.getPositionCount()];
            Arrays.fill(localToGlobal, UNSEEN);
            try (var builder = blockFactory.newIntVectorBuilder(inputOrds.getPositionCount())) {
                for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                    builder.appendInt(groupForOrd(inputOrds.getInt(p), dict, localToGlobal, scratch));
                }
                return builder.build();
            }
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }

    private IntBlock addOrdinalsBlock(OrdinalBytesRefBlock inputBlock) {
        // See addOrdinalsVector for the rationale behind this split.
        return inputBlock.needsCompaction() ? addOrdinalsBlockLazy(inputBlock) : addOrdinalsBlockEager(inputBlock);
    }

    private IntBlock addOrdinalsBlockEager(OrdinalBytesRefBlock inputBlock) {
        BytesRefVector dict = inputBlock.getDictionaryVector();
        try (
            IntBlock inputOrds = new MultivalueDedupeInt(inputBlock.getOrdinalsBlock()).dedupeToBlockAdaptive(blockFactory);
            IntBlock.Builder builder = blockFactory.newIntBlockBuilder(inputOrds.getPositionCount());
            IntVector hashOrds = add(dict)
        ) {
            for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                int valueCount = inputOrds.getValueCount(p);
                int firstIndex = inputOrds.getFirstValueIndex(p);
                switch (valueCount) {
                    case 0 -> {
                        builder.appendInt(0);
                        seenNull = true;
                    }
                    case 1 -> builder.appendInt(hashOrds.getInt(inputOrds.getInt(firstIndex)));
                    default -> {
                        int end = firstIndex + valueCount;
                        builder.beginPositionEntry();
                        for (int i = firstIndex; i < end; i++) {
                            builder.appendInt(hashOrds.getInt(inputOrds.getInt(i)));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }

    private IntBlock addOrdinalsBlockLazy(OrdinalBytesRefBlock inputBlock) {
        BytesRefVector dict = inputBlock.getDictionaryVector();
        long acquiredBytes = (long) Integer.BYTES * dict.getPositionCount();
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(acquiredBytes, "BytesRefBlockHash");
        try {
            int[] localToGlobal = new int[dict.getPositionCount()];
            Arrays.fill(localToGlobal, UNSEEN);
            BytesRef scratch = new BytesRef();
            try (
                IntBlock inputOrds = new MultivalueDedupeInt(inputBlock.getOrdinalsBlock()).dedupeToBlockAdaptive(blockFactory);
                IntBlock.Builder builder = blockFactory.newIntBlockBuilder(inputOrds.getPositionCount())
            ) {
                for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                    int valueCount = inputOrds.getValueCount(p);
                    int firstIndex = inputOrds.getFirstValueIndex(p);
                    switch (valueCount) {
                        case 0 -> {
                            builder.appendInt(0);
                            seenNull = true;
                        }
                        case 1 -> builder.appendInt(groupForOrd(inputOrds.getInt(firstIndex), dict, localToGlobal, scratch));
                        default -> {
                            int end = firstIndex + valueCount;
                            builder.beginPositionEntry();
                            for (int i = firstIndex; i < end; i++) {
                                builder.appendInt(groupForOrd(inputOrds.getInt(i), dict, localToGlobal, scratch));
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
                return builder.build();
            }
        } finally {
            blockFactory.breaker().addWithoutBreaking(-acquiredBytes);
        }
    }
$endif$

    private IntBlock lookup($Type$Vector vector) {
$if(BytesRef)$
        BytesRef scratch = new BytesRef();
$endif$
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
$if(double)$
                long v = Double.doubleToLongBits(vector.getDouble(i));
$elseif(BytesRef)$
                BytesRef v = vector.getBytesRef(i, scratch);
$else$
                $type$ v = vector.get$Type$(i);
$endif$
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

    private IntBlock lookup($Type$Block block) {
        return new MultivalueDedupe$Type$(block).hashLookup(blockFactory, hash);
    }

    @Override
    public $Type$Block[] getKeys(IntVector selected) {
$if(BytesRef)$
        // TODO replace with takeBytesRefsOwnership ?!
        final BytesRef spare = new BytesRef();
$endif$
        try ($Type$Block.Builder builder = blockFactory.new$Type$BlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (groupId == 0) {
                    builder.appendNull();
                } else {
$if(BytesRef)$
                    builder.appendBytesRef(hash.get(groupId - 1, spare));
$elseif(int)$
                    builder.appendInt((int) hash.get(groupId - 1));
$elseif(double)$
                    builder.appendDouble(Double.longBitsToDouble(hash.get(groupId - 1)));
$elseif(long)$
                    builder.appendLong(hash.get(groupId - 1));
$else$
                    syntax error because we didn't cover this case
$endif$
                }
            }
            return new $Type$Block[] { builder.build() };
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
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
$if(BytesRef)$
        prefetchBarrier.flush();
$endif$
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("$Type$BlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
$if(BytesRef)$
        b.append(", size=").append(ByteSizeValue.ofBytes(hash.ramBytesUsed()));
$endif$
        b.append(", seenNull=").append(seenNull);
        return b.append('}').toString();
    }
}
