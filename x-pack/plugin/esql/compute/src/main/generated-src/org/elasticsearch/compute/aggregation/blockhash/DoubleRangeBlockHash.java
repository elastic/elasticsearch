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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.BytesRefHashTable;
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
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeDoubleRange;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.core.ReleasableIterator;
import java.util.BitSet;
// end generated imports

/**
 * Maps a {@link DoubleRangeBlock} column to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class DoubleRangeBlockHash extends BlockHash {
    private final int channel;
    final BytesRefHashTable hash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    DoubleRangeBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.hash = HashImplFactory.newBytesRefHash(blockFactory);
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
        DoubleRangeBlock castBlock = (DoubleRangeBlock) block;
        try (IntBlock groupIds = add(castBlock)) {
            addInput.add(0, groupIds);
        }
    }

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(DoubleRangeBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeDoubleRange(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        DoubleRangeBlock castBlock = (DoubleRangeBlock) block;
        return ReleasableIterator.single(lookup(castBlock));
    }

    private IntBlock lookup(DoubleRangeBlock block) {
        return new MultivalueDedupeDoubleRange(block).hashLookup(blockFactory, hash);
    }

    @Override
    public DoubleRangeBlock[] getKeys(IntVector selected) {
        final BytesRef spare = new BytesRef();
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                if (groupId == 0) {
                    builder.appendNull();
                } else {
                    hash.get(groupId - 1, spare);
                    builder.appendDoubleRange(
                        Double.longBitsToDouble(ByteUtils.readLongLE(spare.bytes, spare.offset)),
                        Double.longBitsToDouble(ByteUtils.readLongLE(spare.bytes, spare.offset + Double.BYTES))
                    );
                }
            }
            return new DoubleRangeBlock[] { builder.build() };
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
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("DoubleRangeBlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(hash.ramBytesUsed()));
        b.append(", seenNull=").append(seenNull);
        return b.append('}').toString();
    }
}
