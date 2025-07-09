/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBytesRef;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.core.ReleasableIterator;

/**
 * Maps a {@link BytesRefBlock} column to group ids.
 * This class is generated. Edit {@code X-BlockHash.java.st} instead.
 */
final class BytesRefBlockHash extends BlockHash {
    private final int channel;
    final BytesRefHash hash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    BytesRefBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.hash = new BytesRefHash(1, blockFactory.bigArrays());
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
        BytesRefBlock castBlock = (BytesRefBlock) block;
        BytesRefVector vector = castBlock.asVector();
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
    IntVector add(BytesRefVector vector) {
        var ordinals = vector.asOrdinals();
        if (ordinals != null) {
            return addOrdinalsVector(ordinals);
        }
        BytesRef scratch = new BytesRef();
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                BytesRef v = vector.getBytesRef(i, scratch);
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
            }
            return builder.build();
        }
    }

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(BytesRefBlock block) {
        var ordinals = block.asOrdinals();
        if (ordinals != null) {
            return addOrdinalsBlock(ordinals);
        }
        MultivalueDedupe.HashResult result = new MultivalueDedupeBytesRef(block).hashAdd(blockFactory, hash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        BytesRefBlock castBlock = (BytesRefBlock) block;
        BytesRefVector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntVector addOrdinalsVector(OrdinalBytesRefVector inputBlock) {
        IntVector inputOrds = inputBlock.getOrdinalsVector();
        try (
            var builder = blockFactory.newIntVectorBuilder(inputOrds.getPositionCount());
            var hashOrds = add(inputBlock.getDictionaryVector())
        ) {
            for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                int ord = hashOrds.getInt(inputOrds.getInt(p));
                builder.appendInt(ord);
            }
            return builder.build();
        }
    }

    private IntBlock addOrdinalsBlock(OrdinalBytesRefBlock inputBlock) {
        try (
            IntBlock inputOrds = new MultivalueDedupeInt(inputBlock.getOrdinalsBlock()).dedupeToBlockAdaptive(blockFactory);
            IntBlock.Builder builder = blockFactory.newIntBlockBuilder(inputOrds.getPositionCount());
            IntVector hashOrds = add(inputBlock.getDictionaryVector())
        ) {
            for (int p = 0; p < inputOrds.getPositionCount(); p++) {
                int valueCount = inputOrds.getValueCount(p);
                int firstIndex = inputOrds.getFirstValueIndex(p);
                switch (valueCount) {
                    case 0 -> {
                        builder.appendInt(0);
                        seenNull = true;
                    }
                    case 1 -> {
                        int ord = hashOrds.getInt(inputOrds.getInt(firstIndex));
                        builder.appendInt(ord);
                    }
                    default -> {
                        int start = firstIndex;
                        int end = firstIndex + valueCount;
                        builder.beginPositionEntry();
                        for (int i = start; i < end; i++) {
                            int ord = hashOrds.getInt(inputOrds.getInt(i));
                            builder.appendInt(ord);
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(BytesRefVector vector) {
        BytesRef scratch = new BytesRef();
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                BytesRef v = vector.getBytesRef(i, scratch);
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

    private IntBlock lookup(BytesRefBlock block) {
        return new MultivalueDedupeBytesRef(block).hashLookup(blockFactory, hash);
    }

    @Override
    public BytesRefBlock[] getKeys() {
        /*
         * Create an un-owned copy of the data so we can close our BytesRefHash
         * without and still read from the block.
         */
        // TODO replace with takeBytesRefsOwnership ?!
        final BytesRef spare = new BytesRef();
        if (seenNull) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(Math.toIntExact(hash.size() + 1))) {
                builder.appendNull();
                for (long i = 0; i < hash.size(); i++) {
                    builder.appendBytesRef(hash.get(i, spare));
                }
                return new BytesRefBlock[] { builder.build() };
            }
        }
        try (var builder = blockFactory.newBytesRefBlockBuilder(Math.toIntExact(hash.size()))) {
            for (long i = 0; i < hash.size(); i++) {
                builder.appendBytesRef(hash.get(i, spare));
            }
            return new BytesRefBlock[] { builder.build() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(hash.size() + 1), blockFactory);
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
        b.append("BytesRefBlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
        b.append(", size=").append(ByteSizeValue.ofBytes(hash.ramBytesUsed()));
        b.append(", seenNull=").append(seenNull);
        return b.append('}').toString();
    }
}
