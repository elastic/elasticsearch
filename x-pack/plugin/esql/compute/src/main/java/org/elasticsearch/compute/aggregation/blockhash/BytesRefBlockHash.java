/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.compute.operator.MultivalueDedupeBytesRef;

import java.io.IOException;

/**
 * Maps a {@link BytesRefBlock} column to group ids.
 */
final class BytesRefBlockHash extends BlockHash {
    private final BytesRef bytes = new BytesRef();
    private final int channel;
    private final BytesRefHash bytesRefHash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    BytesRefBlockHash(int channel, DriverContext driverContext) {
        super(driverContext);
        this.channel = channel;
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        Block block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            seenNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
        } else {
            BytesRefBlock bytesBlock = (BytesRefBlock) block;
            BytesRefVector bytesVector = bytesBlock.asVector();
            if (bytesVector == null) {
                try (IntBlock groupIds = add(bytesBlock)) {
                    addInput.add(0, groupIds);
                }
            } else {
                try (IntVector groupIds = add(bytesVector)) {
                    addInput.add(0, groupIds);
                }
            }
        }
    }

    private IntVector add(BytesRefVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(bytesRefHash.add(vector.getBytesRef(i, bytes)))));
            }
            return builder.build();
        }
    }

    private IntBlock add(BytesRefBlock block) {
        // TODO: use block factory
        MultivalueDedupe.HashResult result = new MultivalueDedupeBytesRef(block).hash(bytesRefHash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public BytesRefBlock[] getKeys() {
        /*
         * Create an un-owned copy of the data so we can close our BytesRefHash
         * without and still read from the block.
         */
        // TODO replace with takeBytesRefsOwnership ?!

        if (seenNull) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(Math.toIntExact(bytesRefHash.size() + 1))) {
                builder.appendNull();
                BytesRef spare = new BytesRef();
                for (long i = 0; i < bytesRefHash.size(); i++) {
                    builder.appendBytesRef(bytesRefHash.get(i, spare));
                }
                return new BytesRefBlock[] { builder.build() };
            }
        }

        final int size = Math.toIntExact(bytesRefHash.size());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            bytesRefHash.getBytesRefs().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new BytesRefBlock[] {
                    blockFactory.newBytesRefArrayVector(new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE), size).asBlock() };
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(bytesRefHash.size() + 1), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(bytesRefHash.size() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        bytesRefHash.close();
    }

    @Override
    public String toString() {
        return "BytesRefBlockHash{channel="
            + channel
            + ", entries="
            + bytesRefHash.size()
            + ", size="
            + ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed())
            + ", seenNull="
            + seenNull
            + '}';
    }
}
