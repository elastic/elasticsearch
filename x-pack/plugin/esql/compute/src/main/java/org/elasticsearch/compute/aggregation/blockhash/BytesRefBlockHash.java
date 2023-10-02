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
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
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

    BytesRefBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock block = page.getBlock(channel);
        BytesRefVector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private IntVector add(BytesRefVector vector) {
        int[] groups = new int[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = Math.toIntExact(hashOrdToGroupNullReserved(bytesRefHash.add(vector.getBytesRef(i, bytes))));
        }
        return new IntArrayVector(groups, vector.getPositionCount());
    }

    private IntBlock add(BytesRefBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeBytesRef(Block.Ref.floating(block)).hash(bytesRefHash);
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
            BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(Math.toIntExact(bytesRefHash.size() + 1));
            builder.appendNull();
            BytesRef spare = new BytesRef();
            for (long i = 0; i < bytesRefHash.size(); i++) {
                builder.appendBytesRef(bytesRefHash.get(i, spare));
            }
            return new BytesRefBlock[] { builder.build() };
        }

        final int size = Math.toIntExact(bytesRefHash.size());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            bytesRefHash.getBytesRefs().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new BytesRefBlock[] {
                    new BytesRefArrayVector(new BytesRefArray(in, BigArrays.NON_RECYCLING_INSTANCE), size).asBlock() };
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(bytesRefHash.size() + 1));
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
