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
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupeBytesRef;

import java.io.IOException;

/**
 * Maps a {@link BytesRefBlock} column to group ids.
 */
final class BytesRefBlockHash extends BlockHash {
    private final BytesRef bytes = new BytesRef();
    private final int channel;
    private final BytesRefHash bytesRefHash;

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

    private LongVector add(BytesRefVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = hashOrdToGroup(bytesRefHash.add(vector.getBytesRef(i, bytes)));
        }
        return new LongArrayVector(groups, vector.getPositionCount());
    }

    private LongBlock add(BytesRefBlock block) {
        return new MultivalueDedupeBytesRef(block).hash(bytesRefHash);
    }

    protected static int addOrd(LongBlock.Builder builder, long[] seen, int nextSeen, long ord) {
        if (ord < 0) { // already seen
            ord = -1 - ord;
            /*
             * Check if we've seen the value before. This is n^2 on the number of
             * values, but we don't expect many of them in each entry.
             */
            for (int j = 0; j < nextSeen; j++) {
                if (seen[j] == ord) {
                    return nextSeen;
                }
            }
        }
        seen[nextSeen] = ord;
        builder.appendLong(ord);
        return nextSeen + 1;
    }

    @Override
    public BytesRefBlock[] getKeys() {
        final int size = Math.toIntExact(bytesRefHash.size());
        /*
         * Create an un-owned copy of the data so we can close our BytesRefHash
         * without and still read from the block.
         */
        // TODO replace with takeBytesRefsOwnership ?!
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
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()));
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
            + '}';
    }
}
