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
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.io.IOException;

final class BytesRefBlockHash extends BlockHash {
    private final BytesRef bytes = new BytesRef();
    private final int channel;
    private final BytesRefHash bytesRefHash;

    BytesRefBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        BytesRefBlock block = page.getBlock(channel);
        int positionCount = block.getPositionCount();
        BytesRefVector vector = block.asVector();
        if (vector != null) {
            long[] groups = new long[positionCount];
            for (int i = 0; i < positionCount; i++) {
                groups[i] = hashOrdToGroup(bytesRefHash.add(vector.getBytesRef(i, bytes)));
            }
            return new LongArrayVector(groups, positionCount).asBlock();
        }
        LongBlock.Builder builder = LongBlock.newBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (block.isNull(i)) {
                builder.appendNull();
            } else {
                builder.appendLong(hashOrdToGroup(bytesRefHash.add(block.getBytesRef(block.getFirstValueIndex(i), bytes))));
            }
        }
        return builder.build();
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
