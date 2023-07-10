/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import static org.elasticsearch.compute.aggregation.blockhash.LongLongBlockHash.add;

/**
 * Maps a {@link LongBlock} column paired with a {@link BytesRefBlock} column to group ids.
 */
final class BytesRefLongBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final BytesRefHash bytesHash;
    private final LongLongHash finalHash;
    private final boolean reverseOutput;

    BytesRefLongBlockHash(BigArrays bigArrays, int channel1, int channel2, boolean reverseOutput) {
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.reverseOutput = reverseOutput;

        boolean success = false;
        BytesRefHash bytesHash = null;
        LongLongHash longHash = null;
        try {
            bytesHash = new BytesRefHash(1, bigArrays);
            longHash = new LongLongHash(1, bigArrays);
            this.bytesHash = bytesHash;
            this.finalHash = longHash;
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(bytesHash);
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(bytesHash, finalHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        BytesRefVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            addInput.add(0, add(vector1, vector2));
        } else {
            add(block1, block2, addInput);
        }
    }

    public LongVector add(BytesRefVector vector1, LongVector vector2) {
        BytesRef scratch = new BytesRef();
        int positions = vector1.getPositionCount();
        final long[] ords = new long[positions];
        for (int i = 0; i < positions; i++) {
            long hash1 = hashOrdToGroup(bytesHash.add(vector1.getBytesRef(i, scratch)));
            ords[i] = hashOrdToGroup(finalHash.add(hash1, vector2.getLong(i)));
        }
        return new LongArrayVector(ords, positions);
    }

    private static final long[] EMPTY = new long[0];

    public void add(BytesRefBlock block1, LongBlock block2, GroupingAggregatorFunction.AddInput addInput) {
        BytesRef scratch = new BytesRef();
        int positions = block1.getPositionCount();
        LongBlock.Builder ords = LongBlock.newBlockBuilder(positions);
        long[] seen1 = EMPTY;
        long[] seen2 = EMPTY;
        for (int p = 0; p < positions; p++) {
            if (block1.isNull(p) || block2.isNull(p)) {
                ords.appendNull();
                continue;
            }
            // TODO use MultivalueDedupe
            int start1 = block1.getFirstValueIndex(p);
            int start2 = block2.getFirstValueIndex(p);
            int count1 = block1.getValueCount(p);
            int count2 = block2.getValueCount(p);
            if (count1 == 1 && count2 == 1) {
                long bytesOrd = hashOrdToGroup(bytesHash.add(block1.getBytesRef(start1, scratch)));
                ords.appendLong(hashOrdToGroup(finalHash.add(bytesOrd, block2.getLong(start2))));
                continue;
            }
            int end = start1 + count1;
            if (seen1.length < count1) {
                seen1 = new long[ArrayUtil.oversize(count1, Long.BYTES)];
            }
            int seenSize1 = 0;
            for (int i = start1; i < end; i++) {
                long bytesOrd = bytesHash.add(block1.getBytesRef(i, scratch));
                if (bytesOrd < 0) { // already seen
                    seenSize1 = LongLongBlockHash.add(seen1, seenSize1, -1 - bytesOrd);
                } else {
                    seen1[seenSize1++] = bytesOrd;
                }
            }
            if (seen2.length < count2) {
                seen2 = new long[ArrayUtil.oversize(count2, Long.BYTES)];
            }
            int seenSize2 = 0;
            end = start2 + count2;
            for (int i = start2; i < end; i++) {
                seenSize2 = LongLongBlockHash.add(seen2, seenSize2, block2.getLong(i));
            }
            if (seenSize1 == 1 && seenSize2 == 1) {
                ords.appendLong(hashOrdToGroup(finalHash.add(seen1[0], seen2[0])));
                continue;
            }
            ords.beginPositionEntry();
            for (int s1 = 0; s1 < seenSize1; s1++) {
                for (int s2 = 0; s2 < seenSize2; s2++) {
                    ords.appendLong(hashOrdToGroup(finalHash.add(seen1[s1], seen2[s2])));
                }
            }
            ords.endPositionEntry();
        }
        addInput.add(0, ords.build()); // TODO exploit for a crash and then call incrementally
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) finalHash.size();
        BytesRefVector.Builder keys1 = BytesRefVector.newVectorBuilder(positions);
        LongVector.Builder keys2 = LongVector.newVectorBuilder(positions);
        BytesRef scratch = new BytesRef();
        for (long i = 0; i < positions; i++) {
            keys2.appendLong(finalHash.getKey2(i));
            long h1 = finalHash.getKey1(i);
            keys1.appendBytesRef(bytesHash.get(h1, scratch));
        }
        if (reverseOutput) {
            return new Block[] { keys2.build().asBlock(), keys1.build().asBlock() };
        } else {
            return new Block[] { keys1.build().asBlock(), keys2.build().asBlock() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(finalHash.size()));
    }

    @Override
    public String toString() {
        return "BytesRefLongBlockHash{keys=[BytesRefKey[channel="
            + channel1
            + "], LongKey[channel="
            + channel2
            + "]], entries="
            + finalHash.size()
            + ", size="
            + bytesHash.ramBytesUsed()
            + "b}";
    }
}
