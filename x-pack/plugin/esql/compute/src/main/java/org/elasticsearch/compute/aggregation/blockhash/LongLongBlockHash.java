/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.core.Releasables;

/**
 * Maps two {@link LongBlock} columns to group ids.
 */
final class LongLongBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final int emitBatchSize;
    private final LongLongHash hash;

    LongLongBlockHash(BigArrays bigArrays, int channel1, int channel2, int emitBatchSize) {
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.emitBatchSize = emitBatchSize;
        this.hash = new LongLongHash(1, bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(hash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        LongBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        LongVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            addInput.add(0, add(vector1, vector2));
        } else {
            add(block1, block2, addInput);
        }
    }

    private LongVector add(LongVector vector1, LongVector vector2) {
        int positions = vector1.getPositionCount();
        final long[] ords = new long[positions];
        for (int i = 0; i < positions; i++) {
            ords[i] = hashOrdToGroup(hash.add(vector1.getLong(i), vector2.getLong(i)));
        }
        return new LongArrayVector(ords, positions);
    }

    private static final long[] EMPTY = new long[0];

    private void add(LongBlock block1, LongBlock block2, GroupingAggregatorFunction.AddInput addInput) {
        int positions = block1.getPositionCount();
        LongBlock.Builder ords = LongBlock.newBlockBuilder(
            Math.min(LuceneSourceOperator.PAGE_SIZE, block1.getPositionCount() * block2.getPositionCount())
        );
        long[] seen1 = EMPTY;
        long[] seen2 = EMPTY;
        int added = 0;
        int positionOffset = 0;
        for (int p = 0; p < positions; p++) {
            if (block1.isNull(p) || block2.isNull(p)) {
                ords.appendNull();
                if (++added % emitBatchSize == 0) {
                    addInput.add(positionOffset, ords.build());
                    positionOffset = p;
                    ords = LongBlock.newBlockBuilder(positions); // TODO build a clear method on the builder?
                }
                continue;
            }
            // TODO use MultivalueDedupe
            int start1 = block1.getFirstValueIndex(p);
            int start2 = block2.getFirstValueIndex(p);
            int count1 = block1.getValueCount(p);
            int count2 = block2.getValueCount(p);
            if (count1 == 1 && count2 == 1) {
                ords.appendLong(hashOrdToGroup(hash.add(block1.getLong(start1), block2.getLong(start2))));
                if (++added % emitBatchSize == 0) {
                    addInput.add(positionOffset, ords.build());
                    positionOffset = p;
                    ords = LongBlock.newBlockBuilder(positions); // TODO build a clear method on the builder?
                }
                continue;
            }
            int end = start1 + count1;
            if (seen1.length < count1) {
                seen1 = new long[ArrayUtil.oversize(count1, Long.BYTES)];
            }
            int seenSize1 = 0;
            for (int i = start1; i < end; i++) {
                seenSize1 = add(seen1, seenSize1, block1.getLong(i));
            }
            if (seen2.length < count2) {
                seen2 = new long[ArrayUtil.oversize(count2, Long.BYTES)];
            }
            int seenSize2 = 0;
            end = start2 + count2;
            for (int i = start2; i < end; i++) {
                seenSize2 = add(seen2, seenSize2, block2.getLong(i));
            }
            if (seenSize1 == 1 && seenSize2 == 1) {
                ords.appendLong(hashOrdToGroup(hash.add(seen1[0], seen2[0])));
                if (++added % emitBatchSize == 0) {
                    addInput.add(positionOffset, ords.build());
                    positionOffset = p;
                    ords = LongBlock.newBlockBuilder(positions); // TODO build a clear method on the builder?
                }
                continue;
            }
            ords.beginPositionEntry();
            for (int s1 = 0; s1 < seenSize1; s1++) {
                for (int s2 = 0; s2 < seenSize2; s2++) {
                    ords.appendLong(hashOrdToGroup(hash.add(seen1[s1], seen2[s2])));
                    if (++added % emitBatchSize == 0) {
                        ords.endPositionEntry();
                        addInput.add(positionOffset, ords.build());
                        positionOffset = p;
                        ords = LongBlock.newBlockBuilder(positions); // TODO build a clear method on the builder?
                        ords.beginPositionEntry();
                    }
                }
            }
            ords.endPositionEntry();
        }
        addInput.add(positionOffset, ords.build());
    }

    static int add(long[] seen, int nextSeen, long v) {
        for (int c = 0; c < nextSeen; c++) {
            if (seen[c] == v) {
                return nextSeen;
            }
        }
        seen[nextSeen] = v;
        return nextSeen + 1;
    }

    @Override
    public Block[] getKeys() {
        int positions = (int) hash.size();
        LongVector.Builder keys1 = LongVector.newVectorBuilder(positions);
        LongVector.Builder keys2 = LongVector.newVectorBuilder(positions);
        for (long i = 0; i < positions; i++) {
            keys1.appendLong(hash.getKey1(i));
            keys2.appendLong(hash.getKey2(i));
        }
        return new Block[] { keys1.build().asBlock(), keys2.build().asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(hash.size()));
    }

    @Override
    public String toString() {
        return "LongLongBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
