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
            new AddBlock(block1, block2, addInput).add();
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

    private class AddBlock extends AbstractAddBlock {
        private final LongBlock block1;
        private final LongBlock block2;

        AddBlock(LongBlock block1, LongBlock block2, GroupingAggregatorFunction.AddInput addInput) {
            super(emitBatchSize, addInput);
            this.block1 = block1;
            this.block2 = block2;
        }

        void add() {
            int positions = block1.getPositionCount();
            long[] seen1 = EMPTY;
            long[] seen2 = EMPTY;
            for (int p = 0; p < positions; p++) {
                if (block1.isNull(p) || block2.isNull(p)) {
                    ords.appendNull();
                    addedValue(p);
                    continue;
                }
                // TODO use MultivalueDedupe
                int start1 = block1.getFirstValueIndex(p);
                int start2 = block2.getFirstValueIndex(p);
                int count1 = block1.getValueCount(p);
                int count2 = block2.getValueCount(p);
                if (count1 == 1 && count2 == 1) {
                    ords.appendLong(hashOrdToGroup(hash.add(block1.getLong(start1), block2.getLong(start2))));
                    addedValue(p);
                    continue;
                }
                int end = start1 + count1;
                if (seen1.length < count1) {
                    seen1 = new long[ArrayUtil.oversize(count1, Long.BYTES)];
                }
                int seenSize1 = 0;
                for (int i = start1; i < end; i++) {
                    seenSize1 = LongLongBlockHash.add(seen1, seenSize1, block1.getLong(i));
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
                    ords.appendLong(hashOrdToGroup(hash.add(seen1[0], seen2[0])));
                    addedValue(p);
                    continue;
                }
                ords.beginPositionEntry();
                for (int s1 = 0; s1 < seenSize1; s1++) {
                    for (int s2 = 0; s2 < seenSize2; s2++) {
                        ords.appendLong(hashOrdToGroup(hash.add(seen1[s1], seen2[s2])));
                        addedValueInMultivaluePosition(p);
                    }
                }
                ords.endPositionEntry();
            }
            emitOrds();
        }
    }

    static class AbstractAddBlock {
        private final int emitBatchSize;
        private final GroupingAggregatorFunction.AddInput addInput;

        private int positionOffset = 0;
        private int added = 0;
        protected LongBlock.Builder ords;

        AbstractAddBlock(int emitBatchSize, GroupingAggregatorFunction.AddInput addInput) {
            this.emitBatchSize = emitBatchSize;
            this.addInput = addInput;

            this.ords = LongBlock.newBlockBuilder(emitBatchSize);
        }

        protected final void addedValue(int position) {
            if (++added % emitBatchSize == 0) {
                rollover(position + 1);
            }
        }

        protected final void addedValueInMultivaluePosition(int position) {
            if (++added % emitBatchSize == 0) {
                ords.endPositionEntry();
                rollover(position);
                ords.beginPositionEntry();
            }
        }

        protected final void emitOrds() {
            LongBlock groupIdsBlock = ords.build();
            LongVector groupIdsVector = groupIdsBlock.asVector();
            if (groupIdsVector == null) {
                addInput.add(positionOffset, groupIdsBlock);
            } else {
                addInput.add(positionOffset, groupIdsVector);
            }
        }

        private void rollover(int position) {
            emitOrds();
            positionOffset = position;
            ords = LongBlock.newBlockBuilder(emitBatchSize); // TODO build a clear method on the builder?
        }
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
