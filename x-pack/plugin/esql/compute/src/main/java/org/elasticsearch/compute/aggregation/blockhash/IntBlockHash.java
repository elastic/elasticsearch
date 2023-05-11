/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

final class IntBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    IntBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        IntBlock block = page.getBlock(channel);
        IntVector vector = block.asVector();
        if (vector == null) {
            return add(block);
        }
        return add(vector).asBlock();
    }

    private LongVector add(IntVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = hashOrdToGroup(longHash.add(vector.getInt(i)));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private static final int[] EMPTY = new int[0];

    private LongBlock add(IntBlock block) {
        int[] seen = EMPTY;
        LongBlock.Builder builder = LongBlock.newBlockBuilder(block.getTotalValueCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                builder.appendNull();
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int count = block.getValueCount(p);
            if (count == 1) {
                builder.appendLong(hashOrdToGroup(longHash.add(block.getInt(start))));
                continue;
            }
            if (seen.length < count) {
                seen = new int[ArrayUtil.oversize(count, Integer.BYTES)];
            }
            builder.beginPositionEntry();
            // TODO if we know the elements were in sorted order we wouldn't need an array at all.
            // TODO we could also have an assertion that there aren't any duplicates on the block.
            // Lucene has them in ascending order without duplicates
            int end = start + count;
            int nextSeen = 0;
            for (int offset = start; offset < end; offset++) {
                nextSeen = add(builder, seen, nextSeen, block.getInt(offset));
            }
            builder.endPositionEntry();
        }
        return builder.build();
    }

    private int add(LongBlock.Builder builder, int[] seen, int nextSeen, int value) {
        /*
         * Check if we've seen the value before. This is n^2 on the number of
         * values, but we don't expect many of them in each entry.
         */
        for (int j = 0; j < nextSeen; j++) {
            if (seen[j] == value) {
                return nextSeen;
            }
        }
        seen[nextSeen] = value;
        builder.appendLong(hashOrdToGroup(longHash.add(value)));
        return nextSeen + 1;
    }

    @Override
    public IntBlock[] getKeys() {
        final int size = Math.toIntExact(longHash.size());
        final int[] keys = new int[size];
        for (int i = 0; i < size; i++) {
            keys[i] = (int) longHash.get(i);
        }
        return new IntBlock[] { new IntArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(longHash.size()));
    }

    @Override
    public void close() {
        longHash.close();
    }

    @Override
    public String toString() {
        return "IntBlockHash{channel=" + channel + ", entries=" + longHash.size() + '}';
    }
}
