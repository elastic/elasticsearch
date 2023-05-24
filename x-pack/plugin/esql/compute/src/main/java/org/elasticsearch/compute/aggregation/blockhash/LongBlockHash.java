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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;

final class LongBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    LongBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        LongBlock block = page.getBlock(channel);
        LongVector vector = block.asVector();
        if (vector == null) {
            return add(block);
        }
        return add(vector).asBlock();
    }

    private LongVector add(LongVector vector) {
        long[] groups = new long[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = hashOrdToGroup(longHash.add(vector.getLong(i)));
        }
        return new LongArrayVector(groups, groups.length);
    }

    private static final long[] EMPTY = new long[0];

    private LongBlock add(LongBlock block) {
        long[] seen = EMPTY;
        LongBlock.Builder builder = LongBlock.newBlockBuilder(block.getTotalValueCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                builder.appendNull();
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int count = block.getValueCount(p);
            if (count == 1) {
                builder.appendLong(hashOrdToGroup(longHash.add(block.getLong(start))));
                continue;
            }
            if (seen.length < count) {
                seen = new long[ArrayUtil.oversize(count, Long.BYTES)];
            }
            builder.beginPositionEntry();
            // TODO if we know the elements were in sorted order we wouldn't need an array at all.
            // TODO we could also have an assertion that there aren't any duplicates on the block.
            // Lucene has them in ascending order without duplicates
            int end = start + count;
            int nextSeen = 0;
            for (int offset = start; offset < end; offset++) {
                nextSeen = add(builder, seen, nextSeen, block.getLong(offset));
            }
            builder.endPositionEntry();
        }
        return builder.build();
    }

    private int add(LongBlock.Builder builder, long[] seen, int nextSeen, long value) {
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
    public LongBlock[] getKeys() {
        final int size = Math.toIntExact(longHash.size());
        final long[] keys = new long[size];
        for (int i = 0; i < size; i++) {
            keys[i] = longHash.get(i);
        }

        // TODO call something like takeKeyOwnership to claim the keys array directly
        return new LongBlock[] { new LongArrayVector(keys, keys.length).asBlock() };
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
        return "LongBlockHash{channel=" + channel + ", entries=" + longHash.size() + '}';
    }
}
