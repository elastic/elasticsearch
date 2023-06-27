/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.MultivalueDedupeLong;

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
        return new MultivalueDedupeLong(block).hash(longHash);
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
