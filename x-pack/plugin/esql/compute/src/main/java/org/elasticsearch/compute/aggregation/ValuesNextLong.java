/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Helper for the {@link ValuesIntAggregator}, {@link ValuesFloatAggregator} and {@link ValuesBytesRefAggregator}
 * collecting values after the first in each group are collected in a hash, keyed
 * by the pair of groupId and value.
 */
public class ValuesNextLong implements Releasable {
    private final LongHashTable hashes;

    public ValuesNextLong(BlockFactory blockFactory) {
        this.hashes = HashImplFactory.newLongHash(blockFactory);
    }

    void add(int groupId, int v) {
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        hashes.add((((long) groupId) << Integer.SIZE) | (v & 0xFFFFFFFFL));
    }

    void add(int groupId, float v) {
        add(groupId, Float.floatToIntBits(v));
    }

    int getInt(ValuesNextPreparedForEmitting prepared, int index) {
        long both = hashes.get(prepared.ids[index]);
        return (int) (both & 0xFFFFFFFFL);
    }

    float getFloat(ValuesNextPreparedForEmitting prepared, int index) {
        return Float.intBitsToFloat(getInt(prepared, index));
    }

    ValuesNextPreparedForEmitting prepareForEmitting(BlockFactory blockFactory, IntVector selected) {
        return ValuesNextPreparedForEmitting.build(blockFactory, selected, hashes);
    }

    long size() {
        return hashes.size();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hashes);
    }
}
