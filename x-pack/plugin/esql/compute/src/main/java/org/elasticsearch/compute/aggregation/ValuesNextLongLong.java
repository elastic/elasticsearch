/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Helper for the {@link ValuesLongAggregator} and {@link ValuesDoubleAggregator}
 * collecting values after the first in each group are collected in a hash, keyed
 * by the pair of groupId and value.
 */
public class ValuesNextLongLong implements Releasable {
    private final BlockFactory blockFactory;
    private final LongLongHash hashes;

    public ValuesNextLongLong(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        this.hashes = new LongLongHash(1, blockFactory.bigArrays());
    }

    void add(int groupId, long v) {
        hashes.add(groupId, v);
    }

    void add(int groupId, double v) {
        add(groupId, Double.doubleToLongBits(v));
    }

    long getLong(ValuesNextPreparedForEmitting prepared, int index) {
        return hashes.getKey2(prepared.ids[index]);
    }

    double getDouble(ValuesNextPreparedForEmitting prepared, int index) {
        return Double.longBitsToDouble(getLong(prepared, index));
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
