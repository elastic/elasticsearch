/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;

/**
 * Grouping state base class for "all first/last by long key" aggregators.
 * Extends {@link AbstractAllByKeyGroupingState} with a {@code long}-typed key array.
 */
public abstract class AbstractAllByLongGroupingState extends AbstractAllByKeyGroupingState {

    /**
     * The group-indexed keys
     */
    private LongArray keys;

    protected AbstractAllByLongGroupingState(BigArrays bigArrays) {
        super(bigArrays);
        boolean success = false;
        try {
            keys = bigArrays.newLongArray(1, false);
            keys.set(0, -1L);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(keys, super::close);
            }
        }
    }

    protected long key(int group) {
        return keys.get(group);
    }

    protected void key(int group, long value) {
        keys.set(group, value);
    }

    @Override
    protected void grow(int group) {
        keys = bigArrays.grow(keys, group + 1);
    }

    @Override
    public void close() {
        Releasables.close(keys, super::close);
    }
}
