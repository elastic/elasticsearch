/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasables;

/**
 * Grouping state base class for "all first/last by int key" aggregators.
 * Extends {@link AbstractAllByKeyGroupingState} with an {@code int}-typed key array.
 */
public abstract class AbstractAllByIntGroupingState extends AbstractAllByKeyGroupingState {

    /**
     * The group-indexed keys
     */
    private IntArray keys;

    protected AbstractAllByIntGroupingState(BigArrays bigArrays) {
        super(bigArrays);
        boolean success = false;
        try {
            keys = bigArrays.newIntArray(1, false);
            keys.set(0, -1);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(keys, super::close);
            }
        }
    }

    protected int key(int group) {
        return keys.get(group);
    }

    protected void key(int group, int value) {
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
