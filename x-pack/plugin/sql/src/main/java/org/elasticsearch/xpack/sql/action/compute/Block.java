/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

public class Block {

    // private final int arrayOffset;
    private final int positionCount;
    // private final boolean[] valueIsNull;
    // private final Block values;
    // private final int[] offsets;

    Block(int positionCount) {
        this.positionCount = positionCount;
    }

    public int getPositionCount() {
        return positionCount;
    }

    public int getInt(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public long getLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
