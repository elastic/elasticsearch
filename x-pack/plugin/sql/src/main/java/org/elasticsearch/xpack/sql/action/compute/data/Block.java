/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.data;

/**
 * A block has a simple columnar data representation.
 * It has a position (row) count, and various methods
 * for accessing the data that's stored at a given position in the block.
 */
public class Block {

    private final int positionCount;

    public Block(int positionCount) {
        this.positionCount = positionCount;
    }

    /**
     * Returns the number of positions in this block
     */
    public int getPositionCount() {
        return positionCount;
    }

    /**
     * Retrieves the integer value stored at the given position
     */
    public int getInt(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Retrieves the long value stored at the given position
     */
    public long getLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
