/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

/**
 * A Block is a columnar data representation. It has a position (row) count, and various data
 * retrieval methods for accessing the underlying data that is stored at a given position.
 *
 * <p> All Blocks share the same set of data retrieval methods, but actual concrete implementations
 * effectively support a subset of these, throwing {@code UnsupportedOperationException} where a
 * particular data retrieval method is not supported. For example, a Block of primitive longs may
 * not support retrieval as an integer, {code getInt}. This greatly simplifies Block usage and
 * avoids cumbersome use-site casting.
 *
 * <p> Block are immutable and can be passed between threads.
 */
public abstract class Block {

    private final int positionCount;

    protected Block(int positionCount) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
    }

    /**
     * The number of positions in this block.
     *
     * @return the number of positions
     */
    public final int getPositionCount() {
        return positionCount;
    }

    /**
     * Retrieves the integer value stored at the given position.
     *
     * @param position the position
     * @return the data value (as an int)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    public int getInt(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Retrieves the long value stored at the given position, widening if necessary.
     *
     * @param position the position
     * @return the data value (as a long)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    public long getLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Retrieves the value stored at the given position as a double, widening if necessary.
     *
     * @param position the position
     * @return the data value (as a double)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    public double getDouble(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    protected final boolean assertPosition(int position) {
        assert (position >= 0 || position < getPositionCount())
            : "illegal position, " + position + ", position count:" + getPositionCount();
        return true;
    }

    // TODO: improve implementation not to waste as much space
    public Block getRow(int position) {
        Block curr = this;
        return new Block(1) {
            @Override
            public int getInt(int ignored) {
                return curr.getInt(position);
            }

            @Override
            public long getLong(int ignored) {
                return curr.getLong(position);
            }

            @Override
            public double getDouble(int ignored) {
                return curr.getDouble(position);
            }

            @Override
            public String toString() {
                return "only-position " + position + ": " + curr;
            }
        };
    }
}
