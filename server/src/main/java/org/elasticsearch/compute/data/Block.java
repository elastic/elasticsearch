/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.Experimental;

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

    /**
     * @param positionCount the number of values in this block
     */
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

    /**
     * Retrieves the value stored at the given position as a BytesRef.
     *
     * @param position the position
     * @param spare    the spare BytesRef that can be used as a temporary buffer during retrieving
     * @return the data value (as a BytesRef)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    public BytesRef getBytesRef(int position, BytesRef spare) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Retrieves the value stored at the given position.
     *
     * @param position the position
     * @return the data value
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    public Object getObject(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Returns true if the value stored at the given position is null, false otherwise.
     *
     * @param position the position
     * @return true or false
     */
    public boolean isNull(int position) {
        return false;
    }

    /**
     * @return the number of null values in this block.
     */
    public int nullValuesCount() {
        return 0;
    }

    /**
     * @return the number of non-null values in this block.
     */
    public int validPositionCount() {
        return positionCount - nullValuesCount();
    }

    /**
     * @return true if some values might be null. False, if all values are guaranteed to be not null.
     */
    public boolean mayHaveNulls() {
        return false;
    }

    /**
     * @return true if all values in this block are guaranteed to be null.
     */
    public boolean areAllValuesNull() {
        return false;
    }

    protected final boolean assertPosition(int position) {
        assert (position >= 0 || position < getPositionCount())
            : "illegal position, " + position + ", position count:" + getPositionCount();
        return true;
    }

    @Experimental
    // TODO: improve implementation not to waste as much space
    public Block getRow(int position) {
        return filter(position);
    }

    /**
     * Creates a new block that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered block
     */
    public Block filter(int... positions) {
        return new FilteredBlock(this, positions);
    }
}
