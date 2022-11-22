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
import org.elasticsearch.core.Nullable;

import java.util.BitSet;

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
    @Nullable
    final BitSet nullsMask;

    protected Block(int positionCount) {
        this(positionCount, new BitSet(positionCount));
    }

    /**
     * @param positionCount the number of values in this block
     * @param nullsMask a {@link BitSet} indicating which values of this block are null (a set bit value
     *                  represents a null value). A null nullsMask indicates this block cannot have null values.
     */
    protected Block(int positionCount, BitSet nullsMask) {
        assert positionCount >= 0;
        this.positionCount = positionCount;
        this.nullsMask = nullsMask;
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
    public final boolean isNull(int position) {
        return mayHaveNull() && nullsMask.get(position);
    }

    /**
     * @return false if all values of this block are not null, true otherwise.
     */
    public boolean mayHaveNull() {
        return nullsMask != null;
    }

    /**
     * @return the number of null values in this block.
     */
    public int nullValuesCount() {
        return mayHaveNull() ? nullsMask.cardinality() : 0;
    }

    /**
     * @return the number of non-null values in this block.
     */
    public int validPositionCount() {
        return positionCount - nullValuesCount();
    }

    /**
     * @return true if all values in this block are null.
     */
    public boolean areAllValuesNull() {
        return mayHaveNull() ? nullsMask.cardinality() == positionCount : false;
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
