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

import java.util.Optional;

/**
 * A Block is a columnar representation of homogenous data. It has a position (row) count, and
 * various data retrieval methods for accessing the underlying data that is stored at a given
 * position.
 *
 * <p> Blocks can represent various shapes of underlying data. A Block can represent either sparse
 * or dense data. A Block can represent either single or multi valued data. A Block that represents
 * dense single-valued data can be viewed as a {@link Vector}.
 *
 * <p> All Blocks share the same set of data retrieval methods, but actual concrete implementations
 * effectively support a subset of these, throwing {@code UnsupportedOperationException} where a
 * particular data retrieval method is not supported. For example, a Block of primitive longs may
 * not support retrieval as an integer, {code getInt}. This greatly simplifies Block usage and
 * avoids cumbersome use-site casting.
 *
 * <p> Block are immutable and can be passed between threads.
 */
public interface Block {

    /**
     * {@return an efficient dense single-value view of this block}.
     * The optional is empty, if the block is not dense single-valued.
     * mayHaveNulls == true optional is empty, otherwise the optional is non-empty
     */
    Optional<Vector> asVector();

    /** {@return The total number of values in this block.} */
    int getTotalValueCount();

    /** {@return The number of positions in this block.} */
    int getPositionCount();

    /** Gets the index of the first value for the given position. */
    int getFirstValueIndex(int position);

    /** Gets the number of values for the given position, possibly 0. */
    int getValueCount(int position);

    /**
     * Retrieves the integer value stored at the given value index.
     *
     * <p> Values for a given position are between getFirstValueIndex(position) (inclusive) and
     * getFirstValueIndex(position) + getValueCount(position) (exclusive).
     *
     * @param valueIndex the value index
     * @return the data value (as an int)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    int getInt(int valueIndex);

    /**
     * Retrieves the long value stored at the given value index, widening if necessary.
     *
     * @param valueIndex the value index
     * @return the data value (as a long)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    long getLong(int valueIndex);

    /**
     * Retrieves the value stored at the given value index as a double, widening if necessary.
     *
     * @param valueIndex the value index
     * @return the data value (as a double)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    double getDouble(int valueIndex);

    /**
     * Retrieves the value stored at the given value index  as a BytesRef.
     *
     * @param valueIndex the value index
     * @param spare    the spare BytesRef that can be used as a temporary buffer during retrieving
     * @return the data value (as a BytesRef)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    BytesRef getBytesRef(int valueIndex, BytesRef spare);

    /**
     * Retrieves the value stored at the given value index.
     *
     * @param valueIndex the value index
     * @return the data value
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    Object getObject(int valueIndex);

    /**
     * {@return the primitive element type of this vector}
     */
    Class<?> elementType();

    /**
     * Returns true if the value stored at the given position is null, false otherwise.
     *
     * @param position the position
     * @return true or false
     */
    boolean isNull(int position);

    /**
     * @return the number of null values in this block.
     */
    int nullValuesCount();

    /**
     * @return the number of non-null values in this block.
     */
    int validPositionCount();

    /**
     * @return true if some values might be null. False, if all values are guaranteed to be not null.
     */
    boolean mayHaveNulls();

    /**
     * @return true if all values in this block are guaranteed to be null.
     */
    boolean areAllValuesNull();

    @Experimental
    // TODO: improve implementation not to waste as much space
    Block getRow(int position);

    /**
     * Creates a new block that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered block
     */
    Block filter(int... positions);
}
