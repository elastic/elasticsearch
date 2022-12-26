/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * A dense Vector of single values.
 */
public interface Vector {

    /**
     * {@return Returns a Block view over this vector.}
     */
    Block asBlock();

    /**
     * The number of positions in this vector.
     *
     * @return the number of positions
     */
    int getPositionCount();

    /**
     * Retrieves the integer value stored at the given position.
     *
     * @param position the position
     * @return the data value (as an int)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    int getInt(int position);

    /**
     * Retrieves the long value stored at the given position, widening if necessary.
     *
     * @param position the position
     * @return the data value (as a long)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    long getLong(int position);

    /**
     * Retrieves the value stored at the given position as a double, widening if necessary.
     *
     * @param position the position
     * @return the data value (as a double)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    double getDouble(int position);

    /**
     * Retrieves the value stored at the given position as a BytesRef.
     *
     * @param position the position
     * @param spare    the spare BytesRef that can be used as a temporary buffer during retrieving
     * @return the data value (as a BytesRef)
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    BytesRef getBytesRef(int position, BytesRef spare);

    /**
     * Retrieves the value stored at the given position.
     *
     * @param position the position
     * @return the data value
     * @throws UnsupportedOperationException if retrieval as this primitive data type is not supported
     */
    Object getObject(int position);

    // TODO: improve implementation not to waste as much space
    Vector getRow(int position);

    /**
     * Creates a new vector that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered vector
     */
    Vector filter(int... positions);

    /**
     * {@return the element type of this vector, unboxed if the type is a primitive}
     */
    Class<?> elementType();

    /**
     * {@return true iff this vector is a constant vector - returns the same constant value for every position}
     */
    boolean isConstant();
}
