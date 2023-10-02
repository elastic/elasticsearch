/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Releasable;

/**
 * A dense Vector of single values.
 */
public interface Vector extends Accountable, Releasable {

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

    // TODO: improve implementation not to waste as much space
    Vector getRow(int position);

    /**
     * Creates a new vector that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered vector
     */
    Vector filter(int... positions);

    /**
     * {@return the element type of this vector}
     */
    ElementType elementType();

    /**
     * {@return true iff this vector is a constant vector - returns the same constant value for every position}
     */
    boolean isConstant();

    /** The block factory associated with this vector. */
    BlockFactory blockFactory();

    /**
     * Builds {@link Vector}s. Typically, you use one of it's direct supinterfaces like {@link IntVector.Builder}.
     * This is {@link Releasable} and should be released after building the vector or if building the vector fails.
     */
    interface Builder extends Releasable {
        /**
         * Builds the block. This method can be called multiple times.
         */
        Vector build();
    }
}
