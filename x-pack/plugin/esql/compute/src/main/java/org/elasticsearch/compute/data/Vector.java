/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;

/**
 * A dense Vector of single values.
 */
public interface Vector extends Accountable, RefCounted, Releasable {

    /**
     * {@return Returns a new Block containing this vector.}
     */
    Block asBlock();

    /**
     * The number of positions in this vector.
     *
     * @return the number of positions
     */
    int getPositionCount();

    /**
     * Creates a new vector that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered vector
     */
    Vector filter(int... positions);

    /**
     * Build a {@link Block} the same values as this {@link Vector}, but replacing
     * all values for which {@code mask.getBooleanValue(position)} returns
     * {@code false} with {@code null}. The {@code mask} vector must be at least
     * as long as this {@linkplain Vector}.
     */
    Block keepMask(BooleanVector mask);

    /**
     * Builds an Iterator of new {@link Block}s with the same {@link #elementType}
     * as this {@link Vector} whose values are copied from positions in this Vector.
     * It has the same number of {@link #getPositionCount() positions} as the
     * {@code positions} parameter.
     * <p>
     *     For example, if this vector contained {@code [a, b, c]}
     *     and were called with the block {@code [0, 1, 1, [1, 2]]} then the
     *     result would be {@code [a, b, b, [b, c]]}.
     * </p>
     * <p>
     *     This process produces {@code count(positions)} values per
     *     positions which could be quite large. Instead of returning a single
     *     Block, this returns an Iterator of Blocks containing all of the promised
     *     values.
     * </p>
     * <p>
     *     The returned {@link ReleasableIterator} may retain a reference to the
     *     {@code positions} parameter. Close it to release those references.
     * </p>
     * <p>
     *     This block is built using the same {@link BlockFactory} as was used to
     *     build the {@code positions} parameter.
     * </p>
     */
    ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * {@return the element type of this vector}
     */
    ElementType elementType();

    /**
     * {@return true iff this vector is a constant vector - returns the same constant value for every position}
     */
    boolean isConstant();

    /** The block factory associated with this vector. */
    // TODO: Renaming this to owningBlockFactory
    BlockFactory blockFactory();

    /**
     * Before passing a Vector to another Driver, it is necessary to switch the owning block factory to its parent, which is associated
     * with the global circuit breaker. This ensures that when the new driver releases this Vector, it returns memory directly to the
     * parent block factory instead of the local block factory of this Block. This is important because the local block factory is
     * not thread safe and doesn't support simultaneous access by more than one thread.
     */
    void allowPassingToDifferentDriver();

    /**
     * Builds {@link Vector}s. Typically, you use one of it's direct supinterfaces like {@link IntVector.Builder}.
     * This is {@link Releasable} and should be released after building the vector or if building the vector fails.
     */
    interface Builder extends Releasable {
        /**
         * An estimate of the number of bytes the {@link Vector} created by
         * {@link #build} will use. This may overestimate the size but shouldn't
         * underestimate it.
         */
        long estimatedBytes();

        /**
         * Builds the block. This method can be called multiple times.
         */
        Vector build();
    }

    /**
     * Whether this vector was released
     */
    boolean isReleased();

    /**
     * The serialization type of vectors: 0 and 1 replaces the boolean false/true in pre-8.14.
     */
    byte SERIALIZE_VECTOR_VALUES = 0;
    byte SERIALIZE_VECTOR_CONSTANT = 1;
    byte SERIALIZE_VECTOR_ARRAY = 2;
    byte SERIALIZE_VECTOR_BIG_ARRAY = 3;
    byte SERIALIZE_VECTOR_ORDINAL = 4;
}
