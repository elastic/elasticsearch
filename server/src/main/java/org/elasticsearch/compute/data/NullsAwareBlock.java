/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Nullable;

import java.util.BitSet;

/**
 * Base class for blocks that use a BitSet to mask some positions as null.
 */
public abstract class NullsAwareBlock extends Block {
    @Nullable
    protected final BitSet nullsMask;

    /**
     * @param positionCount the number of values in this block
     * @param nullsMask     a {@link BitSet} indicating which values of this block are null (a set bit value
     *                      represents a null value). A null nullsMask indicates this block cannot have null values.
     */
    public NullsAwareBlock(int positionCount, BitSet nullsMask) {
        super(positionCount);
        this.nullsMask = nullsMask == null || nullsMask.isEmpty() ? null : nullsMask;
    }

    public NullsAwareBlock(int positionCount) {
        this(positionCount, null);
    }

    @Override
    public final boolean isNull(int position) {
        return mayHaveNulls() && nullsMask.get(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return nullsMask != null;
    }

    @Override
    public int nullValuesCount() {
        return mayHaveNulls() ? nullsMask.cardinality() : 0;
    }

    @Override
    public boolean areAllValuesNull() {
        return nullValuesCount() == getPositionCount();
    }
}
