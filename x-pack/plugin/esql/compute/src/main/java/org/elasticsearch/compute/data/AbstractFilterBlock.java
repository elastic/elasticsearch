/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

abstract class AbstractFilterBlock implements Block {

    protected final int[] positions;

    private final Block block;

    AbstractFilterBlock(Block block, int[] positions) {
        this.positions = positions;
        this.block = block;
    }

    @Override
    public ElementType elementType() {
        return block.elementType();
    }

    @Override
    public boolean isNull(int position) {
        return block.isNull(mapPosition(position));
    }

    @Override
    public boolean mayHaveNulls() {
        return block.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return block.areAllValuesNull();
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        /*
         * This could return a false positive. The block may have multivalued
         * fields, but we're not pointing to any of them. That's acceptable.
         */
        return block.mayHaveMultivaluedFields();
    }

    @Override
    public final int nullValuesCount() {
        if (mayHaveNulls() == false) {
            return 0;
        } else if (areAllValuesNull()) {
            return getPositionCount();
        } else {
            int nulls = 0;
            for (int i = 0; i < getPositionCount(); i++) {
                if (isNull(i)) {
                    nulls++;
                }
            }
            return nulls;
        }
    }

    @Override
    public final int getTotalValueCount() {
        if (positions.length == block.getPositionCount()) {
            // All the positions are still in the block, just jumbled.
            return block.getTotalValueCount();
        }
        // TODO this is expensive. maybe cache or something.
        int total = 0;
        for (int p = 0; p < positions.length; p++) {
            total += getValueCount(p);
        }
        return total;
    }

    @Override
    public final int getValueCount(int position) {
        return block.getValueCount(mapPosition(position));
    }

    @Override
    public final int getPositionCount() {
        return positions.length;
    }

    @Override
    public final int getFirstValueIndex(int position) {
        return block.getFirstValueIndex(mapPosition(position));
    }

    @Override
    public MvOrdering mvOrdering() {
        return block.mvOrdering();
    }

    private int mapPosition(int position) {
        assert assertPosition(position);
        return positions[position];
    }

    @Override
    public String toString() {
        return "FilteredBlock{" + "positions=" + Arrays.toString(positions) + ", block=" + block + '}';
    }

    protected final boolean assertPosition(int position) {
        assert (position >= 0 || position < getPositionCount())
            : "illegal position, " + position + ", position count:" + getPositionCount();
        return true;
    }
}
