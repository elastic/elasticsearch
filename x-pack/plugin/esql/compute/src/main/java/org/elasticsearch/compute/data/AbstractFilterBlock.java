/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

abstract class AbstractFilterBlock extends AbstractBlock {

    protected final int[] positions;

    private final Block block;

    AbstractFilterBlock(Block block, int[] positions) {
        super(positions.length);
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
    public int nullValuesCount() {
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

    protected int mapPosition(int position) {
        assert assertPosition(position);
        return positions[position];
    }

    @Override
    public String toString() {
        return "FilteredBlock{" + "positions=" + Arrays.toString(positions) + ", block=" + block + '}';
    }

}
