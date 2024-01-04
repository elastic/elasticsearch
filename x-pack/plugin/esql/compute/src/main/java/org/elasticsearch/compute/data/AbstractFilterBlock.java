/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

abstract class AbstractFilterBlock extends AbstractBlock implements Block {

    // TODO: positions need to be tracked and released from the breaker on closeInternal
    protected final int[] positions;

    AbstractFilterBlock(int[] positions, BlockFactory blockFactory) {
        // TODO: assert positions here, also check that we map to valid positions
        super(positions.length, blockFactory);
        this.positions = positions;
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

    protected int mapPosition(int position) {
        assert assertPosition(position);
        return positions[position];
    }


    protected final boolean assertPosition(int position) {
        assert (position >= 0 || position < getPositionCount())
            : "illegal position, " + position + ", position count:" + getPositionCount();
        return true;
    }
}
