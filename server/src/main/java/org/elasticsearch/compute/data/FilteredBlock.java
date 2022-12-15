/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.Arrays;

/**
 * Wraps another block and only allows access to positions that have not been filtered out.
 *
 * To ensure fast access, the filter is implemented as an array of positions that map positions in the filtered block to positions in the
 * wrapped block.
 */
public class FilteredBlock extends Block {

    private final int[] positions;
    private final Block block;

    public FilteredBlock(Block block, int[] positions) {
        super(positions.length);
        this.positions = positions;
        this.block = block;
    }

    @Override
    public int getInt(int position) {
        return block.getInt(mapPosition(position));
    }

    @Override
    public long getLong(int position) {
        return block.getLong(mapPosition(position));
    }

    @Override
    public double getDouble(int position) {
        return block.getDouble(mapPosition(position));
    }

    @Override
    public Object getObject(int position) {
        return block.getObject(mapPosition(position));
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return block.getBytesRef(mapPosition(position), spare);
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

    private int mapPosition(int position) {
        assert assertPosition(position);
        return positions[position];
    }

    @Override
    public String toString() {
        return "FilteredBlock{" + "positions=" + Arrays.toString(positions) + ", block=" + block + '}';
    }
}
