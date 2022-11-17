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
        super(positions.length, block.nullsMask);
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

    private int mapPosition(int position) {
        assert assertPosition(position);
        return positions[position];
    }

    @Override
    public String toString() {
        return "FilteredBlock{" + "positions=" + Arrays.toString(positions) + ", block=" + block + '}';
    }
}
