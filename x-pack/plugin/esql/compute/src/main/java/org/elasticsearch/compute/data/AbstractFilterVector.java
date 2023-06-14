/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * Wraps another vector and only allows access to positions that have not been filtered out.
 *
 * To ensure fast access, the filter is implemented as an array of positions that map positions in
 * the filtered block to positions in the wrapped vector.
 */
abstract class AbstractFilterVector extends AbstractVector {

    private final int[] positions;

    protected AbstractFilterVector(int[] positions) {
        super(positions.length);
        this.positions = positions;
    }

    protected int mapPosition(int position) {
        return positions[position];
    }
}
