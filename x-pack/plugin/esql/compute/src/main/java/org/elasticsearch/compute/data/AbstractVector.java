/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

/**
 * A dense Vector of single values.
 */
abstract class AbstractVector implements Vector {

    private final int positionCount;

    protected AbstractVector(int positionCount) {
        this.positionCount = positionCount;
    }

    public final int getPositionCount() {
        return positionCount;
    }

    @Override
    public final Vector getRow(int position) {
        return filter(position);
    }
}
