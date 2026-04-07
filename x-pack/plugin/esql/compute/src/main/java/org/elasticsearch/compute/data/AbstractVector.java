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
abstract class AbstractVector extends AbstractNonThreadSafeRefCounted implements Vector {

    private final int positionCount;
    private BlockFactory blockFactory;

    protected AbstractVector(int positionCount, BlockFactory blockFactory) {
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
    }

    public final int getPositionCount() {
        return positionCount;
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        blockFactory = blockFactory.parent();
    }

    @Override
    protected void closeInternal() {
        blockFactory.adjustBreaker(-ramBytesUsed());
    }
}
