/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.AbstractNonThreadSafeRefCounted;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.ReleasableIterator;

public abstract class AbstractArrowBufVector<V extends Vector, B extends Block> extends AbstractNonThreadSafeRefCounted implements Vector {

    protected final ArrowBuf valueBuffer;
    protected final int positionCount;
    protected final BlockFactory blockFactory;

    /**
     *  Create an ArrowBuf block based on the constituents of an Arrow ValueVector. It does not take ownership of buffers but rather
     *  increases their reference count. This means that callers must release the buffers (and decrease their reference counters)
     *  if they don't need them anymore.
     */
    protected AbstractArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        valueBuffer.getReferenceManager().retain();
        this.valueBuffer = valueBuffer;
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
    }

    protected abstract int byteSize();

    protected abstract ArrowBufVectorConstructor<V> vectorConstructor();

    protected abstract ArrowBufBlockConstructor<B> blockConstructor();

    @Override
    protected void closeInternal() {
        this.valueBuffer.getReferenceManager().release();
    }

    @Override
    public int getPositionCount() {
        return this.positionCount;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        // FIXME: does this apply to Arrow buffers?
    }

    @Override
    public B asBlock() {
        return blockConstructor().create(valueBuffer, null, null, positionCount, 0, blockFactory);
    }

    @Override
    public B keepMask(BooleanVector mask) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public ReleasableIterator<? extends B> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        return valueBuffer.getActualMemoryConsumed();
    }

    @Override
    public V filter(boolean mayContainDuplicates, int... positions) {
        final var allocator = valueBuffer.getReferenceManager().getAllocator();
        final int size = byteSize();
        final var buffer = allocator.buffer((long) positions.length * size);
        for (int pos : positions) {
            buffer.setBytes((long) pos * size, valueBuffer, (long) pos * size, size);
        }
        try {
            return vectorConstructor().create(buffer, positions.length, blockFactory);
        } finally {
            buffer.getReferenceManager().release();
        }
    }

    // TODO
    // - implement writeTo more efficiently (we can piggyback on SERIALIZE_VECTOR_BIG_ARRAY)
}
