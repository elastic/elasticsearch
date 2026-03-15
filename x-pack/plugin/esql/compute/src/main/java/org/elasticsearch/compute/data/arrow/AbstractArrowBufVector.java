/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedWidthVector;
import org.elasticsearch.compute.data.AbstractNonThreadSafeRefCounted;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Vector;

public abstract class AbstractArrowBufVector<V extends Vector, B extends Block> extends AbstractNonThreadSafeRefCounted implements Vector {

    protected final ArrowBuf valueBuffer;
    protected final int positionCount;
    protected final BlockFactory blockFactory;
    protected boolean closed = false;

    /**
     * Create an ArrowBuf ES|QL vector based on the constituents of an Arrow <code>ValueVector</code>. The caller must retain the buffer
     * if it's shared with other blocks or Arrow vectors.
     */
    protected AbstractArrowBufVector(ArrowBuf valueBuffer, int positionCount, BlockFactory blockFactory) {
        this.valueBuffer = valueBuffer;
        this.positionCount = positionCount;
        this.blockFactory = blockFactory;
    }

    @SuppressWarnings("this-escape")
    protected AbstractArrowBufVector(FixedWidthVector arrowVector, BlockFactory blockFactory) {
        this(arrowVector.getDataBuffer(), arrowVector.getValueCount(), blockFactory);

        if (arrowVector.getNullCount() > 0) {
            throw new IllegalArgumentException("Expecting no nulls in " + arrowVector.getClass().getSimpleName());
        }

        ArrowUtils.checkItemSize(arrowVector, byteSize());
        valueBuffer.getReferenceManager().retain();
    }

    /** Retains (increments the reference count) this vector's Arrow buffers */
    public void retainBuffers() {
        this.valueBuffer.getReferenceManager().retain();
    }

    /** Releases (decrements the reference count) this vector's Arrow buffers */
    public void releaseBuffers() {
        this.valueBuffer.getReferenceManager().release();
    }

    protected abstract int byteSize();

    protected abstract ArrowBufVectorConstructor<V> vectorConstructor();

    protected abstract ArrowBufBlockConstructor<B> blockConstructor();

    @Override
    protected void closeInternal() {
        if (closed) {
            return;
        }
        closed = true;
        releaseBuffers();
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
        // Don't retain the buffer, as this takes ownership of the vector.
        return blockConstructor().create(valueBuffer, null, null, positionCount, 0, blockFactory);
    }

    @Override
    @SuppressWarnings("unchecked")
    public B keepMask(BooleanVector mask) {

        // Looks redundant with AbstractArrowBufBlock.keepMask, but we must retain buffers
        // instead of incRef() that happens there as the result of `asBlock` is transient.
        if (getPositionCount() == 0) {
            retainBuffers();
            return asBlock();
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                retainBuffers();
                return asBlock();
            }
            return (B) blockFactory.newConstantNullBlock(getPositionCount());
        }

        return (B) asBlock().keepMask(mask);
    }

    @Override
    public long ramBytesUsed() {
        return valueBuffer.getActualMemoryConsumed();
    }

    @Override
    public V filter(boolean mayContainDuplicates, int... positions) {
        final var allocator = blockFactory.arrowAllocator();
        final int size = byteSize();
        final var buffer = allocator.buffer((long) positions.length * size);
        for (int pos : positions) {
            buffer.setBytes((long) pos * size, valueBuffer, (long) pos * size, size);
        }
        return vectorConstructor().create(buffer, positions.length, blockFactory);
    }

    // TODO
    // - implement writeTo more efficiently (we can piggyback on SERIALIZE_VECTOR_BIG_ARRAY)
}
