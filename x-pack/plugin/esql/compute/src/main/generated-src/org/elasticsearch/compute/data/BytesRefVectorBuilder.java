/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

/**
 * Builder for {@link BytesRefVector}s that grows as needed.
 * This class is generated. Edit {@code X-VectorBuilder.java.st} instead.
 */
final class BytesRefVectorBuilder extends AbstractVectorBuilder implements BytesRefVector.Builder {

    private BytesRefArray values;

    BytesRefVectorBuilder(int estimatedSize, BlockFactory blockFactory) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE, blockFactory);
    }

    BytesRefVectorBuilder(int estimatedSize, BigArrays bigArrays, BlockFactory blockFactory) {
        super(blockFactory);
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

    @Override
    public BytesRefVectorBuilder appendBytesRef(BytesRef value) {
        ensureCapacity();
        values.append(value);
        valueCount++;
        return this;
    }

    @Override
    protected int elementSize() {
        return -1;
    }

    @Override
    protected int valuesLength() {
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public BytesRefVector build() {
        finish();
        BytesRefVector vector;
        assert estimatedBytes == 0;
        if (valueCount == 1) {
            vector = new ConstantBytesRefVector(BytesRef.deepCopyOf(values.get(0, new BytesRef())), 1, blockFactory);
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(vector.ramBytesUsed());
            Releasables.closeExpectNoException(values);
        } else {
            vector = new BytesRefArrayVector(values, valueCount, blockFactory);
            /*
             * Update the breaker with the actual bytes used.
             * We pass false below even though we've used the bytes. That's weird,
             * but if we break here we will throw away the used memory, letting
             * it be deallocated. The exception will bubble up and the builder will
             * still technically be open, meaning the calling code should close it
             * which will return all used memory to the breaker.
             */
            blockFactory.adjustBreaker(vector.ramBytesUsed() - values.bigArraysRamBytesUsed());
        }
        values = null;
        built();
        return vector;
    }

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
