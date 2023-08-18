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

/**
 * Block build of BytesRefBlocks.
 * This class is generated. Do not edit it.
 */
final class BytesRefVectorBuilder extends AbstractVectorBuilder implements BytesRefVector.Builder {

    private BytesRefArray values;

    BytesRefVectorBuilder(int estimatedSize) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    BytesRefVectorBuilder(int estimatedSize, BigArrays bigArrays) {
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
    protected int valuesLength() {
        return Integer.MAX_VALUE; // allow the BytesRefArray through its own append
    }

    @Override
    protected void growValuesArray(int newSize) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public BytesRefVector build() {
        if (valueCount == 1) {
            return new ConstantBytesRefVector(values.get(0, new BytesRef()), 1);
        }
        return new BytesRefArrayVector(values, valueCount);
    }
}
