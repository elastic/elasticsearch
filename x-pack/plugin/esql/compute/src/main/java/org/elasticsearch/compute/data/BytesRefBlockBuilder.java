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

final class BytesRefBlockBuilder extends AbstractBlockBuilder implements BytesRefBlock.Builder {

    private static final BytesRef NULL_VALUE = new BytesRef();

    private BytesRefArray values;

    BytesRefBlockBuilder(int estimatedSize) {
        this(estimatedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    BytesRefBlockBuilder(int estimatedSize, BigArrays bigArrays) {
        values = new BytesRefArray(Math.max(estimatedSize, 2), bigArrays);
    }

    @Override
    public BytesRefBlockBuilder appendBytesRef(BytesRef value) {
        ensureCapacity();
        values.append(value);
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
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

    public BytesRefBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public BytesRefBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public BytesRefBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    protected void writeNullValue() {
        values.append(NULL_VALUE);
    }

    @Override
    public BytesRefBlock build() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (hasNonNullValue && positionCount == 1) {
            return new ConstantBytesRefVector(values.get(0, new BytesRef()), 1).asBlock();
        } else {
            // TODO: may wanna trim the array, if there N% unused tail space
            if (isDense() && singleValued()) {
                return new BytesRefArrayVector(values, positionCount).asBlock();
            } else {
                if (firstValueIndexes != null) {
                    firstValueIndexes[positionCount] = valueCount;  // TODO remove hack
                }
                return new BytesRefArrayBlock(values, positionCount, firstValueIndexes, nullsMask);
            }
        }
    }
}
