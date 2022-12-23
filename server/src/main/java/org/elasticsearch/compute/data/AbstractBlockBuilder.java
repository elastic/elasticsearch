/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.BitSet;
import java.util.stream.IntStream;

abstract class AbstractBlockBuilder implements BlockBuilder {

    protected int[] firstValueIndexes; // lazily initialized, if multi-values

    protected BitSet nullsMask; // lazily initialized, if sparse

    protected int valueCount;

    protected int positionCount;

    protected boolean positionEntryIsOpen;

    protected boolean hasNonNullValue;

    protected AbstractBlockBuilder() {}

    @Override
    public BlockBuilder appendInt(int value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public BlockBuilder appendLong(long value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public BlockBuilder appendDouble(double value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public BlockBuilder appendBytesRef(BytesRef value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public final BlockBuilder appendNull() {
        ensureCapacity();
        if (nullsMask == null) {
            nullsMask = new BitSet();
        }
        nullsMask.set(valueCount);
        writeNullValue();
        valueCount++;
        updatePosition();
        return this;
    }

    protected void writeNullValue() {} // default is a no-op for array backed builders - since they have default value.

    /** The length of the internal values array. */
    protected abstract int valuesLength();

    @Override
    public final BlockBuilder beginPositionEntry() {
        if (firstValueIndexes == null) {
            firstValueIndexes = new int[valuesLength()];
            IntStream.range(0, positionCount).forEach(i -> firstValueIndexes[i] = i);
        }
        positionEntryIsOpen = true;
        firstValueIndexes[positionCount] = valueCount;
        return this;
    }

    @Override
    public final BlockBuilder endPositionEntry() {
        positionCount++;
        positionEntryIsOpen = false;
        return this;
    }

    protected final boolean isDense() {
        return nullsMask == null;
    }

    protected final boolean singleValued() {
        return firstValueIndexes == null;
    }

    protected final void updatePosition() {
        if (firstValueIndexes == null) {
            positionCount++;
        }
    }

    protected abstract void growValuesArray(int newSize);

    protected final void ensureCapacity() {
        int valuesLength = valuesLength();
        if (valueCount < valuesLength) {
            return;
        }
        int newSize = calculateNewArraySize(valuesLength);
        growValuesArray(newSize);
    }

    static int calculateNewArraySize(int currentSize) {
        // trivially, grows array by 50%
        return currentSize + (currentSize >> 1);
    }
}
