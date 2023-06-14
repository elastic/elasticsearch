/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;

abstract class AbstractBlockBuilder implements Block.Builder {

    protected int[] firstValueIndexes; // lazily initialized, if multi-values

    protected BitSet nullsMask; // lazily initialized, if sparse

    protected int valueCount;

    protected int positionCount;

    protected boolean positionEntryIsOpen;

    protected boolean hasNonNullValue;

    protected Block.MvOrdering mvOrdering = Block.MvOrdering.UNORDERED;

    protected AbstractBlockBuilder() {}

    @Override
    public AbstractBlockBuilder appendNull() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        ensureCapacity();
        if (nullsMask == null) {
            nullsMask = new BitSet();
        }
        nullsMask.set(positionCount);
        if (firstValueIndexes != null) {
            setFirstValue(positionCount, valueCount);
        }
        positionCount++;
        writeNullValue();
        valueCount++;
        return this;
    }

    protected void writeNullValue() {} // default is a no-op for array backed builders - since they have default value.

    /** The length of the internal values array. */
    protected abstract int valuesLength();

    @Override
    public AbstractBlockBuilder beginPositionEntry() {
        if (firstValueIndexes == null) {
            firstValueIndexes = new int[positionCount + 1];
            IntStream.range(0, positionCount).forEach(i -> firstValueIndexes[i] = i);
        }
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        positionEntryIsOpen = true;
        setFirstValue(positionCount, valueCount);
        return this;
    }

    public AbstractBlockBuilder endPositionEntry() {
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
        if (positionEntryIsOpen == false) {
            if (firstValueIndexes != null) {
                setFirstValue(positionCount, valueCount - 1);
            }
            positionCount++;
        }
    }

    protected final void finish() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (firstValueIndexes != null) {
            setFirstValue(positionCount, valueCount);
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

    private void setFirstValue(int position, int value) {
        if (position >= firstValueIndexes.length) {
            firstValueIndexes = Arrays.copyOf(firstValueIndexes, position + 1);
        }
        firstValueIndexes[position] = value;
    }
}
