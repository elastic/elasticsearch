/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * TODO: Add javadoc
 */
public final class DoubleVectorVector extends AbstractVector implements Vector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DoubleVectorVector.class);

    private final DoubleVector[] doubleVectors;

    public DoubleVectorVector(DoubleVector... doubleVectors) {
        this(doubleVectors, doubleVectors.length > 0 ? doubleVectors[0].blockFactory() : null);
    }

    public DoubleVectorVector(DoubleVector[] doubleVectors, BlockFactory blockFactory) {
        super(doubleVectors.length > 0 ? doubleVectors[0].getPositionCount() : 0, blockFactory);
        this.doubleVectors = doubleVectors;
    }

    public DoubleVector[] doubleVectors() {
        return doubleVectors;
    }

    @Override
    public Block asBlock() {
        DoubleVectorBlock[] array = (DoubleVectorBlock[]) Arrays.stream(this.doubleVectors).map(DoubleVectorBlock::new).toArray();
        return new DoubleVectorVectorBlock(array);
    }

    @Override
    public DoubleVectorVector filter(int... positions) {
        DoubleVectorVector result = null;
        try {
            DoubleVector[] newVectors = new DoubleVector[this.doubleVectors.length];
            int i = 0;
            for (DoubleVector vector : this.doubleVectors) {
                vector = vector.filter(positions);
                newVectors[i] = vector;
                i++;
            }
            result = new DoubleVectorVector(newVectors);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(doubleVectors);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from DocVector");
    }

    @Override
    public ElementType elementType() {
        return ElementType.DENSE_VECTOR;
    }

    @Override
    public boolean isConstant() {
        return Arrays.stream(doubleVectors).allMatch(DoubleVector::isConstant);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(doubleVectors);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleVectorVector == false) {
            return false;
        }
        DoubleVectorVector other = (DoubleVectorVector) obj;
        return Arrays.equals(doubleVectors, other.doubleVectors);
    }

    private static long ramBytesEstimated(DoubleVector... doubleVectors) {
        return Arrays.stream(doubleVectors).map(DoubleVector::ramBytesUsed).reduce(Long::sum).orElse(0L);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(doubleVectors);
    }

    @Override
    public void allowPassingToDifferentDriver() {
        super.allowPassingToDifferentDriver();
        for (DoubleVector doubleVector : doubleVectors) {
            doubleVector.allowPassingToDifferentDriver();
        }
    }

    sealed interface Builder extends Vector.Builder permits DoubleVectorVectorBuilder {
        /**
         * Appends a double to the current entry.
         */
        DoubleVectorVector.Builder appendDoubles(double... values);

        @Override
        DoubleVectorVector build();
    }
}
