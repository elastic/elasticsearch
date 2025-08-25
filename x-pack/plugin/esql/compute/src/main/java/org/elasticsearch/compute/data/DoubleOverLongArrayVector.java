/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
// end generated imports

/**
 * Vector implementation that converts long to double values on-the-fly.
 */
final class DoubleOverLongArrayVector extends AbstractVector implements DoubleVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DoubleOverLongArrayVector.class)
        // TODO: remove these extra bytes once `asBlock` returns a block with a separate reference to the vector.
        + RamUsageEstimator.shallowSizeOfInstance(DoubleVectorBlock.class)
        // TODO: remove this if/when we account for memory used by Pages
        + Block.PAGE_MEM_OVERHEAD_PER_BLOCK;

    private final long[] values;
    private final BlockUtils.ToDouble toDouble;

    DoubleOverLongArrayVector(long[] values, BlockUtils.ToDouble toDouble, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.toDouble = toDouble;
    }

    @Override
    public DoubleBlock asBlock() {
        return new DoubleVectorBlock(this);
    }

    @Override
    public double getDouble(int position) {
        return toDouble.convert(values[position]);
    }

    @Override
    public ElementType elementType() {
        return ElementType.DOUBLE;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public DoubleVector filter(int... positions) {
        try (DoubleVector.Builder builder = blockFactory().newDoubleVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendDouble(toDouble.convert((values[pos])));
            }
            return builder.build();
        }
    }

    @Override
    public DoubleBlock keepMask(BooleanVector mask) {
        if (getPositionCount() == 0) {
            incRef();
            return new DoubleVectorBlock(this);
        }
        if (mask.isConstant()) {
            if (mask.getBoolean(0)) {
                incRef();
                return new DoubleVectorBlock(this);
            }
            return (DoubleBlock) blockFactory().newConstantNullBlock(getPositionCount());
        }
        try (DoubleBlock.Builder builder = blockFactory().newDoubleBlockBuilder(getPositionCount())) {
            // TODO if X-ArrayBlock used BooleanVector for it's null mask then we could shuffle references here.
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendDouble(getDouble(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<DoubleBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new DoubleLookup(asBlock(), positions, targetBlockSize);
    }

    private static long ramBytesEstimated(long[] values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleVector that) {
            return DoubleVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleVector.hash(this);
    }

    @Override
    public String toString() {
        String valuesString = IntStream.range(0, getPositionCount())
            .limit(10)
            .mapToObj(n -> String.valueOf(toDouble.convert(values[n])))
            .collect(Collectors.joining(", ", "[", getPositionCount() > 10 ? ", ...]" : "]"));
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ", values=" + valuesString + ']';
    }

}
