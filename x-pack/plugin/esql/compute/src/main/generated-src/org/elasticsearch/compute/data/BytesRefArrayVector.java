/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;

/**
 * Vector implementation that stores an array of BytesRef values.
 * This class is generated. Do not edit it.
 */
public final class BytesRefArrayVector extends AbstractVector implements BytesRefVector {

    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefArrayVector.class);

    private final BytesRefArray values;

    private final BytesRefBlock block;

    public BytesRefArrayVector(BytesRefArray values, int positionCount) {
        this(values, positionCount, BlockFactory.getNonBreakingInstance());
    }

    public BytesRefArrayVector(BytesRefArray values, int positionCount, BlockFactory blockFactory) {
        super(positionCount, blockFactory);
        this.values = values;
        this.block = new BytesRefVectorBlock(this);
    }

    @Override
    public BytesRefBlock asBlock() {
        return block;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef dest) {
        return values.get(position, dest);
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public BytesRefVector filter(int... positions) {
        final var scratch = new BytesRef();
        try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(positions.length)) {
            for (int pos : positions) {
                builder.appendBytesRef(values.get(pos, scratch));
            }
            return builder.build();
        }
    }

    public static long ramBytesEstimated(BytesRefArray values) {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesEstimated(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefVector that) {
            return BytesRefVector.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefVector.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[positions=" + getPositionCount() + ']';
    }

    @Override
    public void close() {
        if (released) {
            throw new IllegalStateException("can't release already released vector [" + this + "]");
        }
        released = true;
        blockFactory.adjustBreaker(-ramBytesUsed() + values.bigArraysRamBytesUsed(), true);
        Releasables.closeExpectNoException(values);
    }
}
