/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Block view of a LongVector.
 * This class is generated. Do not edit it.
 */
public final class LongVectorBlock extends AbstractVectorBlock implements LongBlock {

    private final LongVector vector;

    LongVectorBlock(LongVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public LongVector asVector() {
        return vector;
    }

    @Override
    public long getLong(int valueIndex) {
        return vector.getLong(valueIndex);
    }

    @Override
    public int getTotalValueCount() {
        return vector.getPositionCount();
    }

    @Override
    public ElementType elementType() {
        return vector.elementType();
    }

    @Override
    public LongBlock filter(int... positions) {
        return new FilterLongVector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "LongVectorBlock",
        LongVectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "LongVectorBlock";
    }

    static LongVectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new LongVectorBlock(new ConstantLongVector(in.readLong(), positions));
        } else {
            var builder = LongVector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.appendLong(in.readLong());
            }
            return new LongVectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final LongVector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
            out.writeLong(getLong(0));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeLong(getLong(i));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongBlock that) {
            return LongBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return LongBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
