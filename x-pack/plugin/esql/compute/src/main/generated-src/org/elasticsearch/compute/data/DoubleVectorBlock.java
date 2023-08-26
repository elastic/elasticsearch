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
 * Block view of a DoubleVector.
 * This class is generated. Do not edit it.
 */
public final class DoubleVectorBlock extends AbstractVectorBlock implements DoubleBlock {

    private final DoubleVector vector;

    DoubleVectorBlock(DoubleVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public DoubleVector asVector() {
        return vector;
    }

    @Override
    public double getDouble(int valueIndex) {
        return vector.getDouble(valueIndex);
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
    public DoubleBlock filter(int... positions) {
        return new FilterDoubleVector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "DoubleVectorBlock",
        DoubleVectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "DoubleVectorBlock";
    }

    static DoubleVectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new DoubleVectorBlock(new ConstantDoubleVector(in.readDouble(), positions));
        } else {
            var builder = DoubleVector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.appendDouble(in.readDouble());
            }
            return new DoubleVectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final DoubleVector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
            out.writeDouble(getDouble(0));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeDouble(getDouble(i));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DoubleBlock that) {
            return DoubleBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return DoubleBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
