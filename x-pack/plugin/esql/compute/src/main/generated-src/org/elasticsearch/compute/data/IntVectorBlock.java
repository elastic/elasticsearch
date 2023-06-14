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
 * Block view of a IntVector.
 * This class is generated. Do not edit it.
 */
public final class IntVectorBlock extends AbstractVectorBlock implements IntBlock {

    private final IntVector vector;

    IntVectorBlock(IntVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public IntVector asVector() {
        return vector;
    }

    @Override
    public int getInt(int valueIndex) {
        return vector.getInt(valueIndex);
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
    public IntBlock filter(int... positions) {
        return new FilterIntVector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "IntVectorBlock",
        IntVectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "IntVectorBlock";
    }

    static IntVectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new IntVectorBlock(new ConstantIntVector(in.readInt(), positions));
        } else {
            var builder = IntVector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.appendInt(in.readInt());
            }
            return new IntVectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final IntVector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
            out.writeInt(getInt(0));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeInt(getInt(i));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntBlock that) {
            return IntBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return IntBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
