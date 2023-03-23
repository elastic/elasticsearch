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
 * Block view of a BooleanVector.
 * This class is generated. Do not edit it.
 */
public final class BooleanVectorBlock extends AbstractVectorBlock implements BooleanBlock {

    private final BooleanVector vector;

    BooleanVectorBlock(BooleanVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public BooleanVector asVector() {
        return vector;
    }

    @Override
    public boolean getBoolean(int valueIndex) {
        return vector.getBoolean(valueIndex);
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
    public BooleanBlock filter(int... positions) {
        return new FilterBooleanVector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "BooleanVectorBlock",
        BooleanVectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "BooleanVectorBlock";
    }

    static BooleanVectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new BooleanVectorBlock(new ConstantBooleanVector(in.readBoolean(), positions));
        } else {
            var builder = BooleanVector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(in.readBoolean());
            }
            return new BooleanVectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final BooleanVector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
            out.writeBoolean(getBoolean(0));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeBoolean(getBoolean(i));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BooleanBlock that) {
            return BooleanBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BooleanBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
