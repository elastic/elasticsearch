/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Block view of a BytesRefVector.
 * This class is generated. Do not edit it.
 */
public final class BytesRefVectorBlock extends AbstractVectorBlock implements BytesRefBlock {

    private final BytesRefVector vector;

    BytesRefVectorBlock(BytesRefVector vector) {
        super(vector.getPositionCount());
        this.vector = vector;
    }

    @Override
    public BytesRefVector asVector() {
        return vector;
    }

    @Override
    public BytesRef getBytesRef(int valueIndex, BytesRef dest) {
        return vector.getBytesRef(valueIndex, dest);
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
    public BytesRefBlock filter(int... positions) {
        return new FilterBytesRefVector(vector, positions).asBlock();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "BytesRefVectorBlock",
        BytesRefVectorBlock::of
    );

    @Override
    public String getWriteableName() {
        return "BytesRefVectorBlock";
    }

    static BytesRefVectorBlock of(StreamInput in) throws IOException {
        final int positions = in.readVInt();
        final boolean constant = in.readBoolean();
        if (constant && positions > 0) {
            return new BytesRefVectorBlock(new ConstantBytesRefVector(in.readBytesRef(), positions));
        } else {
            var builder = BytesRefVector.newVectorBuilder(positions);
            for (int i = 0; i < positions; i++) {
                builder.appendBytesRef(in.readBytesRef());
            }
            return new BytesRefVectorBlock(builder.build());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        final BytesRefVector vector = this.vector;
        final int positions = vector.getPositionCount();
        out.writeVInt(positions);
        out.writeBoolean(vector.isConstant());
        if (vector.isConstant() && positions > 0) {
            out.writeBytesRef(getBytesRef(0, new BytesRef()));
        } else {
            for (int i = 0; i < positions; i++) {
                out.writeBytesRef(getBytesRef(i, new BytesRef()));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BytesRefBlock that) {
            return BytesRefBlock.equals(this, that);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return BytesRefBlock.hash(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[vector=" + vector + "]";
    }
}
