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
import org.elasticsearch.compute.aggregation.AggregatorState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class AggregatorStateBlock<T extends AggregatorState<T>> extends AbstractVectorBlock {

    private final AggregatorStateVector<T> vector;

    AggregatorStateBlock(AggregatorStateVector<T> vector, int positionCount) {
        super(positionCount);
        this.vector = vector;
    }

    public AggregatorStateVector<T> asVector() {
        return vector;
    }

    @Override
    public ElementType elementType() {
        return ElementType.UNKNOWN;
    } // TODO AGGS_STATE

    @Override
    public AggregatorStateBlock<T> filter(int... positions) {
        throw new UnsupportedOperationException();
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Block.class,
        "AggregatorStateBlock",
        AggregatorStateBlock::of
    );

    @Override
    public String getWriteableName() {
        return "AggregatorStateBlock";
    }

    static <T extends AggregatorState<T>> AggregatorStateBlock<T> of(StreamInput in) throws IOException {
        int positions = in.readVInt(); // verify that the positions have the same value
        byte[] ba = in.readByteArray();
        int itemSize = in.readInt();
        String description = in.readString();
        return new AggregatorStateBlock<T>(new AggregatorStateVector<>(ba, positions, itemSize, description), positions);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getPositionCount());
        out.writeByteArray(vector.ba);
        out.writeInt(vector.itemSize);
        out.writeString(vector.description);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AggregatorStateBlock<?> that) {
            return this.getPositionCount() == that.getPositionCount()
                && Arrays.equals(this.vector.ba, that.vector.ba)
                && this.vector.itemSize == that.vector.itemSize
                && this.vector.description.equals(that.vector.description);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPositionCount(), Arrays.hashCode(vector.ba), vector.itemSize, vector.description);
    }

    @Override
    public String toString() {
        return "AggregatorStateBlock[positions=" + getPositionCount() + ", vector=" + vector + "]";
    }
}
