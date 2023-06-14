/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.aggregation.AggregatorState;
import org.elasticsearch.compute.ann.Experimental;

import java.util.Arrays;

@Experimental
public class AggregatorStateVector<T extends AggregatorState<T>> extends AbstractVector {
    final byte[] ba;

    final int itemSize;

    final String description;

    public AggregatorStateVector(byte[] ba, int positionCount, int itemSize, String description) {
        super(positionCount);
        this.ba = ba;
        this.itemSize = itemSize;
        this.description = description;
    }

    public T get(int position, T item) {
        item.serializer().deserialize(item, ba, position * itemSize);
        return item;
    }

    @Override
    public String toString() {
        return "AggregatorStateVector{"
            + "ba length="
            + ba.length
            + ", positionCount="
            + getPositionCount()
            + ", description="
            + description
            + "}";
    }

    public static <T extends AggregatorState<T>> Builder<AggregatorStateVector<T>, T> builderOfAggregatorState(
        Class<? extends AggregatorState<T>> cls,
        long estimatedSize
    ) {
        return new AggregatorStateBuilder<>(cls, estimatedSize);
    }

    @Override
    public Block asBlock() {
        return new AggregatorStateBlock<>(this, this.getPositionCount());
    }

    @Override
    public Vector filter(int... positions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ElementType elementType() {
        return ElementType.UNKNOWN;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    public interface Builder<B extends Vector, V> {

        Class<?> type();

        Builder<B, V> add(V value, IntVector selected);

        B build();
    }

    static class AggregatorStateBuilder<T extends AggregatorState<T>> implements Builder<AggregatorStateVector<T>, T> {

        private final byte[] ba; // use BigArrays and growable

        private int offset; // offset of next write in the array

        private int size = -1; // hack(ish)

        private int positionCount;

        // The type of data objects that are in the block. Could be an aggregate type.
        private final Class<? extends AggregatorState<T>> cls;

        private AggregatorStateBuilder(Class<? extends AggregatorState<T>> cls) {
            this(cls, 4096);
        }

        private AggregatorStateBuilder(Class<? extends AggregatorState<T>> cls, long estimatedSize) {
            this.cls = cls;
            // cls.getAnnotation() - -
            ba = new byte[(int) estimatedSize];
        }

        @Override
        public Class<? extends AggregatorState<T>> type() {
            return cls;
        }

        @Override
        public Builder<AggregatorStateVector<T>, T> add(T value, IntVector selected) {
            int bytesWritten = value.serializer().serialize(value, ba, offset, selected);
            offset += bytesWritten;
            positionCount++;
            if (size == -1) {
                size = bytesWritten;
            } else {
                if (bytesWritten != size) {
                    throw new RuntimeException("variable size values");
                }
            }
            return this;
        }

        @Override
        public AggregatorStateVector<T> build() {
            return new AggregatorStateVector<>(Arrays.copyOf(ba, ba.length), positionCount, size, "aggregator state for " + cls);
        }
    }
}
