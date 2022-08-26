/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.data;

import org.elasticsearch.xpack.sql.action.compute.aggregation.AggregatorState;

import java.util.Arrays;

public class AggregatorStateBlock<T extends AggregatorState<T>> extends Block {
    private final byte[] ba;

    private final int itemSize;

    private final String description;

    public AggregatorStateBlock(byte[] ba, int positionCount, int itemSize, String description) {
        super(positionCount);
        this.ba = ba;
        this.itemSize = itemSize;
        this.description = description;
    }

    public void get(int position, T item) {
        item.serializer().deserialize(item, ba, position * itemSize);
    }

    @Override
    public String toString() {
        return "ByteArrayBlock{"
            + "ba length="
            + ba.length
            + ", positionCount="
            + getPositionCount()
            + ", description="
            + description
            + "}";
    }

    public static <T extends AggregatorState<T>> Builder<AggregatorStateBlock<T>, T> builderOfAggregatorState(
        Class<? extends AggregatorState<T>> cls
    ) {
        return new AggregatorStateBuilder<>(cls);
    }

    public interface Builder<B extends Block, V> {

        Class<?> type();

        Builder<B, V> add(V value);

        B build();
    }

    static class AggregatorStateBuilder<T extends AggregatorState<T>> implements Builder<AggregatorStateBlock<T>, T> {

        private final byte[] ba; // use BigArrays and growable

        private int offset; // offset of next write in the array

        private int size = -1; // hack(ish)

        private int positionCount;

        // The type of data objects that are in the block. Could be an aggregate type.
        private final Class<? extends AggregatorState<T>> cls;

        private AggregatorStateBuilder(Class<? extends AggregatorState<T>> cls) {
            this.cls = cls;
            // cls.getAnnotation() - -
            ba = new byte[4096]; // for now, should size based on Aggregator state size
        }

        @Override
        public Class<? extends AggregatorState<T>> type() {
            return cls;
        }

        @Override
        public Builder<AggregatorStateBlock<T>, T> add(T value) {
            int bytesWritten = value.serializer().serialize(value, ba, offset);
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
        public AggregatorStateBlock<T> build() {
            return new AggregatorStateBlock<>(Arrays.copyOf(ba, ba.length), positionCount, size, "aggregator state for " + cls);
        }
    }
}
