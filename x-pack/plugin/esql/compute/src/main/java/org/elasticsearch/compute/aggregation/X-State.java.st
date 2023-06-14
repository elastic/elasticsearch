/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.IntVector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Aggregator state for a single $type$.
 * This class is generated. Do not edit it.
 */
@Experimental
final class $Type$State implements AggregatorState<$Type$State> {
    private $type$ value;

    $Type$State() {
        this(0);
    }

    $Type$State($type$ init) {
        this.value = init;
    }

    $type$ $type$Value() {
        return value;
    }

    void $type$Value($type$ value) {
        this.value = value;
    }

    @Override
    public long getEstimatedSize() {
        return $BYTES$;
    }

    @Override
    public void close() {}

    @Override
    public AggregatorStateSerializer<$Type$State> serializer() {
        return new $Type$StateSerializer();
    }

    private static class $Type$StateSerializer implements AggregatorStateSerializer<$Type$State> {
        private static final VarHandle handle = MethodHandles.byteArrayViewVarHandle($type$[].class, ByteOrder.BIG_ENDIAN);

        @Override
        public int size() {
            return $BYTES$;
        }

        @Override
        public int serialize($Type$State state, byte[] ba, int offset, IntVector selected) {
            assert selected.getPositionCount() == 1;
            assert selected.getInt(0) == 0;
            handle.set(ba, offset, state.value);
            return $BYTES$; // number of bytes written
        }

        @Override
        public void deserialize($Type$State state, byte[] ba, int offset) {
            Objects.requireNonNull(state);
            state.value = ($type$) handle.get(ba, offset);
        }
    }
}
