/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.PartitionedHashTable.PartitionedHashKeys;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

/**
 * Reads the partitions of a slice run back, one at a time: the record's bytes come into a charged buffer first, then the
 * keys are decoded through the block hash and the states through the aggregators, each charged to {@code breaker},
 * and the buffer goes. A partition the writer skipped, one with no keys, reads back as {@link Slice#EMPTY}.
 */
public final class SliceRunReader implements Releasable {

    /**
     * One partition restored: its keys, the states in the aggregators' order, the key count and the per-aggregator
     * all-values flags. Released as a resident generation's partition is, through the breaker that charged it.
     */
    public record Slice(
        int keyCount,
        boolean[] hasAllValues,
        @Nullable PartitionedHashKeys keys,
        GroupingAggregatorFunction.PartitionedState[] states
    ) {
        public static final Slice EMPTY = new Slice(0, new boolean[0], null, new GroupingAggregatorFunction.PartitionedState[0]);

        public boolean isEmpty() {
            return keys == null;
        }

        public void release(CircuitBreaker breaker) {
            if (keys != null) {
                keys.releaseAll(breaker);
            }
            for (GroupingAggregatorFunction.PartitionedState state : states) {
                state.releaseAll(breaker);
            }
        }
    }

    private final FramedRunReader reader;
    private final PartitionedBlockHash hash;
    private final List<GroupingAggregatorFunction> aggregators;
    private final CircuitBreaker breaker;

    /**
     * @param hash the block hash of the operator that reads the slices back, which owns the key codec
     * @param aggregators the functions in the order the writer wrote their states
     * @param breaker what the decoded keys and states are charged to: the query's root view
     */
    public SliceRunReader(
        FramedRunReader reader,
        PartitionedBlockHash hash,
        List<GroupingAggregatorFunction> aggregators,
        CircuitBreaker breaker
    ) {
        if (reader.header().kind() != FramedRunLayout.KIND_SLICE) {
            throw new IllegalArgumentException("run is of kind [" + reader.header().kind() + "], not a slice run");
        }
        this.reader = reader;
        this.hash = hash;
        this.aggregators = aggregators;
        this.breaker = breaker;
    }

    public FramedRunReader reader() {
        return reader;
    }

    /**
     * Whether the writer put a record for {@code partition}.
     */
    public boolean hasSlice(int partition) {
        return reader.records(partition) > 0;
    }

    /**
     * Restores one partition; the listener gets the slice, {@link Slice#EMPTY} for a partition without keys.
     */
    public void readSlice(int partition, ActionListener<Slice> listener) {
        if (hasSlice(partition) == false) {
            listener.onResponse(Slice.EMPTY);
            return;
        }
        FramedRunReader.RecordCursor cursor = reader.cursor(partition);
        cursor.next(ActionListener.runAfter(listener.delegateFailureAndWrap((l, record) -> {
            if (record == null) {
                l.onFailure(new IOException("slice run partition [" + partition + "] has no record"));
                return;
            }
            Slice slice;
            try (record) {
                slice = decode(partition, record.payload());
            } catch (Exception e) {
                l.onFailure(e);
                return;
            }
            l.onResponse(slice);
        }), cursor::close));
    }

    private Slice decode(int partition, ReleasableBytesReference payload) throws IOException {
        try (StreamInput in = payload.streamInput()) {
            in.setTransportVersion(reader.transportVersion());
            int keyCount = in.readVInt();
            int functions = in.readVInt();
            if (functions != aggregators.size()) {
                throw new IOException(
                    "slice run partition [" + partition + "] holds [" + functions + "] states, expected [" + aggregators.size() + "]"
                );
            }
            boolean[] hasAllValues = new boolean[functions];
            for (int i = 0; i < functions; i++) {
                hasAllValues[i] = in.readBoolean();
            }
            PartitionedHashKeys keys = null;
            GroupingAggregatorFunction.PartitionedState[] states = new GroupingAggregatorFunction.PartitionedState[functions];
            boolean success = false;
            try {
                keys = hash.readPartitionKeys(in, partition, breaker);
                for (int i = 0; i < functions; i++) {
                    states[i] = aggregators.get(i).readPartition(in, partition, breaker);
                }
                success = true;
                return new Slice(keyCount, hasAllValues, keys, states);
            } finally {
                if (success == false) {
                    if (keys != null) {
                        keys.releaseAll(breaker);
                    }
                    for (GroupingAggregatorFunction.PartitionedState state : states) {
                        if (state != null) {
                            state.releaseAll(breaker);
                        }
                    }
                }
            }
        }
    }

    /**
     * Decodes what {@code decoder} reads from the payload; a helper for codec tests.
     */
    static <T> T decodeWith(ReleasableBytesReference payload, CheckedFunction<StreamInput, T, IOException> decoder) throws IOException {
        try (StreamInput in = payload.streamInput()) {
            return decoder.apply(in);
        }
    }

    @Override
    public void close() {
        Releasables.close(reader);
    }
}
