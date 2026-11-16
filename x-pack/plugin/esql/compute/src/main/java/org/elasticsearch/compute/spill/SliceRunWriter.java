/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.StreamOutputHelper;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.List;

/**
 * Writes one partitioned generation of a grouping aggregation as a slice run: one record per partition holding its
 * keys and every aggregator's state for it. The record starts with the key count and one flag per aggregator saying
 * whether it has a value for every group, so that a restore answers the unseen-groups question before it decodes
 * anything. The writer keeps that metadata, with the record sizes, and hands it over with the published run, so that
 * the generation's owner decides and sizes restores without touching the store.
 */
public final class SliceRunWriter {

    /**
     * A published slice run with the metadata a restore needs: per partition the key count and the record size, per
     * aggregator and partition the all-values flag. A partition the writer skipped has no keys and no record.
     */
    public record SliceRun(FramedRun run, int[] keyCount, long[] recordBytes, boolean[][] allValues) {
        public boolean hasSlice(int partition) {
            return keyCount[partition] > 0;
        }
    }

    private final FramedRunWriter writer;
    private final int[] keyCount = new int[PartitionedHashTable.NUM_PARTITIONS];
    private final long[] recordBytes = new long[PartitionedHashTable.NUM_PARTITIONS];
    private final boolean[][] allValues;

    /**
     * @param aggregators how many aggregator states every record carries
     */
    public SliceRunWriter(FramedRunWriter writer, int aggregators) {
        this.writer = writer;
        this.allValues = new boolean[aggregators][PartitionedHashTable.NUM_PARTITIONS];
    }

    public FramedRunWriter writer() {
        return writer;
    }

    /**
     * Queues one partition. {@code keys} and {@code states} run on the codec thread over the generation's arrays, which
     * nothing mutates any more; {@code onDone} is released once the record is serialized, or when the write fails. The
     * listener gets the record's size.
     *
     * @param keyCount the keys of the partition, which the states' writers take as their count
     * @param hasAllValues per aggregator, whether every group of the partition has a value
     */
    public void append(
        int partition,
        int keyCount,
        boolean[] hasAllValues,
        CheckedConsumer<StreamOutput, IOException> keys,
        List<CheckedConsumer<StreamOutput, IOException>> states,
        Releasable onDone,
        ActionListener<Long> listener
    ) {
        if (hasAllValues.length != allValues.length || states.size() != allValues.length) {
            throw new IllegalArgumentException(
                "expected ["
                    + allValues.length
                    + "] aggregators, got ["
                    + hasAllValues.length
                    + "] flags and ["
                    + states.size()
                    + "] states"
            );
        }
        if (keyCount <= 0) {
            throw new IllegalArgumentException("a slice holds at least one key, got [" + keyCount + "]");
        }
        this.keyCount[partition] = keyCount;
        for (int i = 0; i < hasAllValues.length; i++) {
            allValues[i][partition] = hasAllValues[i];
        }
        writer.append(partition, out -> {
            CountingStreamOutput counting = new CountingStreamOutput(out);
            counting.writeVInt(keyCount);
            counting.writeVInt(hasAllValues.length);
            for (boolean all : hasAllValues) {
                counting.writeBoolean(all);
            }
            keys.accept(counting);
            for (CheckedConsumer<StreamOutput, IOException> state : states) {
                state.accept(counting);
            }
            recordBytes[partition] = counting.position();
        }, onDone, listener.map(ignored -> recordBytes[partition]));
    }

    /**
     * Publishes the run with its metadata. Nothing may be appended after it.
     */
    public void finish(ActionListener<SliceRun> listener) {
        writer.finish(listener.map(run -> new SliceRun(run, keyCount, recordBytes, allValues)));
    }

    public void abort(ActionListener<Void> listener) {
        writer.abort(listener);
    }

    /**
     * Forwards every byte to the run and counts it, so that a record's size is known without a second pass.
     */
    private static final class CountingStreamOutput extends StreamOutput {
        private final StreamOutput out;
        private long position;

        CountingStreamOutput(StreamOutput out) {
            this.out = out;
            setTransportVersion(out.getTransportVersion());
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            out.writeByte(b);
            position++;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            out.writeBytes(b, offset, length);
            position += length;
        }

        // the helpers write through this stream's byte writes, which count; their returns are not added again

        @Override
        public void writeString(String str) throws IOException {
            StreamOutputHelper.writeString(str, this);
        }

        @Override
        public void writeOptionalString(@Nullable String str) throws IOException {
            StreamOutputHelper.writeOptionalString(str, this);
        }

        @Override
        public void writeGenericString(String value) throws IOException {
            StreamOutputHelper.writeGenericString(value, this);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
