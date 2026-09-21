/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;

/**
 * Writes a {@code DirectMonotonic} table straight into its file as the entries arrive, keeping only the table's
 * metadata on the heap. The metadata records where each block of {@code 1 << BLOCK_SHIFT} entries begins, so
 * tables written into one file at once may interleave. No entry count is needed up front.
 */
public final class MonotonicWriter {

    /**
     * Block shift of every table this class writes. It is frozen: {@link MonotonicReader} decodes with the
     * same value, and a table written with a different one would decode to wrong offsets.
     */
    public static final int BLOCK_SHIFT = 16;

    private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

    /** Location of a finished table in its file, plus its {@code DirectMonotonic} metadata. */
    public record Table(long dataOffset, long dataLength, byte[] meta) {
        public static final Table NONE = new Table(0, 0, new byte[0]);
    }

    private final IndexOutput out;
    private final long start;
    private final ByteBuffersDataOutput meta = new ByteBuffersDataOutput();
    /** The entries of the block being filled, grown as they arrive and never past one block. */
    private long[] buffer = new long[0];
    private int buffered;
    private boolean finished;

    public MonotonicWriter(IndexOutput out) {
        this.out = out;
        this.start = out.getFilePointer();
    }

    public void add(long value) throws IOException {
        assert finished == false : "already finished";
        if (buffered == buffer.length) {
            buffer = ArrayUtil.growExact(buffer, Math.min(BLOCK_SIZE, ArrayUtil.oversize(buffered + 1, Long.BYTES)));
        }
        buffer[buffered++] = value;
        if (buffered == BLOCK_SIZE) {
            flush();
        }
    }

    /** Writes what is left and answers where the table is and how to read it. */
    public Table finish() throws IOException {
        assert finished == false : "already finished";
        finished = true;
        if (buffered > 0) {
            flush();
        }
        return new Table(start, out.getFilePointer() - start, meta.toArrayCopy());
    }

    /** One block, laid out as {@code DirectMonotonicWriter} lays it out, so {@code DirectMonotonicReader} reads it. */
    private void flush() throws IOException {
        final float avgInc = (float) ((double) (buffer[buffered - 1] - buffer[0]) / Math.max(1, buffered - 1));
        long min = Long.MAX_VALUE;
        for (int i = 0; i < buffered; ++i) {
            buffer[i] -= (long) (avgInc * (long) i);
            min = Math.min(buffer[i], min);
        }
        long maxDelta = 0;
        for (int i = 0; i < buffered; ++i) {
            buffer[i] -= min;
            maxDelta |= buffer[i];
        }
        meta.writeLong(min);
        meta.writeInt(Float.floatToIntBits(avgInc));
        meta.writeLong(out.getFilePointer() - start);
        if (maxDelta == 0) {
            meta.writeByte((byte) 0);
        } else {
            final int bitsRequired = DirectWriter.unsignedBitsRequired(maxDelta);
            final DirectWriter writer = DirectWriter.getInstance(out, buffered, bitsRequired);
            for (int i = 0; i < buffered; ++i) {
                writer.add(buffer[i]);
            }
            writer.finish();
            meta.writeByte((byte) bitsRequired);
        }
        buffered = 0;
    }
}
