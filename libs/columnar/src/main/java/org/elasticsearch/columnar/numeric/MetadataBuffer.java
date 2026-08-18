/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Write-side buffer for per-block {@link BlockTransform} stage metadata. Implements
 * {@link MetadataWriter} by accumulating bytes in a {@link ByteBuffersDataOutput}; call
 * {@link #reset()} between blocks and {@link #copyTo(DataOutput)} to flush to the main output.
 */
final class MetadataBuffer implements MetadataWriter {

    private final ByteBuffersDataOutput buf = new ByteBuffersDataOutput();

    /** Clears the buffer so it is ready for the next block. */
    void reset() {
        buf.reset();
    }

    /** Copies accumulated bytes to {@code out}. */
    void copyTo(DataOutput out) throws IOException {
        buf.copyTo(out);
    }

    /** Returns the number of bytes accumulated in the buffer. */
    long size() {
        return buf.size();
    }

    /** Returns a copy of the accumulated bytes as a new array. */
    byte[] toArrayCopy() {
        return buf.toArrayCopy();
    }

    @Override
    public MetadataWriter writeByte(byte v) {
        buf.writeByte(v);
        return this;
    }

    @Override
    public MetadataWriter writeInt(int v) {
        buf.writeInt(v);
        return this;
    }

    @Override
    public MetadataWriter writeLong(long v) {
        buf.writeLong(v);
        return this;
    }

    @Override
    public MetadataWriter writeVInt(int v) {
        try {
            buf.writeVInt(v);
        } catch (IOException e) {
            // Writing to memory via ByteBuffersDataOutput, so this should never happen
            throw new UncheckedIOException(e);
        }
        return this;
    }

    @Override
    public MetadataWriter writeVLong(long v) {
        try {
            buf.writeVLong(v);
        } catch (IOException e) {
            // Writing to memory via ByteBuffersDataOutput, so this should never happen
            throw new UncheckedIOException(e);
        }
        return this;
    }

    @Override
    public MetadataWriter writeZInt(int v) {
        try {
            buf.writeZInt(v);
        } catch (IOException e) {
            // Writing to memory via ByteBuffersDataOutput, so this should never happen
            throw new UncheckedIOException(e);
        }
        return this;
    }

    @Override
    public MetadataWriter writeZLong(long v) {
        try {
            buf.writeZLong(v);
        } catch (IOException e) {
            // Writing to memory via ByteBuffersDataOutput, so this should never happen
            throw new UncheckedIOException(e);
        }
        return this;
    }
}
