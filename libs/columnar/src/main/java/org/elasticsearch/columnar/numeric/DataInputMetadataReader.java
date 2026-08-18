/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;

import java.io.IOException;

/**
 * Read-side adapter that implements {@link MetadataReader} by delegating to a {@link DataInput}.
 * The wrapped input is mutable via {@link #reset(DataInput)} so a single instance can be reused
 * across blocks without per-block allocation.
 */
final class DataInputMetadataReader implements MetadataReader {

    private DataInput in;

    /** Binds this reader to {@code in} for the next block decode. */
    void reset(DataInput in) {
        this.in = in;
    }

    /** A reader over the bytes a {@link MetadataBuffer} accumulated, for reading back what was just written. */
    static DataInputMetadataReader wrap(MetadataBuffer buf) {
        DataInputMetadataReader reader = new DataInputMetadataReader();
        reader.reset(new ByteArrayDataInput(buf.toArrayCopy()));
        return reader;
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public int readVInt() throws IOException {
        return in.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return in.readVLong();
    }

    @Override
    public int readZInt() throws IOException {
        return in.readZInt();
    }

    @Override
    public long readZLong() throws IOException {
        return in.readZLong();
    }
}
