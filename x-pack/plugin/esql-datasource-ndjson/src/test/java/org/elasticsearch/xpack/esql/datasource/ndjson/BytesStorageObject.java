/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ByteRegionInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Byte-array backed storage object for testing. Returns {@link ByteRegionInputStream}
 * so iterator tests hit the in-place parse path.
 */
public class BytesStorageObject extends AbstractTestStorageObject {
    private final StoragePath path;
    private final byte[] bytes;
    private final int offset;
    private final int length;

    public BytesStorageObject(String path, byte[] bytes) {
        this(path, bytes, 0, bytes.length);
    }

    public BytesStorageObject(String path, byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.path = StoragePath.of(path);
    }

    /** Backing array, including any padding outside the logical region. */
    byte[] bytes() {
        return bytes;
    }

    @Override
    public InputStream newStream() throws IOException {
        return new ByteRegionInputStream(bytes, offset, length);
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        return new ByteRegionInputStream(bytes, offset + (int) position, (int) length);
    }

    @Override
    public long length() throws IOException {
        return length;
    }

    @Override
    public Instant lastModified() throws IOException {
        return Instant.now();
    }

    @Override
    public boolean exists() throws IOException {
        return true;
    }

    @Override
    public StoragePath path() {
        return path;
    }
}
