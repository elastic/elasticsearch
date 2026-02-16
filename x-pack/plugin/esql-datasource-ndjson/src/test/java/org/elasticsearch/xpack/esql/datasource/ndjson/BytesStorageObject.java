/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Byte-array backed storage object for testing
 */
public class BytesStorageObject implements StorageObject {
    private final StoragePath path;
    private final byte[] bytes;

    public BytesStorageObject(String path, byte[] bytes) {
        this.bytes = bytes;
        this.path = StoragePath.of(path);
    }

    @Override
    public InputStream newStream() throws IOException {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream newStream(long position, long length) throws IOException {
        return new ByteArrayInputStream(bytes, (int) position, (int) length);
    }

    @Override
    public long length() throws IOException {
        return bytes.length;
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
