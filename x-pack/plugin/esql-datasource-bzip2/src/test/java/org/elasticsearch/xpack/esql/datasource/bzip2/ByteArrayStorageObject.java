/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;

/**
 * Byte-array backed storage object for testing.
 */
class ByteArrayStorageObject implements StorageObject {
    private final byte[] data;
    private final StoragePath path;

    ByteArrayStorageObject(byte[] data) {
        this(data, StoragePath.of("file:///test.bz2"));
    }

    ByteArrayStorageObject(byte[] data, StoragePath path) {
        this.data = data;
        this.path = path;
    }

    @Override
    public InputStream newStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public InputStream newStream(long position, long length) {
        return new ByteArrayInputStream(data, (int) position, (int) length);
    }

    @Override
    public long length() {
        return data.length;
    }

    @Override
    public Instant lastModified() {
        return Instant.now();
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public StoragePath path() {
        return path;
    }
}
