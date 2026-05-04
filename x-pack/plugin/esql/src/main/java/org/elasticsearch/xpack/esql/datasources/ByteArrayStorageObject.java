/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * A {@link StorageObject} backed by a region of a byte array.
 * Used to wrap decompressed chunks as seekable storage objects for parallel parsing.
 */
final class ByteArrayStorageObject implements StorageObject {

    private final StoragePath path;
    private final byte[] data;
    private final int offset;
    private final int length;

    ByteArrayStorageObject(StoragePath path, byte[] data, int offset, int length) {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("Invalid region: offset=" + offset + ", length=" + length);
        }
        // Math.addExact rejects the corner case where both ints are large positives and `offset + length`
        // silently wraps to a negative value, which would let the < data.length check pass.
        int end;
        try {
            end = Math.addExact(offset, length);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("offset + length overflows int: offset=" + offset + ", length=" + length, e);
        }
        if (end > data.length) {
            throw new IllegalArgumentException("Invalid region: offset=" + offset + ", length=" + length + ", data.length=" + data.length);
        }
        this.path = path;
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public InputStream newStream() {
        return new ByteArrayInputStream(data, offset, length);
    }

    @Override
    public InputStream newStream(long position, long len) {
        int pos = Math.toIntExact(position);
        int l = Math.toIntExact(len);
        // Validate the sub-region against the logical [0, length) window: ByteArrayInputStream itself
        // would only catch a read past data.length, so without this guard a caller could read into
        // bytes that belong to an adjacent region of the same backing array.
        if (pos < 0 || l < 0 || Math.addExact(pos, l) > length) {
            throw new IllegalArgumentException(
                "Invalid sub-region: position=" + position + ", length=" + len + ", region length=" + length
            );
        }
        return new ByteArrayInputStream(data, offset + pos, l);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Instant lastModified() {
        return Instant.EPOCH;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public StoragePath path() {
        return path;
    }

    @Override
    public int readBytes(long position, ByteBuffer target) {
        if (position >= length) {
            return -1;
        }
        int pos = Math.toIntExact(position);
        int available = length - pos;
        int toRead = Math.min(available, target.remaining());
        target.put(data, offset + pos, toRead);
        return toRead;
    }
}
