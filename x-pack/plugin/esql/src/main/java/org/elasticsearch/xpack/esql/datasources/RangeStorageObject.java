/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * A view over a byte range of a delegate {@link StorageObject}.
 * Used when a {@link FileSplit} has a non-zero offset, so that format readers
 * see only the relevant portion of the file.
 */
class RangeStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final long offset;
    private final long length;

    RangeStorageObject(StorageObject delegate, long offset, long length) {
        Check.notNull(delegate, "delegate must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0, got: " + length);
        }
        this.delegate = delegate;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public InputStream newStream() throws IOException {
        return delegate.newStream(offset, length);
    }

    @Override
    public InputStream newStream(long position, long rangeLength) throws IOException {
        return delegate.newStream(Math.addExact(offset, position), rangeLength);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }
}
