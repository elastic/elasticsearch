/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.io.IOException;

/** A {@link ParquetObject} whose ranged reads go through a {@link ParquetBlobCache}, so that a range is fetched once. */
final class CachedParquetObject implements ParquetObject {

    private final ParquetObject delegate;
    private final ParquetBlobCache cache;

    CachedParquetObject(ParquetObject delegate, ParquetBlobCache cache) {
        this.delegate = delegate;
        this.cache = cache;
    }

    @Override
    public String location() {
        return delegate.location();
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public byte[] readRange(long position, int length) throws IOException {
        byte[] bytes = new byte[length];
        cache.read(delegate, position, bytes, 0, length);
        return bytes;
    }

    @Override
    public void readRange(long position, byte[] destination, int destinationOffset, int length) throws IOException {
        cache.read(delegate, position, destination, destinationOffset, length);
    }

    @Override
    public void prefetchRange(long position, int length) {
        if (cache.prefetchesNextChunk()) {
            cache.prefetch(delegate, position, length);
        }
    }
}
