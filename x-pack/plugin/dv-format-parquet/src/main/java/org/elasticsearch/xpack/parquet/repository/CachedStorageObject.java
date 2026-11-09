/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * A {@link StorageObject} whose reads go through a {@link ParquetBlobCache}, so that the ES|QL reader fetches a range of the file from
 * the repository once for the node. Every read method of {@link StorageObject} ends in {@link #newStream(long, long)} or in
 * {@link #readBytes(long, ByteBuffer)} (the asynchronous ones are defaults over them), so those two are the whole surface that needs to
 * change; the rest is the object that it wraps.
 */
public final class CachedStorageObject implements StorageObject, Closeable {

    /**
     * The most that one fetch of a stream asks the cache for. It matches the largest window that the reader reads at once, so that a
     * window is one request to the repository and not one for each small read.
     */
    static final long MAX_CHUNK_BYTES = 8L * 1024 * 1024;

    private final StorageObject delegate;
    private final ParquetBlobCache cache;
    private final String key;

    /** @param key what the cache keys the file by, see {@link ParquetBlobCache#read} */
    public CachedStorageObject(StorageObject delegate, ParquetBlobCache cache, String key) {
        this.delegate = delegate;
        this.cache = cache;
        this.key = key;
    }

    public CachedStorageObject(StorageObject delegate, ParquetBlobCache cache) {
        this(delegate, cache, delegate.path().toString());
    }

    /** Releases the borrow of the file that the wrapped object holds, if it holds one (a file of an ES|QL data source does). */
    @Override
    public void close() throws IOException {
        if (delegate instanceof Closeable closeable) {
            closeable.close();
        }
    }

    @Override
    public StorageIdentity storageIdentity() {
        return delegate.storageIdentity();
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {
        delegate.attachMetrics(metrics, scheme);
    }

    @Override
    public InputStream newStream() throws IOException {
        return newStream(0, READ_TO_END);
    }

    @Override
    public InputStream newStream(long position, long readLength) throws IOException {
        long fileLength = delegate.length();
        long end = readLength == READ_TO_END ? fileLength : Math.min(fileLength, position + readLength);
        if (position < 0 || end < position) {
            throw new IOException(
                "range [" + position + ", +" + readLength + ") is outside of [" + delegate.path() + "] of length " + fileLength
            );
        }
        return new CachedRangeStream(position, end);
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        if (target.hasRemaining() == false) {
            return 0;
        }
        long fileLength = delegate.length();
        if (position >= fileLength) {
            return -1;
        }
        int count = (int) Math.min(target.remaining(), fileLength - position);
        if (target.hasArray()) {
            cache.read(delegate, key, position, target.array(), target.arrayOffset() + target.position(), count);
            target.position(target.position() + count);
        } else {
            byte[] bytes = new byte[count];
            cache.read(delegate, key, position, bytes, 0, count);
            target.put(bytes);
        }
        return count;
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
    }

    @Override
    public String version() {
        return delegate.version();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    /** The bytes of {@code [next, end)}, fetched through the cache one chunk at a time. */
    private final class CachedRangeStream extends InputStream {
        private long next;
        private final long end;
        private byte[] chunk = new byte[0];
        private int chunkPosition;
        private int chunkLimit;

        CachedRangeStream(long start, long end) {
            this.next = start;
            this.end = end;
        }

        private boolean fill() throws IOException {
            if (chunkPosition < chunkLimit) {
                return true;
            }
            if (next >= end) {
                return false;
            }
            int count = (int) Math.min(MAX_CHUNK_BYTES, end - next);
            if (chunk.length < count) {
                chunk = new byte[count];
            }
            cache.read(delegate, key, next, chunk, 0, count);
            next += count;
            chunkPosition = 0;
            chunkLimit = count;
            return true;
        }

        @Override
        public int read() throws IOException {
            if (fill() == false) {
                return -1;
            }
            return chunk[chunkPosition++] & 0xff;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (fill() == false) {
                return -1;
            }
            int count = Math.min(length, chunkLimit - chunkPosition);
            System.arraycopy(chunk, chunkPosition, destination, offset, count);
            chunkPosition += count;
            return count;
        }

        @Override
        public int available() {
            return chunkLimit - chunkPosition;
        }
    }
}
