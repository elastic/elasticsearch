/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Reads a byte range of a {@link StorageObject} as a stream of bounded, fully-consumed chunks.
 * <p>
 * Record-boundary probing needs a few hundred bytes from an arbitrary offset, but the length it must declare
 * up front is unknown — a record can legitimately run to {@code maxRecordBytes}. Declaring the whole remainder
 * of the object (the previous approach) made the request undrainable, so the only way to release it was
 * {@link StorageObject#abortStream}, and an aborted response destroys its HTTP connection instead of returning
 * it to the pool. Every probe then re-paid a TCP connect and TLS handshake, which is most of the cost of a
 * cross-region probe and all of the reason ~150 serial probes took ~73s (esql-planning#1528).
 * <p>
 * Fetching in bounded chunks removes the dilemma: each chunk is a closed range that is read to completion, so
 * it closes cleanly and its connection is reusable — the same reason {@code S3StorageObject#fetchMetadata}
 * drains its one-byte length probe rather than aborting it. Chunk sizes grow geometrically from
 * {@link #FIRST_CHUNK_SIZE}, so a normal record costs a single request and a pathological one costs a
 * logarithmic number, with total transfer within roughly twice the bytes actually consumed.
 * <p>
 * Consumers see a plain {@link InputStream}: chunk boundaries are invisible, so a record splitter's lookahead
 * (for instance peeking past a lone {@code CR} to test for {@code LF}) simply reads the first byte of the next
 * chunk, and a splitter's own byte budget keeps counting across chunks unchanged.
 */
final class ChunkedStorageInputStream extends InputStream {

    /**
     * Size of the first fetch. Large enough that a single request answers essentially any real record, small
     * enough that the bytes wasted when only a line's prefix is needed stay far below the stride being probed.
     */
    static final int FIRST_CHUNK_SIZE = 32 * 1024;

    /** Ceiling on chunk growth, so one probe of a pathological record cannot turn into a single huge request. */
    static final int MAX_CHUNK_SIZE = 4 * 1024 * 1024;

    private final StorageObject object;
    private final long endExclusive;
    private final int maxChunkSize;

    /** Absolute position of the next byte to fetch. */
    private long fetchPosition;
    private int nextChunkSize;

    private byte[] chunk = new byte[0];
    private int chunkPosition;
    private int chunkLength;
    private boolean exhausted;

    ChunkedStorageInputStream(StorageObject object, long position, long endExclusive) {
        this(object, position, endExclusive, FIRST_CHUNK_SIZE, MAX_CHUNK_SIZE);
    }

    ChunkedStorageInputStream(StorageObject object, long position, long endExclusive, int firstChunkSize, int maxChunkSize) {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative, got: " + position);
        }
        if (firstChunkSize <= 0 || maxChunkSize < firstChunkSize) {
            throw new IllegalArgumentException("require 0 < firstChunkSize <= maxChunkSize, got: " + firstChunkSize + ", " + maxChunkSize);
        }
        this.object = Objects.requireNonNull(object);
        this.endExclusive = endExclusive;
        this.fetchPosition = position;
        this.nextChunkSize = firstChunkSize;
        this.maxChunkSize = maxChunkSize;
    }

    @Override
    public int read() throws IOException {
        if (ensureAvailable() == false) {
            return -1;
        }
        return chunk[chunkPosition++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        if (ensureAvailable() == false) {
            return -1;
        }
        int n = Math.min(len, chunkLength - chunkPosition);
        System.arraycopy(chunk, chunkPosition, b, off, n);
        chunkPosition += n;
        return n;
    }

    /**
     * Ensures at least one unread byte is buffered, fetching the next chunk only when the current one is spent.
     * A read that is satisfied from the current chunk never triggers a fetch, so the read that finds a record
     * boundary cannot pull a speculative chunk across the wire.
     *
     * @return false once the range is exhausted
     */
    private boolean ensureAvailable() throws IOException {
        if (chunkPosition < chunkLength) {
            return true;
        }
        if (exhausted || fetchPosition >= endExclusive) {
            return false;
        }
        int size = (int) Math.min(nextChunkSize, endExclusive - fetchPosition);
        if (chunk.length < size) {
            chunk = new byte[size];
        }
        ByteBuffer target = ByteBuffer.wrap(chunk, 0, size);
        // Positional reads may return short (a FileChannel-backed provider is entitled to), so refill until the
        // chunk is full or the object ends.
        while (target.hasRemaining()) {
            int read = object.readBytes(fetchPosition + target.position(), target);
            if (read <= 0) {
                break;
            }
        }
        int filled = target.position();
        if (filled == 0) {
            exhausted = true;
            return false;
        }
        chunkPosition = 0;
        chunkLength = filled;
        fetchPosition += filled;
        nextChunkSize = (int) Math.min((long) nextChunkSize * 2, maxChunkSize);
        return true;
    }

    /**
     * No-op: every chunk is fully consumed by the time it is replaced, so this stream holds no provider
     * resource between fetches and there is nothing to release — which is the point of reading in chunks.
     */
    @Override
    public void close() {}
}
