/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adapter that wraps a StorageObject to implement Parquet's InputFile interface.
 * This allows using our storage abstraction with Parquet's ParquetFileReader.
 *
 * <p>Key features:
 * <ul>
 *   <li>Uses <strong>only</strong> range reads ({@code newStream(position, length)}) — never full-object GET</li>
 *   <li>Sliding window cache (default 4MB) to amortize seeks and avoid {@code InputStream.skip}</li>
 *   <li>Optimized for remote storage (S3, HTTP) where full GET and skip-download are expensive</li>
 *   <li>No Hadoop dependencies — uses pure Java InputStream</li>
 * </ul>
 */
public class ParquetStorageObjectAdapter implements org.apache.parquet.io.InputFile {
    private final StorageObject storageObject;
    private final long length;
    private final FooterCacheKey footerCacheKey;
    private final int windowSize;

    /** Default window size (4MB) for the sliding range cache. */
    static final int DEFAULT_WINDOW_SIZE = 4 * 1024 * 1024;

    /** Maximum window size (16MB). Caps adaptive window hints to prevent unbounded memory allocation. */
    static final int MAX_WINDOW_SIZE = 16 * 1024 * 1024;

    /** Footer cache budget across the JVM (8MB). */
    static final int FOOTER_CACHE_MAX_BYTES = 8 * 1024 * 1024;

    /** Max single footer entry (2MB). Prevents caching unusually large footers. */
    static final int FOOTER_CACHE_MAX_ENTRY_BYTES = 2 * 1024 * 1024;

    private static final FooterCache FOOTER_CACHE = new FooterCache(FOOTER_CACHE_MAX_BYTES, FOOTER_CACHE_MAX_ENTRY_BYTES);

    /**
     * Creates an adapter with the default 4MB sliding window.
     */
    public ParquetStorageObjectAdapter(StorageObject storageObject) {
        this(storageObject, DEFAULT_WINDOW_SIZE);
    }

    /**
     * Creates an adapter with an adaptive window sized to cover the given byte range.
     * This allows all column chunks within a small row-group split to be fetched in a single I/O
     * instead of incurring multiple range GETs with the default 4 MiB window.
     *
     * @param rangeBytes byte span of the range being read; clamped to [{@link #DEFAULT_WINDOW_SIZE}, {@link #MAX_WINDOW_SIZE}]
     */
    public static ParquetStorageObjectAdapter forRange(StorageObject storageObject, long rangeBytes) {
        int windowSize = (int) Math.min(Math.max(rangeBytes, DEFAULT_WINDOW_SIZE), MAX_WINDOW_SIZE);
        return new ParquetStorageObjectAdapter(storageObject, windowSize);
    }

    private ParquetStorageObjectAdapter(StorageObject storageObject, int windowSize) {
        if (storageObject == null) {
            throw new QlIllegalArgumentException("storageObject cannot be null");
        }
        this.storageObject = storageObject;
        this.windowSize = windowSize;
        try {
            this.length = storageObject.length();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read storage object length for [" + storageObject.path() + "]", e);
        }
        this.footerCacheKey = buildFooterCacheKey(storageObject, this.length);
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new RangeFirstSeekableInputStream(storageObject, footerCacheKey, length, windowSize);
    }

    static void clearFooterCacheForTests() {
        FOOTER_CACHE.clear();
    }

    private static FooterCacheKey buildFooterCacheKey(StorageObject storageObject, long length) {
        Instant lastModified;
        try {
            lastModified = storageObject.lastModified();
        } catch (IOException e) {
            lastModified = null;
        }
        Long lastModifiedMillis = lastModified == null ? null : lastModified.toEpochMilli();
        return new FooterCacheKey(storageObject.path().toString(), length, lastModifiedMillis);
    }

    /**
     * SeekableInputStream that uses only range reads and a sliding window cache.
     * Never calls {@link StorageObject#newStream()} (full GET) or {@link java.io.InputStream#skip(long)}.
     * On seek: if the target position is within the current window, only the cursor is updated;
     * otherwise a new range is fetched via {@code newStream(position, windowSize)}.
     */
    private static class RangeFirstSeekableInputStream extends SeekableInputStream {
        private final StorageObject storageObject;
        private final FooterCacheKey footerCacheKey;
        private final long length;
        private final int windowSize;
        private final byte[] window;

        private long windowStart;
        private int windowLength;
        private long position;
        private boolean closed;

        RangeFirstSeekableInputStream(StorageObject storageObject, FooterCacheKey footerCacheKey, long length, int windowSize) {
            this.storageObject = storageObject;
            this.footerCacheKey = footerCacheKey;
            this.length = length;
            this.windowSize = windowSize;
            this.window = new byte[windowSize];
            this.windowStart = -1;
            this.windowLength = 0;
            this.position = 0;
            this.closed = false;
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (newPos < 0) {
                throw new IOException("Cannot seek to negative position: " + newPos);
            }
            if (newPos > length) {
                throw new IOException("Cannot seek beyond end of file: " + newPos + " > " + length);
            }

            position = newPos;

            if (position >= windowStart && position < windowStart + windowLength) {
                return;
            }

            fetchWindowAt(position);
        }

        private void fetchWindowAt(long pos) throws IOException {
            long remaining = length - pos;
            long toRead = Math.min(windowSize, remaining);
            if (toRead <= 0) {
                windowStart = pos;
                windowLength = 0;
                return;
            }

            FooterCacheEntry cached = FOOTER_CACHE.get(footerCacheKey);
            if (cached != null && cached.covers(pos, (int) toRead)) {
                int from = (int) (pos - cached.startOffset());
                System.arraycopy(cached.bytes(), from, window, 0, (int) toRead);
                windowStart = pos;
                windowLength = (int) toRead;
                return;
            }

            windowStart = pos;
            windowLength = 0;
            ByteBuffer target = ByteBuffer.wrap(window, 0, (int) toRead);
            int bytesRead = storageObject.readBytes(pos, target);
            windowLength = bytesRead < 0 ? 0 : bytesRead;

            if (windowLength > 0 && windowStart + windowLength == length) {
                FOOTER_CACHE.putTailIfEligible(footerCacheKey, windowStart, window, windowLength);
            }
        }

        private void ensureWindow() throws IOException {
            if (position >= length) {
                return;
            }
            if (position >= windowStart && position < windowStart + windowLength) {
                return;
            }
            fetchWindowAt(position);
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (position >= length) {
                return -1;
            }
            ensureWindow();
            if (position >= windowStart + windowLength) {
                return -1;
            }
            int offset = (int) (position - windowStart);
            int b = window[offset] & 0xFF;
            position++;
            return b;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (position >= length) {
                return -1;
            }
            if (len <= 0) {
                return 0;
            }
            ensureWindow();
            int availableInWindow = windowLength - (int) (position - windowStart);
            if (availableInWindow <= 0) {
                return -1;
            }
            int toRead = Math.min(len, availableInWindow);
            int offset = (int) (position - windowStart);
            System.arraycopy(window, offset, b, off, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public long skip(long n) throws IOException {
            if (n <= 0) {
                return 0;
            }
            long newPos = Math.min(position + n, length);
            long skipped = newPos - position;
            seek(newPos);
            return skipped;
        }

        @Override
        public int available() throws IOException {
            if (closed || position >= length) {
                return 0;
            }
            if (position >= windowStart && position < windowStart + windowLength) {
                return windowLength - (int) (position - windowStart);
            }
            return 0;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            windowStart = -1;
            windowLength = 0;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            int offset = start;
            int remaining = len;
            while (remaining > 0) {
                int bytesRead = read(bytes, offset, remaining);
                if (bytesRead < 0) {
                    throw new IOException("Reached end of stream before reading " + len + " bytes");
                }
                offset += bytesRead;
                remaining -= bytesRead;
            }
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasRemaining() == false) {
                return 0;
            }
            if (buf.hasArray()) {
                int off = buf.arrayOffset() + buf.position();
                int bytesRead = read(buf.array(), off, buf.remaining());
                if (bytesRead > 0) {
                    buf.position(buf.position() + bytesRead);
                }
                return bytesRead;
            }
            byte[] transfer = new byte[Math.min(buf.remaining(), StorageObject.TRANSFER_BUFFER_SIZE)];
            int totalRead = 0;
            while (buf.hasRemaining()) {
                int toRead = Math.min(transfer.length, buf.remaining());
                int n = read(transfer, 0, toRead);
                if (n < 0) {
                    break;
                }
                buf.put(transfer, 0, n);
                totalRead += n;
            }
            return totalRead == 0 ? -1 : totalRead;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasArray()) {
                int off = buf.arrayOffset() + buf.position();
                readFully(buf.array(), off, buf.remaining());
                buf.position(buf.limit());
                return;
            }
            byte[] transfer = new byte[Math.min(buf.remaining(), StorageObject.TRANSFER_BUFFER_SIZE)];
            while (buf.hasRemaining()) {
                int toRead = Math.min(transfer.length, buf.remaining());
                readFully(transfer, 0, toRead);
                buf.put(transfer, 0, toRead);
            }
        }
    }

    private record FooterCacheKey(String path, long length, Long lastModifiedMillis) {}

    private record FooterCacheEntry(long startOffset, byte[] bytes) {
        boolean covers(long position, int length) {
            if (length <= 0) {
                return true;
            }
            long start = startOffset;
            long endExclusive = startOffset + bytes.length;
            long requestedEnd = position + length;
            return position >= start && requestedEnd <= endExclusive;
        }
    }

    private static class FooterCache {
        private final int maxBytes;
        private final int maxEntryBytes;
        private final LinkedHashMap<FooterCacheKey, FooterCacheEntry> map = new LinkedHashMap<>(16, 0.75f, true);
        private int totalBytes;

        FooterCache(int maxBytes, int maxEntryBytes) {
            this.maxBytes = maxBytes;
            this.maxEntryBytes = maxEntryBytes;
        }

        synchronized FooterCacheEntry get(FooterCacheKey key) {
            return map.get(key);
        }

        synchronized void putTailIfEligible(FooterCacheKey key, long startOffset, byte[] buffer, int length) {
            if (length <= 0 || length > maxEntryBytes) {
                return;
            }
            byte[] bytes = new byte[length];
            System.arraycopy(buffer, 0, bytes, 0, length);

            FooterCacheEntry previous = map.put(key, new FooterCacheEntry(startOffset, bytes));
            if (previous != null) {
                totalBytes -= previous.bytes().length;
            }
            totalBytes += bytes.length;
            evictIfNeeded();
        }

        private void evictIfNeeded() {
            while (totalBytes > maxBytes && map.isEmpty() == false) {
                Map.Entry<FooterCacheKey, FooterCacheEntry> eldest = map.entrySet().iterator().next();
                FooterCacheEntry removed = eldest.getValue();
                map.remove(eldest.getKey());
                totalBytes -= removed.bytes().length;
            }
        }

        synchronized void clear() {
            map.clear();
            totalBytes = 0;
        }
    }
}
