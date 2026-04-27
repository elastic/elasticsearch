/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.io.SeekableInputStream;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Adapter that wraps a StorageObject to implement Parquet's InputFile interface.
 * This allows using our storage abstraction with Parquet's ParquetFileReader.
 *
 * <p>Two {@link SeekableInputStream} implementations are available, selected by
 * {@link #windowCacheEnabled}:
 * <ul>
 *   <li><b>Direct</b> (default): each request issues a fresh positional read via
 *       {@link StorageObject#readBytes}. Correctness-safe fallback while the window cache bug
 *       is being investigated, see <a href="https://github.com/elastic/esql-planning/issues/585">esql-planning#585</a>.
 *       May increase the number of range requests on remote storage (S3, HTTP).</li>
 *   <li><b>Windowed</b> (currently disabled): sliding window cache (default 4 MiB) that amortizes
 *       seeks and avoids {@link java.io.InputStream#skip}. Also consults the JVM-wide footer cache
 *       and any prefetched column chunks installed via {@link #installPrefetchedData}.</li>
 * </ul>
 *
 * <p>Both paths use <strong>only</strong> range reads ({@code newStream(position, length)})
 * and {@link StorageObject#readBytes} — never a full-object GET — and have no Hadoop dependencies.
 *
 * <p>A JVM-wide {@link FooterCache} (8 MiB budget) caches the tail bytes of Parquet files
 * to avoid redundant footer reads across splits. Thundering-herd protection ensures that
 * concurrent tail reads for the same file coalesce into a single I/O via {@link CompletableFuture}.
 */
public class ParquetStorageObjectAdapter implements org.apache.parquet.io.InputFile {

    private static final Logger logger = LogManager.getLogger(ParquetStorageObjectAdapter.class);

    private final StorageObject storageObject;
    private final long length;
    private final FooterCacheKey footerCacheKey;
    private final int windowSize;
    private volatile NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> prefetchedChunks;

    /** Default window size (4MB) for the sliding range cache. */
    static final int DEFAULT_WINDOW_SIZE = 4 * 1024 * 1024;

    /** Maximum window size (16MB). Caps adaptive window hints to prevent unbounded memory allocation. */
    static final int MAX_WINDOW_SIZE = 16 * 1024 * 1024;

    /** Footer cache budget across the JVM (8MB). */
    static final int FOOTER_CACHE_MAX_BYTES = 8 * 1024 * 1024;

    /** Max single footer entry (2MB). Prevents caching unusually large footers. */
    static final int FOOTER_CACHE_MAX_ENTRY_BYTES = 2 * 1024 * 1024;

    /**
     * Controls whether the sliding-window read cache is active. Disabled by default due to a
     * non-deterministic correctness bug where seeks within the cached window can serve stale
     * bytes, corrupting dictionary-encoded column values.
     *
     * <p>Visible for testing only. Production code should not mutate this field; tests flip it
     * via {@link #setWindowCacheEnabledForTests(boolean)} and must restore the previous value.
     */
    static volatile boolean windowCacheEnabled = false;

    /**
     * Test-only hook to toggle the window cache in a single test method. Returns the previous
     * value so the caller can restore it (typically in a {@code @After} method).
     */
    static boolean setWindowCacheEnabledForTests(boolean enabled) {
        boolean previous = windowCacheEnabled;
        windowCacheEnabled = enabled;
        return previous;
    }

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
     * <p>Note: the configured window size is only honored when {@link #windowCacheEnabled} is
     * {@code true}. With the default direct-read path, this factory is equivalent to the
     * no-arg constructor — the window buffer itself is only allocated when
     * {@link #newStream()} actually instantiates {@code RangeFirstSeekableInputStream}.
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

    /**
     * Installs prefetched column chunk data that existing streams will consult before issuing I/O.
     * Thread-safe: uses volatile write; streams read this field on every {@code fetchWindowAt} call
     * so data installed after the stream was opened is still visible.
     */
    void installPrefetchedData(NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks) {
        this.prefetchedChunks = chunks;
    }

    void clearPrefetchedData() {
        this.prefetchedChunks = null;
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        if (windowCacheEnabled) {
            return new RangeFirstSeekableInputStream(storageObject, footerCacheKey, length, windowSize, this);
        }
        return new DirectSeekableInputStream(storageObject, length);
    }

    static void clearFooterCacheForTests() {
        FOOTER_CACHE.clear();
    }

    private static FooterCacheKey buildFooterCacheKey(StorageObject storageObject, long length) {
        return new FooterCacheKey(storageObject.path().toString(), length);
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
        private final ParquetStorageObjectAdapter adapter;

        private long windowStart;
        private int windowLength;
        private long position;
        private boolean closed;

        RangeFirstSeekableInputStream(
            StorageObject storageObject,
            FooterCacheKey footerCacheKey,
            long length,
            int windowSize,
            ParquetStorageObjectAdapter adapter
        ) {
            this.storageObject = storageObject;
            this.footerCacheKey = footerCacheKey;
            this.length = length;
            this.windowSize = windowSize;
            this.window = new byte[windowSize];
            this.adapter = adapter;
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

            if (tryFillFromPrefetched(pos, (int) toRead)) {
                return;
            }

            boolean isTailRead = pos + toRead == length;

            FooterCacheEntry cached = FOOTER_CACHE.getCompleted(footerCacheKey);
            if (cached != null && cached.covers(pos, (int) toRead)) {
                int from = (int) (pos - cached.startOffset());
                System.arraycopy(cached.bytes(), from, window, 0, (int) toRead);
                windowStart = pos;
                windowLength = (int) toRead;
                return;
            }

            if (isTailRead) {
                cached = FOOTER_CACHE.getOrAwaitPending(footerCacheKey);
                if (cached != null && cached.covers(pos, (int) toRead)) {
                    int from = (int) (pos - cached.startOffset());
                    System.arraycopy(cached.bytes(), from, window, 0, (int) toRead);
                    windowStart = pos;
                    windowLength = (int) toRead;
                    return;
                }

                CompletableFuture<FooterCacheEntry> future = new CompletableFuture<>();
                if (FOOTER_CACHE.tryRegisterPending(footerCacheKey, future)) {
                    try {
                        windowStart = pos;
                        windowLength = 0;
                        ByteBuffer target = ByteBuffer.wrap(window, 0, (int) toRead);
                        int bytesRead = storageObject.readBytes(pos, target);
                        windowLength = bytesRead < 0 ? 0 : bytesRead;
                        FOOTER_CACHE.completePending(footerCacheKey, windowStart, window, windowLength);
                    } catch (IOException e) {
                        FOOTER_CACHE.failPending(footerCacheKey);
                        throw e;
                    }
                } else {
                    cached = FOOTER_CACHE.getOrAwaitPending(footerCacheKey);
                    if (cached != null && cached.covers(pos, (int) toRead)) {
                        int from = (int) (pos - cached.startOffset());
                        System.arraycopy(cached.bytes(), from, window, 0, (int) toRead);
                        windowStart = pos;
                        windowLength = (int) toRead;
                    } else {
                        windowStart = pos;
                        windowLength = 0;
                        ByteBuffer target = ByteBuffer.wrap(window, 0, (int) toRead);
                        int bytesRead = storageObject.readBytes(pos, target);
                        windowLength = bytesRead < 0 ? 0 : bytesRead;
                    }
                }
            } else {
                windowStart = pos;
                windowLength = 0;
                ByteBuffer target = ByteBuffer.wrap(window, 0, (int) toRead);
                int bytesRead = storageObject.readBytes(pos, target);
                windowLength = bytesRead < 0 ? 0 : bytesRead;
            }
        }

        /**
         * Tries to fill the window from prefetched column chunk data. Finds the chunk whose
         * range contains the requested position and copies as many bytes as available into the
         * window buffer. Allows partial fills — the prefetched chunk does not need to cover
         * the full window size, just the start position.
         *
         * @return true if at least some data was filled from prefetched data
         */
        private boolean tryFillFromPrefetched(long pos, int toRead) {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = adapter.prefetchedChunks;
            if (chunks == null || chunks.isEmpty()) {
                return false;
            }
            Map.Entry<Long, ColumnChunkPrefetcher.PrefetchedChunk> entry = chunks.floorEntry(pos);
            if (entry == null) {
                return false;
            }
            ColumnChunkPrefetcher.PrefetchedChunk chunk = entry.getValue();
            if (pos < chunk.offset() || pos >= chunk.offset() + chunk.length()) {
                return false;
            }
            int offsetInChunk = (int) (pos - chunk.offset());
            ByteBuffer src = chunk.data().duplicate();
            src.position(offsetInChunk);
            int available = src.remaining();
            int toCopy = Math.min(toRead, available);
            src.get(window, 0, toCopy);
            windowStart = pos;
            windowLength = toCopy;
            return true;
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

    /**
     * SeekableInputStream that issues a fresh positional read for every request,
     * bypassing the sliding-window cache entirely. Correctness-safe fallback while
     * the window cache bug is investigated.
     *
     * <p>Not thread-safe: the single-byte scratch buffer and the {@code position} cursor are
     * mutable per-stream state. Each consumer must open its own stream.
     */
    private static class DirectSeekableInputStream extends SeekableInputStream {
        private final StorageObject storageObject;
        private final long length;
        // Reused on every single-byte read() to avoid allocating a fresh byte[1]/ByteBuffer pair
        // per call — critical on remote storage where that would translate to a 1-byte range GET.
        private final byte[] singleByte = new byte[1];
        private final ByteBuffer singleByteBuf = ByteBuffer.wrap(singleByte);
        private long position;
        private boolean closed;

        DirectSeekableInputStream(StorageObject storageObject, long length) {
            this.storageObject = storageObject;
            this.length = length;
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
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (position >= length) {
                return -1;
            }
            singleByteBuf.clear();
            int n = storageObject.readBytes(position, singleByteBuf);
            if (n <= 0) {
                return -1;
            }
            position++;
            return singleByte[0] & 0xFF;
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
            int toRead = (int) Math.min(len, length - position);
            ByteBuffer target = ByteBuffer.wrap(b, off, toRead);
            int bytesRead = storageObject.readBytes(position, target);
            if (bytesRead <= 0) {
                return -1;
            }
            position += bytesRead;
            return bytesRead;
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
            return (int) Math.min(length - position, Integer.MAX_VALUE);
        }

        @Override
        public void close() throws IOException {
            closed = true;
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

        /**
         * Reads into the caller's buffer in a single {@link StorageObject#readBytes} call, regardless
         * of whether it is heap-backed or direct. The buffer's {@code limit} is temporarily narrowed
         * so we never read past EOF, and restored before returning. This delegates any provider-specific
         * chunking (e.g. S3/HTTP direct-buffer transfer sizes) to the provider itself instead of
         * issuing many small range requests from this class.
         */
        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasRemaining() == false) {
                return 0;
            }
            if (closed) {
                throw new IOException("Stream is closed");
            }
            if (position >= length) {
                return -1;
            }
            int toRead = (int) Math.min(buf.remaining(), length - position);
            int savedLimit = buf.limit();
            buf.limit(buf.position() + toRead);
            int bytesRead;
            try {
                bytesRead = storageObject.readBytes(position, buf);
            } finally {
                buf.limit(savedLimit);
            }
            if (bytesRead <= 0) {
                return -1;
            }
            position += bytesRead;
            return bytesRead;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int bytesRead = read(buf);
                if (bytesRead < 0) {
                    throw new IOException("Reached end of stream before filling ByteBuffer");
                }
            }
        }
    }

    /**
     * Cache key for Parquet footer bytes. Uses {@code (path, length)} only — not {@code lastModified}
     * — so that all range splits of the same file share one cache entry regardless of any timing
     * jitter in {@link StorageObject#lastModified()}.
     *
     * <p>This is safe for immutable object stores (S3, GCS, Azure Blob) where objects are never
     * overwritten in place. For mutable filesystems (local, NFS), same-path same-length overwrites
     * can serve stale footer bytes until the entry is evicted or the JVM restarts.
     */
    // package-private for tests
    record FooterCacheKey(String path, long length) {}

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

    /**
     * JVM-wide footer cache with thundering-herd protection. Completed entries live in a
     * byte-budget LRU ({@code completed}); in-flight reads are tracked in a separate
     * {@code pending} map so concurrent callers coalesce into a single I/O via
     * {@link CompletableFuture}.
     *
     * <p>Thread-safety: {@code completed} and byte accounting are guarded by {@code synchronized(this)};
     * {@code pending} is a {@link ConcurrentHashMap} so registration/removal is lock-free.
     * {@link #clear()} synchronizes on both to ensure no orphaned futures.
     */
    // package-private for tests
    static class FooterCache {
        private final int maxBytes;
        private final int maxEntryBytes;
        private final LinkedHashMap<FooterCacheKey, FooterCacheEntry> completed = new LinkedHashMap<>(16, 0.75f, true);
        private final ConcurrentHashMap<FooterCacheKey, CompletableFuture<FooterCacheEntry>> pending = new ConcurrentHashMap<>();
        private int totalBytes;

        FooterCache(int maxBytes, int maxEntryBytes) {
            this.maxBytes = maxBytes;
            this.maxEntryBytes = maxEntryBytes;
        }

        /** Returns a completed cache entry, or {@code null}. Never blocks. */
        synchronized FooterCacheEntry getCompleted(FooterCacheKey key) {
            return completed.get(key);
        }

        /**
         * Returns a completed entry if available, otherwise awaits any in-flight pending read
         * for this key. If the pending read completes before this call, falls back to the
         * completed map. Returns {@code null} only when no entry exists and no pending read
         * is in progress.
         */
        FooterCacheEntry getOrAwaitPending(FooterCacheKey key) {
            synchronized (this) {
                FooterCacheEntry cached = completed.get(key);
                if (cached != null) {
                    return cached;
                }
            }
            CompletableFuture<FooterCacheEntry> future = pending.get(key);
            if (future != null) {
                FooterCacheEntry result = awaitFuture(future);
                if (result != null) {
                    return result;
                }
            }
            synchronized (this) {
                return completed.get(key);
            }
        }

        /**
         * Registers this caller as the owner of the pending read for the given key.
         * Returns {@code true} if registration succeeded (caller should perform I/O),
         * {@code false} if another caller already registered (caller should call
         * {@link #getOrAwaitPending}).
         */
        boolean tryRegisterPending(FooterCacheKey key, CompletableFuture<FooterCacheEntry> future) {
            return pending.putIfAbsent(key, future) == null;
        }

        /**
         * Completes a pending read: stores the entry in the LRU cache (if within size budget),
         * removes the pending future, and completes it so waiters unblock.
         *
         * <p>When the footer exceeds {@code maxEntryBytes}, it is not cached but the future is
         * still completed with {@code null} — waiters will fall through and perform their own I/O.
         * This matches the pre-coalescing behavior for oversized footers.
         */
        void completePending(FooterCacheKey key, long startOffset, byte[] buffer, int length) {
            FooterCacheEntry entry = null;
            if (length > 0 && length <= maxEntryBytes) {
                byte[] bytes = new byte[length];
                System.arraycopy(buffer, 0, bytes, 0, length);
                entry = new FooterCacheEntry(startOffset, bytes);
                synchronized (this) {
                    FooterCacheEntry previous = completed.put(key, entry);
                    if (previous != null) {
                        totalBytes -= previous.bytes().length;
                    }
                    totalBytes += bytes.length;
                    evictIfNeeded();
                }
            }
            CompletableFuture<FooterCacheEntry> future = pending.remove(key);
            if (future != null) {
                future.complete(entry);
            }
        }

        /** Signals that a pending read failed, allowing waiters to retry or fall through. */
        void failPending(FooterCacheKey key) {
            CompletableFuture<FooterCacheEntry> future = pending.remove(key);
            if (future != null) {
                future.complete(null);
            }
        }

        synchronized void putTailIfEligible(FooterCacheKey key, long startOffset, byte[] buffer, int length) {
            if (length <= 0 || length > maxEntryBytes) {
                return;
            }
            byte[] bytes = new byte[length];
            System.arraycopy(buffer, 0, bytes, 0, length);

            FooterCacheEntry previous = completed.put(key, new FooterCacheEntry(startOffset, bytes));
            if (previous != null) {
                totalBytes -= previous.bytes().length;
            }
            totalBytes += bytes.length;
            evictIfNeeded();
        }

        private void evictIfNeeded() {
            while (totalBytes > maxBytes && completed.isEmpty() == false) {
                Map.Entry<FooterCacheKey, FooterCacheEntry> eldest = completed.entrySet().iterator().next();
                FooterCacheEntry removed = eldest.getValue();
                completed.remove(eldest.getKey());
                totalBytes -= removed.bytes().length;
            }
        }

        /**
         * Clears all cached and pending entries. Pending futures are completed with {@code null}
         * before removal so that any thread blocked in {@link #getOrAwaitPending} unblocks.
         */
        synchronized void clear() {
            completed.clear();
            totalBytes = 0;
            for (Map.Entry<FooterCacheKey, CompletableFuture<FooterCacheEntry>> entry : pending.entrySet()) {
                entry.getValue().complete(null);
            }
            pending.clear();
        }

        private FooterCacheEntry awaitFuture(CompletableFuture<FooterCacheEntry> future) {
            try {
                return future.join();
            } catch (java.util.concurrent.CancellationException | java.util.concurrent.CompletionException e) {
                logger.debug("footer cache await failed", e);
                return null;
            }
        }
    }
}
