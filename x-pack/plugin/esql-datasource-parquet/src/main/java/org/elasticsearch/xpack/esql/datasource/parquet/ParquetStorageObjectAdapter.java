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
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Adapter that wraps a StorageObject to implement Parquet's InputFile interface.
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
    private static final Logger logger = LogManager.getLogger(ParquetStorageObjectAdapter.class);

    private final StorageObject storageObject;
    private final long length;
    private final FooterByteCache.Key cacheKey;
    private final int windowSize;

    /**
     * Optional pre-warmed cache installed before {@code RowGroupFilter} runs. When set, reads
     * whose byte ranges fall inside a pre-fetched chunk are served from memory, bypassing the
     * synchronous range-GET path of the sliding window. This is used to coalesce dictionary
     * page and bloom filter reads for predicate columns into a single batched async fetch
     * instead of issuing one synchronous S3 GET per row group.
     *
     * <p>Installed and cleared via {@link #installPreWarmedChunks}. Each
     * {@link WindowedSeekableInputStream} re-reads this {@code volatile} field on every cache-miss
     * fetch, so an install or clear takes effect immediately even for streams that were already
     * open when the install happened — which matters because parquet-mr opens the file's
     * {@code SeekableInputStream} during {@code ParquetFileReader.open}, before the caller has had
     * a chance to install the cache. The map itself is expected to be unmodifiable (see
     * {@link PreloadedRowGroupMetadata#preWarmedChunks()}).
     */
    private volatile NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> preWarmedChunks;

    /** Default window size (4MB) for the sliding range cache. */
    static final int DEFAULT_WINDOW_SIZE = 4 * 1024 * 1024;

    /** Maximum window size (16MB). Caps adaptive window hints to prevent unbounded memory allocation. */
    static final int MAX_WINDOW_SIZE = 16 * 1024 * 1024;

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
        this.cacheKey = FooterByteCache.Key.keyFor(storageObject, this.length);
    }

    static void clearFooterCacheForTests() {
        FooterByteCache.getInstance().invalidateAll();
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        // Pass a supplier that re-reads the volatile field on every miss-path lookup. Streams
        // opened before {@link #installPreWarmedChunks} (notably the one parquet-mr opens at
        // {@code ParquetFileReader.open}) must still observe a later install, otherwise the
        // pre-warm optimization would be silently bypassed.
        return new WindowedSeekableInputStream(storageObject, cacheKey, length, windowSize, this::currentPreWarmedChunks);
    }

    private NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> currentPreWarmedChunks() {
        return preWarmedChunks;
    }

    /**
     * Installs a pre-warmed cache of byte ranges that subsequent {@link WindowedSeekableInputStream}
     * reads will consult before falling back to range-GET I/O. Intended for one-shot use during
     * {@code computeSurvivingRowGroups()}: the caller batches dictionary/bloom byte ranges via
     * {@link CoalescedRangeReader} and installs the result here, so parquet-mr's per-row-group
     * dictionary and bloom reads are served from memory instead of issuing N synchronous GETs.
     *
     * <p>Already-open streams observe the new map immediately on their next cache-miss fetch
     * because they re-read the {@code volatile} field through a supplier. Pass {@code null} or
     * an empty map to disable. Safe to call from a different thread than {@link #newStream()}
     * thanks to the {@code volatile} field, though typical usage is single-threaded.
     */
    void installPreWarmedChunks(NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks) {
        this.preWarmedChunks = (chunks == null || chunks.isEmpty()) ? null : chunks;
    }

    /**
     * SeekableInputStream backed by a sliding window cache over range reads.
     * Never calls {@link StorageObject#newStream()} (full GET) or {@link InputStream#skip(long)}.
     * On seek: if the target position is within the current window, only the cursor is updated;
     * otherwise a new range is fetched via {@link StorageObject#newStream(long, long)}.
     *
     * <p>Window fills use {@link StorageObject#newStream(long, long)} with chunked
     * {@link InputStream#read(byte[], int, int)} calls capped to {@link #STREAM_READ_CHUNK_SIZE}
     * to prevent the JDK's thread-local direct ByteBuffer pool from growing to window size.
     * The window is invalidated before each I/O so a partial-read failure never leaves
     * stale data visible to subsequent reads.
     */
    private static class WindowedSeekableInputStream extends SeekableInputStream {

        /** Caps each {@link InputStream#read(byte[], int, int)} to limit JDK thread-local direct buffer use. */
        private static final int STREAM_READ_CHUNK_SIZE = 256 * 1024;

        private final StorageObject storageObject;
        private final FooterByteCache.Key cacheKey;
        private final long length;
        private final int windowSize;
        private final byte[] window;

        /**
         * Supplier that returns the adapter's current pre-warmed chunks map (or {@code null}).
         * Re-read on every miss-path lookup so installs/clears that happen <em>after</em> this
         * stream was created take effect immediately. This is essential because parquet-mr opens
         * its single file stream during file open, before the caller has had a chance to install
         * the pre-warm map. The cost is one volatile read per cache-miss.
         */
        private final Supplier<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> preWarmedChunksSupplier;

        private long windowStart;
        private int windowLength;
        private long position;
        private boolean closed;

        WindowedSeekableInputStream(
            StorageObject storageObject,
            FooterByteCache.Key cacheKey,
            long length,
            int windowSize,
            Supplier<NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk>> preWarmedChunksSupplier
        ) {
            this.storageObject = storageObject;
            this.cacheKey = cacheKey;
            this.length = length;
            this.windowSize = windowSize;
            this.window = new byte[windowSize];
            this.preWarmedChunksSupplier = preWarmedChunksSupplier;
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

            if (fillFromPreWarmedChunk(pos, (int) toRead)) {
                return;
            }

            FooterByteCache tailCache = FooterByteCache.getInstance();
            if (fillFromTailCache(tailCache, pos, (int) toRead)) {
                return;
            }

            boolean isTailRead = pos + toRead == length;
            if (isTailRead && toRead <= tailCache.maxEntryBytes()) {
                try {
                    byte[] tailBytes = tailCache.getOrLoad(cacheKey, k -> readTailBytes(pos, (int) toRead));
                    if (tailBytes.length > 0 && fillFromCachedTail(tailBytes, pos, (int) toRead)) {
                        return;
                    }
                } catch (ExecutionException e) {
                    logger.debug("footer cache load failed; retrying direct I/O", e.getCause());
                }
            }

            windowStart = -1;
            windowLength = 0;

            int target = (int) toRead;
            try (InputStream in = storageObject.newStream(pos, toRead)) {
                int totalRead = 0;
                while (totalRead < target) {
                    int chunk = Math.min(STREAM_READ_CHUNK_SIZE, target - totalRead);
                    int n = in.read(window, totalRead, chunk);
                    if (n < 0) {
                        throw new IOException(
                            "Unexpected end of stream while filling window at position " + pos + "; read " + totalRead + " of " + target
                        );
                    }
                    if (n == 0 && chunk > 0) {
                        throw new IOException("InputStream.read returned 0 while " + chunk + " bytes were requested");
                    }
                    totalRead += n;
                }
                windowStart = pos;
                windowLength = totalRead;
            }

            if (windowLength > 0 && windowStart + windowLength == length) {
                byte[] tailBytes = new byte[windowLength];
                System.arraycopy(window, 0, tailBytes, 0, windowLength);
                tailCache.put(cacheKey, tailBytes);
            }
        }

        private byte[] readTailBytes(long pos, int toRead) throws IOException {
            byte[] buf = new byte[toRead];
            try (InputStream in = storageObject.newStream(pos, toRead)) {
                int totalRead = 0;
                while (totalRead < toRead) {
                    int chunk = Math.min(STREAM_READ_CHUNK_SIZE, toRead - totalRead);
                    int n = in.read(buf, totalRead, chunk);
                    if (n < 0) {
                        break;
                    }
                    totalRead += n;
                }
                if (totalRead <= 0) {
                    return new byte[0];
                }
                if (totalRead == toRead) {
                    return buf;
                }
                byte[] result = new byte[totalRead];
                System.arraycopy(buf, 0, result, 0, totalRead);
                return result;
            }
        }

        private boolean fillFromTailCache(FooterByteCache tailCache, long pos, int toRead) {
            byte[] cached = tailCache.get(cacheKey);
            return cached != null && fillFromCachedTail(cached, pos, toRead);
        }

        /**
         * Promotes a pre-warmed chunk into the window buffer when the request offset falls inside
         * one. Copies up to {@code toRead} bytes from the chunk into the window so subsequent
         * {@link #read(byte[], int, int)} calls observe the same window-based code path. Returns
         * {@code false} when no chunk covers the position; callers must then perform real I/O.
         */
        private boolean fillFromPreWarmedChunk(long pos, int toRead) {
            NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks = preWarmedChunksSupplier.get();
            if (chunks == null) {
                return false;
            }
            Map.Entry<Long, ColumnChunkPrefetcher.PrefetchedChunk> entry = chunks.floorEntry(pos);
            if (entry == null) {
                return false;
            }
            ColumnChunkPrefetcher.PrefetchedChunk chunk = entry.getValue();
            long chunkEnd = chunk.offset() + chunk.length();
            if (pos >= chunkEnd) {
                return false;
            }
            long availableInChunk = chunkEnd - pos;
            int copyLen = (int) Math.min(toRead, availableInChunk);

            // Defensive: chunk lengths in CoalescedRangeReader are int-bounded, so the offset
            // within the chunk must also fit. Falling back to range I/O on overflow is safer
            // than silently truncating the cast.
            int offsetInChunk;
            try {
                offsetInChunk = Math.toIntExact(pos - chunk.offset());
            } catch (ArithmeticException e) {
                return false;
            }

            // Invalidate before mutating — partial copies must never leave a half-populated window
            // visible if a later step throws. Copying from a heap ByteBuffer slice is allocation-free.
            windowStart = -1;
            windowLength = 0;
            ByteBuffer src = chunk.data().duplicate();
            src.position(src.position() + offsetInChunk);
            src.get(window, 0, copyLen);
            windowStart = pos;
            windowLength = copyLen;
            return true;
        }

        private boolean fillFromCachedTail(byte[] cached, long pos, int toRead) {
            long cachedStart = length - cached.length;
            if (pos >= cachedStart && pos + toRead <= length) {
                int from = (int) (pos - cachedStart);
                windowStart = -1;
                windowLength = 0;
                System.arraycopy(cached, from, window, 0, toRead);
                windowStart = pos;
                windowLength = toRead;
                return true;
            }
            return false;
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
}
