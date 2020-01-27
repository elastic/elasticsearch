/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link CacheDirectory} uses a {@link CacheService} to cache Lucene files provided by another {@link Directory}.
 */
public class CacheDirectory extends FilterDirectory {

    private static final int COPY_BUFFER_SIZE = 8192;

    private final CacheService cacheService;
    private final Path cacheDir;

    public CacheDirectory(Directory in, CacheService cacheService, Path cacheDir) throws IOException {
        super(in);
        this.cacheService = Objects.requireNonNull(cacheService);
        this.cacheDir = Files.createDirectories(cacheDir);
    }

    public void close() throws IOException {
        super.close();
        // Ideally we could let the cache evict/remove cached files by itself after the
        // directory has been closed.
        cacheService.removeFromCache(key -> key.startsWith(cacheDir.toString()));
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        return new CacheBufferedIndexInput(name, fileLength(name), context);
    }

    public class CacheBufferedIndexInput extends BufferedIndexInput implements CacheFile.EvictionListener {

        private final String fileName;
        private final long fileLength;
        private final Path file;
        private final IOContext ioContext;
        private final long offset;
        private final long end;

        private @Nullable AtomicReference<CacheFile> cacheFile;
        private @Nullable CacheBufferedIndexInput parent;
        private AtomicBoolean closed;

        CacheBufferedIndexInput(String fileName, long fileLength, IOContext ioContext) {
            this(fileName, fileLength, ioContext, "CachedBufferedIndexInput(" + fileName + ")", 0L, fileLength);
        }

        private CacheBufferedIndexInput(String fileName, long fileLength, IOContext ioContext, String desc, long offset, long length) {
            super(desc, ioContext);
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.file = cacheDir.resolve(fileName);
            this.ioContext = ioContext;
            this.offset = offset;
            this.end = offset + length;
            this.cacheFile = new AtomicReference<>();
            this.closed = new AtomicBoolean(false);
        }

        @Override
        public long length() {
            return end - offset;
        }

        @Nullable
        private CacheFile getOrAcquire() throws Exception {
            assert parent == null : "should only be called on non-cloned index inputs";
            CacheFile currentCacheFile = cacheFile.get();
            if (currentCacheFile != null) {
                return currentCacheFile;
            }

            final CacheFile newCacheFile = cacheService.get(fileName, fileLength, file);
            synchronized (this) {
                currentCacheFile = cacheFile.get();
                if (currentCacheFile != null) {
                    return currentCacheFile;
                }
                if (newCacheFile.acquire(this)) {
                    final CacheFile previousCacheFile = cacheFile.getAndSet(newCacheFile);
                    assert previousCacheFile == null;
                    return newCacheFile;
                }
            }
            return null;
        }

        @Override
        public void onEviction(final CacheFile evictedCacheFile) {
            synchronized (this) {
                if (cacheFile.compareAndSet(evictedCacheFile, null)) {
                    evictedCacheFile.release(this);
                }
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                synchronized (this) {
                    final CacheFile currentCacheFile = cacheFile.getAndSet(null);
                    if (currentCacheFile != null) {
                        currentCacheFile.release(this);
                    }
                }
            }
        }

        @Override
        protected void readInternal(final byte[] buffer, final int offset, final int length) throws IOException {
            final long position = getFilePointer() + this.offset;

            int bytesRead = 0;
            while (bytesRead < length) {
                final long pos = position + bytesRead;
                final int off = offset + bytesRead;
                final int len = length - bytesRead;

                try {
                    final CacheFile cacheFile = (parent == null) ? getOrAcquire() : parent.getOrAcquire();
                    if (cacheFile == null) {
                        throw new AlreadyClosedException("Failed to acquire a non-evicted cache file");
                    }

                    try (ReleasableLock ignored = cacheFile.fileLock()) {
                        bytesRead += cacheFile.fetchRange(pos,
                            (start, end) -> readCacheFile(cacheFile.getChannel(), end, pos, buffer, off, len),
                            (start, end) -> writeCacheFile(cacheFile.getChannel(), start, end))
                            .get();
                    }
                } catch (final Exception e) {
                    if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
                        try {
                            // cache file was evicted during the range fetching, read bytes directly from source
                            bytesRead += readDirectly(pos, pos + len, buffer, off);
                            continue;
                        } catch (Exception inner) {
                            e.addSuppressed(inner);
                        }
                    }
                    throw new IOException("Fail to read data from cache", e);

                }
            }
            assert bytesRead == length : "partial read operation, read [" + bytesRead + "] bytes of [" + length + "]";
            assert parent == null || cacheFile.get() == null;
        }

        int readCacheFile(FileChannel fc, long end, long position, byte[] buffer, int offset, long length) throws IOException {
            assert assertFileChannelOpen(fc);
            return Channels.readFromFileChannel(fc, position, buffer, offset, Math.toIntExact(Math.min(length, end - position)));
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        void writeCacheFile(FileChannel fc, long start, long end) throws IOException {
            assert assertFileChannelOpen(fc);
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, end - start))];
            try (IndexInput input = in.openInput(fileName, ioContext)) {
                if (start > 0) {
                    input.seek(start);
                }
                int bytesCopied = 0;
                long remaining = end - start;
                while (remaining > 0) {
                    final int size = (remaining < copyBuffer.length) ? Math.toIntExact(remaining) : copyBuffer.length;
                    input.readBytes(copyBuffer, 0, size);
                    fc.write(ByteBuffer.wrap(copyBuffer, 0, size), start + bytesCopied);
                    bytesCopied += size;
                    remaining -= size;
                }
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length() + "] for " + toString());
            } else if (pos < 0L) {
                throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
            }
        }

        @Override
        public CacheBufferedIndexInput clone() {
            final CacheBufferedIndexInput clone = (CacheBufferedIndexInput) super.clone();
            clone.cacheFile = new AtomicReference<>();
            clone.closed = new AtomicBoolean(false);
            clone.parent = (this.parent != null ? this.parent : this);
            assert clone.parent.parent == null : "parent must not be a clone";
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                    + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }
            final CacheBufferedIndexInput slice = new CacheBufferedIndexInput(fileName, fileLength, ioContext,
                getFullSliceDescription(sliceDescription), this.offset + offset, length);
            slice.parent = (this.parent != null ? this.parent : this);
            assert slice.parent.parent == null : "parent must not be a clone";
            return slice;
        }

        @Override
        public String toString() {
            return "CacheBufferedIndexInput{" +
                "fileName='" + fileName + '\'' +
                ", fileLength=" + fileLength +
                ", offset=" + offset +
                ", end=" + end +
                ", length=" + length() +
                ", clone=" + (parent != null) +
                ", position=" + getFilePointer() +
                '}';
        }

        private int readDirectly(long start, long end, byte[] buffer, int offset) throws IOException {
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, end - start))];

            int bytesCopied = 0;
            try (IndexInput input = in.openInput(fileName, ioContext)) {
                if (start > 0) {
                    input.seek(start);
                }
                long remaining = end - start;
                while (remaining > 0) {
                    final int len = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                    input.readBytes(copyBuffer, 0, len);
                    System.arraycopy(copyBuffer, 0, buffer, offset + bytesCopied, len);
                    bytesCopied += len;
                    remaining -= len;
                }
            }
            return bytesCopied;
        }
    }

    private static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }
}
