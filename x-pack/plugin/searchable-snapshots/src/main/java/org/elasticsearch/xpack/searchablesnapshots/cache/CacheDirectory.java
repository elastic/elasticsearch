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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * {@link CacheDirectory} uses a {@link CacheService} to cache Lucene files provided by another {@link Directory}.
 */
public class CacheDirectory extends FilterDirectory {

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

        private volatile @Nullable CacheFile cacheFile;
        private volatile boolean closed;
        private boolean isClone;

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
        }

        @Override
        public long length() {
            return end - offset;
        }

        @Nullable
        private CacheFile getOrAcquire() throws Exception {
            CacheFile currentCacheFile = cacheFile;
            if (currentCacheFile == null) {
                final CacheFile newCacheFile = cacheService.get(fileName, fileLength, file);
                synchronized (this) {
                    if (cacheFile == null) {
                        if (newCacheFile.acquire(this)) {
                            cacheFile = newCacheFile;
                        }
                    }
                    currentCacheFile = cacheFile;
                }
            }
            return currentCacheFile;
        }

        @Override
        public void onEviction(final CacheFile evictedCacheFile) {
            synchronized (this) {
                assert cacheFile == evictedCacheFile || cacheFile == null;
                releaseAndClear(evictedCacheFile);
            }
        }

        private void releaseAndClear(final CacheFile cacheFile) {
            synchronized (this) {
                try {
                    cacheFile.release(this);
                } finally {
                    if (cacheFile == this.cacheFile) {
                        this.cacheFile = null;
                    }
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                if (cacheFile != null) {
                    releaseAndClear(cacheFile);
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

                CacheFile cacheFile = null;
                try {
                    cacheFile = getOrAcquire();
                    assert cacheFile != null;

                    final CacheFile.FileChannelRefCounted channelRef = cacheFile.getChannelRefCounter();
                    if (channelRef == null || channelRef.tryIncRef() == false) {
                        throw new AlreadyClosedException("Cache file acquired correctly but evicted before increment ref count on channel");
                    }
                    try {
                        bytesRead += cacheFile.fetchRange(pos,
                            (start, end) -> readCacheFile(channelRef.getChannel(), start, end, pos, buffer, off, len),
                            (start, end) -> writeCacheFile(channelRef.getChannel(), start, end))
                            .get();
                    } finally {
                        channelRef.decRef();
                    }
                } catch (Exception e) {
                    if (isEvictionException(e) == false) {
                        throw new IOException("Fail to read data from cache", e);
                    }
                    // cache file was evicted during the range fetching, read bytes directly from source
                    bytesRead += copySource(pos, pos + len, buffer, off);

                } finally {
                    if (bytesRead >= length) {
                        // once all bytes are read, clones immediately release the last acquired cache file
                        if (isClone  && cacheFile != null) {
                            releaseAndClear(cacheFile);
                        }
                    }
                }
            }
            assert bytesRead == length : "partial read operation, read [" + bytesRead + "] bytes of [" + length + "]";
        }

        int readCacheFile(FileChannel fc, long start, long end, long position, byte[] buffer, int offset, long length) throws IOException {
            return Channels.readFromFileChannel(fc, position, buffer, offset, Math.toIntExact(Math.min(length, end - position)));
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        int writeCacheFile(FileChannel fc, long start, long end) throws IOException {
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(8192L, end - start))];
            int bytesCopied = 0;
            try (IndexInput input = in.openInput(fileName, ioContext)) {
                if (start > 0) {
                    input.seek(start);
                }
                long remaining = end - start;
                while (remaining > 0) {
                    final int size = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                    input.readBytes(copyBuffer, 0, size);
                    fc.write(ByteBuffer.wrap(copyBuffer, 0, size), start + bytesCopied);
                    bytesCopied += size;
                    remaining -= size;
                }
            }
            return bytesCopied;
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
            clone.cacheFile = null;
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                    + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }
            final CacheBufferedIndexInput slice = new CacheBufferedIndexInput(fileName, fileLength, ioContext,
                getFullSliceDescription(sliceDescription), this.offset + offset, length);
            slice.isClone = true;
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
                ", clone=" + isClone +
                ", position=" + getFilePointer() +
                '}';
        }

        private int copySource(long start, long end, byte[] buffer, int offset) throws IOException {
            final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(8192L, end - start))];

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

    private static boolean isEvictionException(final Throwable t) {
        if (t instanceof AlreadyClosedException) {
            return true;
        } else if (t instanceof ExecutionException) {
            return isEvictionException(t.getCause());
        }
        return false;
    }
}
