/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    public class CacheBufferedIndexInput extends BufferedIndexInput {

        private final String fileName;
        private final long fileLength;
        private final Path file;
        private final IOContext ioContext;
        private final long offset;
        private final long end;

        /** a releasable provided by the CacheService that must be released when this IndexInput is done with the cached file **/
        private @Nullable Releasable releasable;
        private List<IndexInput> clones;
        private boolean closed;

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
            this.clones = new ArrayList<>();
        }

        @Override
        public long length() {
            return end - offset;
        }

        @Override
        protected void readInternal(final byte[] b, final int off, final int len) throws IOException {
            final long pos = getFilePointer() + offset;
            releasable =
                cacheService.readFromCache(file, fileLength, releasable, () -> in.openInput(fileName, ioContext), pos, b, off, len);
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
            clone.clones = new ArrayList<>();
            clone.releasable = null;
            clones.add(clone);
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
            clones.add(slice);
            return slice;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                if (releasable != null) {
                    Releasables.close(releasable);
                }
                if (clones != null) {
                    for (IndexInput clone : clones) {
                        try {
                            clone.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
            }
        }

        @Override
        public String toString() {
            return "CacheBufferedIndexInput{" +
                "desc='" + super.toString() + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileLength=" + fileLength +
                ", ioContext=" + ioContext +
                ", offset=" + offset +
                ", end=" + end +
                ", length=" + length() +
                '}';
        }
    }
}
