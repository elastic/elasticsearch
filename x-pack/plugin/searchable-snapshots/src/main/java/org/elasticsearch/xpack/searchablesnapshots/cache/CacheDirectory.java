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

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        return new CacheBufferedIndexInput(name, fileLength(name), context);
    }

    @Override
    public void close() throws IOException {
        super.close();
        cacheService.removeFromCache(key -> key.startsWith(cacheDir.toString()));
    }

    private class CacheBufferedIndexInput extends BufferedIndexInput {

        private final String fileName;
        private final long fileLength;
        private final IOContext ioContext;
        private final long offset;
        private final long end;

        CacheBufferedIndexInput(String fileName, long fileLength, IOContext ioContext) {
            this(fileName, fileLength, ioContext, "CachedBufferedIndexInput(" + fileName + ")", 0L, fileLength);
        }

        private CacheBufferedIndexInput(String fileName, long fileLength, IOContext ioContext, String desc, long offset, long length) {
            super(desc, ioContext);
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.ioContext = ioContext;
            this.offset = offset;
            this.end = offset + length;
        }

        @Override
        public long length() {
            return end - offset;
        }

        @Override
        protected void readInternal(byte[] b, int off, int len) throws IOException {
            final Path cacheFile = cacheDir.resolve(fileName);
            final long pos = getFilePointer() + offset;
            cacheService.readFromCache(cacheFile, fileLength, () -> in.openInput(fileName, ioContext), pos, b, off, len);
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
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                    + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
            }
            String sliceDesc = getFullSliceDescription(sliceDescription);
            return new CacheBufferedIndexInput(fileName, fileLength, ioContext, sliceDesc, this.offset + offset, length);
        }

        @Override
        public void close() {
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
