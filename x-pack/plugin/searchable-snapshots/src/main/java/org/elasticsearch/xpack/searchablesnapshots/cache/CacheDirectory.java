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
        return new CacheBufferedIndexInput(name, fileLength(name), context);
    }

    private class CacheBufferedIndexInput extends BufferedIndexInput {

        private final String name;
        private final long length;
        private final IOContext context;

        public CacheBufferedIndexInput(String name, long length, IOContext context) {
            super("CachedBufferedIndexInput(" + name + ")", context);
            this.name = name;
            this.length = length;
            this.context = context;
        }

        @Override
        protected void readInternal(byte[] b, int offset, int length) throws IOException {
            try (IndexInput input = in.openInput(name, context)) {
                input.seek(getFilePointer());
                input.readBytes(b, offset, length);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length) {
                throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length + "] for " + toString());
            } else if (pos < 0L) {
                throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
            }
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public String toString() {
            return "CacheBufferedIndexInput{"
                + "name='" + name + '\''
                + ", length=" + length
                + '}';
        }
    }
}
