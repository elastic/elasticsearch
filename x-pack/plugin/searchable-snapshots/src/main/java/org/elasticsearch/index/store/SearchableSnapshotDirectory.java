/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotShard;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * A read-only {@link org.apache.lucene.store.Directory} implementation that uses a {@link SearchableSnapshotShard}
 * to list and to read shard files stored in a snapshot.
 */
public class SearchableSnapshotDirectory extends BaseDirectory {

    private final SearchableSnapshotShard snapshotShard;
    private final int bufferSize;

    public SearchableSnapshotDirectory(final SearchableSnapshotShard snapshotShard, final int bufferSize) {
        super(new SingleInstanceLockFactory());
        this.snapshotShard = Objects.requireNonNull(snapshotShard);
        this.bufferSize = bufferSize;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return snapshotShard.listSnapshotFiles().keySet().stream()
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpenAndFileExists(name);
        final FileInfo fileInfo = snapshotShard.listSnapshotFiles().get(name);
        assert fileInfo != null;
        return fileInfo.length();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        return new SearchableSnapshotIndexInput(name, bufferSize, fileLength(name));
    }

    private void ensureOpenAndFileExists(final String name) throws IOException {
        ensureOpen();
        if (snapshotShard.listSnapshotFiles().containsKey(name) == false) {
            throw new FileNotFoundException(name);
        }
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
            + ", lockFactory=" + lockFactory
            + ", bufferSize=" + bufferSize;
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() throws IOException {
        throw unsupportedException();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        return new UnsupportedOperationException("Searchable snapshot directory does not support this operation");
    }

    /**
     * A {@link BufferedIndexInput} implementation that uses the directory's {@link SearchableSnapshotShard} to read a snapshot file
     */
    class SearchableSnapshotIndexInput extends BufferedIndexInput {

        private String name;
        private long length;
        private boolean closed;

        SearchableSnapshotIndexInput(String name, int bufferSize, long length) {
            super("SearchableSnapshotIndexInput(" + name + ")", bufferSize);
            this.name = Objects.requireNonNull(name);
            this.length = length;
            this.closed = false;
        }

        @Override
        protected void readInternal(byte[] b, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException(toString() + " is closed");
            }
            final ByteBuffer buffer = snapshotShard.readSnapshotFile(name, getFilePointer(), length);
            buffer.get(b, offset, length);
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
        public long length() {
            return length;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        @Override
        public String toString() {
            return "SearchableSnapshotIndexInput{name='" + name + ", length=" + length + '}' + System.identityHashCode(this);
        }
    }
}
