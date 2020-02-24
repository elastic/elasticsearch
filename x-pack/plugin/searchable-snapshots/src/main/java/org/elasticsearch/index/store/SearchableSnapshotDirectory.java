/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of {@link Directory} that exposes files from a snapshot as a Lucene directory. Because snapshot are immutable this
 * implementation does not allow modification of the directory files and only supports {@link #listAll()}, {@link #fileLength(String)} and
 * {@link #openInput(String, IOContext)} methods.
 *
 * To create a {@link SearchableSnapshotDirectory} both the list of the snapshot files and a {@link BlobContainer} to read these files must
 * be provided. The definition of the snapshot files are provided using a {@link BlobStoreIndexShardSnapshot} object which contains the name
 * of the snapshot and all the files it contains along with their metadata. Because there is no one-to-one relationship between the original
 * shard files and what it stored in the snapshot the {@link BlobStoreIndexShardSnapshot} is used to map a physical file name as expected by
 * Lucene with the one (or the ones) corresponding blob(s) in the snapshot.
 */
public class SearchableSnapshotDirectory extends BaseDirectory {

    private final BlobStoreIndexShardSnapshot snapshot;
    private final BlobContainer blobContainer;

    public SearchableSnapshotDirectory(final BlobStoreIndexShardSnapshot snapshot, final BlobContainer blobContainer) {
        super(new SingleInstanceLockFactory());
        this.snapshot = Objects.requireNonNull(snapshot);
        this.blobContainer = Objects.requireNonNull(blobContainer);
    }

    private FileInfo fileInfo(final String name) throws FileNotFoundException {
        return snapshot.indexFiles().stream()
            .filter(fileInfo -> fileInfo.physicalName().equals(name))
            .findFirst()
            .orElseThrow(() -> new FileNotFoundException(name));
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return snapshot.indexFiles().stream()
            .map(FileInfo::physicalName)
            .sorted(String::compareTo)
            .toArray(String[]::new);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        ensureOpen();
        return fileInfo(name).length();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        ensureOpen();
        return new SearchableSnapshotIndexInput(blobContainer, fileInfo(name), blobContainer.readBlobPreferredLength(),
            BufferedIndexInput.BUFFER_SIZE);
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + snapshot.snapshot() + " lockFactory=" + lockFactory;
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        return new UnsupportedOperationException("Searchable snapshot directory does not support this operation");
    }
}
