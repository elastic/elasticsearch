/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.directory;


import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.LegacyESVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.elasticsearch.index.store.remote.utils.TransferManager;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * a Directory implementation that can read directly from index snapshot stored remotely in a blob store repository.
 * This implementation is following this design https://github.com/opensearch-project/OpenSearch/issues/4033
 *
 * @opensearch.internal
 */
public final class RemoteSnapshotDirectory extends Directory {

    public static final Version SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION = LegacyESVersion.V_6_0_0;

    private static final String VIRTUAL_FILE_PREFIX = BlobStoreRepository.VIRTUAL_DATA_BLOB_PREFIX;

    private final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfoMap;
    private final FSDirectory localStoreDir;
    private final TransferManager transferManager;

    public RemoteSnapshotDirectory(BlobStoreIndexShardSnapshot snapshot, FSDirectory localStoreDir, TransferManager transferManager) {
        this.fileInfoMap = snapshot.indexFiles()
            .stream()
            .collect(Collectors.toMap(BlobStoreIndexShardSnapshot.FileInfo::physicalName, f -> f));
        this.localStoreDir = localStoreDir;
        this.transferManager = transferManager;
    }

    @Override
    public String[] listAll() throws IOException {
        return fileInfoMap.keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) throws IOException {}

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return NoopIndexOutput.INSTANCE;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfoMap.get(name);

        if (fileInfo.name().startsWith(VIRTUAL_FILE_PREFIX)) {
            return new ByteArrayIndexInput(fileInfo.physicalName(), fileInfo.metadata().hash().bytes);
        }
        return new OnDemandBlockSnapshotIndexInput(fileInfo, localStoreDir, transferManager);
    }

    @Override
    public void close() throws IOException {
        localStoreDir.close();
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fileInfoMap.get(name).length();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {}

    @Override
    public void syncMetaData() {}

    @Override
    public void rename(String source, String dest) throws IOException {}

    @Override
    public Lock obtainLock(String name) throws IOException {
        return NoLockFactory.INSTANCE.obtainLock(null, null);
    }

    static class NoopIndexOutput extends IndexOutput {

        final static NoopIndexOutput INSTANCE = new NoopIndexOutput();

        NoopIndexOutput() {
            super("noop", "noop");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long getFilePointer() {
            return 0;
        }

        @Override
        public long getChecksum() throws IOException {
            return 0;
        }

        @Override
        public void writeByte(byte b) throws IOException {

        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {

        }
    }
}
