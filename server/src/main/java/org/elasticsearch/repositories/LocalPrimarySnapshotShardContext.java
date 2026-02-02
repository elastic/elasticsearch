/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link SnapshotShardContext} implementation that reads data from a local primary shard for snapshotting.
 */
public final class LocalPrimarySnapshotShardContext extends SnapshotShardContext {

    private static final Logger logger = LogManager.getLogger(LocalPrimarySnapshotShardContext.class);

    private final Store store;
    private final MapperService mapperService;
    private final SnapshotIndexCommit commitRef;

    /**
     * @param store                 store to be snapshotted
     * @param mapperService         the shards mapper service
     * @param snapshotId            snapshot id
     * @param indexId               id for the index being snapshotted
     * @param commitRef             commit point reference
     * @param shardStateIdentifier  a unique identifier of the state of the shard that is stored with the shard's snapshot and used
     *                              to detect if the shard has changed between snapshots. If {@code null} is passed as the identifier
     *                              snapshotting will be done by inspecting the physical files referenced by {@code snapshotIndexCommit}
     * @param snapshotStatus        snapshot status
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param snapshotStartTime     start time of the snapshot found in
     *                              {@link org.elasticsearch.cluster.SnapshotsInProgress.Entry#startTime()}
     * @param listener              listener invoked on completion
     */
    public LocalPrimarySnapshotShardContext(
        Store store,
        MapperService mapperService,
        SnapshotId snapshotId,
        IndexId indexId,
        SnapshotIndexCommit commitRef,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        final long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(
            snapshotId,
            indexId,
            shardStateIdentifier,
            snapshotStatus,
            repositoryMetaVersion,
            snapshotStartTime,
            commitRef.closingBefore(listener)
        );
        this.store = store;
        this.mapperService = mapperService;
        this.commitRef = commitRef;
    }

    @Override
    public ShardId shardId() {
        return store.shardId();
    }

    public Store store() {
        return store;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexCommit indexCommit() {
        return commitRef.indexCommit();
    }

    @Override
    public Releasable withCommitRef() {
        status().ensureNotAborted(); // check this first to avoid acquiring a ref when aborted even if refs are available
        if (commitRef.tryIncRef()) {
            return Releasables.releaseOnce(commitRef::decRef);
        } else {
            status().ensureNotAborted();
            assert false : "commit ref closed early in state " + status();
            throw new IndexShardSnapshotFailedException(shardId(), "Store got closed concurrently");
        }
    }

    @Override
    public boolean isSearchableSnapshot() {
        return store.indexSettings().getIndexMetadata().isSearchableSnapshot();
    }

    @Override
    public Store.MetadataSnapshot metadataSnapshot() {
        final IndexCommit snapshotIndexCommit = indexCommit();
        logger.trace("[{}] [{}] Loading store metadata using index commit [{}]", shardId(), snapshotId(), snapshotIndexCommit);
        try {
            return store.getMetadata(snapshotIndexCommit);
        } catch (IOException e) {
            throw new IndexShardSnapshotFailedException(shardId(), "Failed to get store file metadata", e);
        }
    }

    @Override
    public Collection<String> fileNames() {
        final IndexCommit snapshotIndexCommit = indexCommit();
        try {
            return snapshotIndexCommit.getFileNames();
        } catch (IOException e) {
            throw new IndexShardSnapshotFailedException(shardId(), "Failed to get store file names", e);
        }
    }

    @Override
    public boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
        if (store.tryIncRef()) {
            try (IndexInput indexInput = store.openVerifyingInput(fileInfo.physicalName(), IOContext.READONCE, fileInfo.metadata())) {
                final byte[] tmp = new byte[Math.toIntExact(fileInfo.metadata().length())];
                indexInput.readBytes(tmp, 0, tmp.length);
                assert fileInfo.metadata().hash().bytesEquals(new BytesRef(tmp));
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                store.decRef();
            }
        } else {
            try {
                status().ensureNotAborted();
                assert false : "if the store is already closed we must have been aborted";
            } catch (Exception e) {
                assert e instanceof AbortedSnapshotException : e;
            }
        }
        return true;
    }

    @Override
    public void failStoreIfCorrupted(Exception e) {
        if (Lucene.isCorruptionException(e)) {
            try {
                store.markStoreCorrupted((IOException) e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("store cannot be marked as corrupted", inner);
            }
        }
    }

    @Override
    public SnapshotShardContext.FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException {
        Releasable commitRefReleasable = null;
        IndexInput indexInput = null;
        try {
            commitRefReleasable = withCommitRef();
            indexInput = store.openVerifyingInput(file, IOContext.DEFAULT, metadata);
            return new IndexInputFileReader(commitRefReleasable, indexInput);
        } catch (Exception e) {
            IOUtils.close(e, indexInput, commitRefReleasable);
            throw e;
        }
    }
}
