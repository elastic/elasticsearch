/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Context holding the state for creating a shard snapshot via {@link Repository#snapshotShard(SnapshotShardContext)}.
 * Wraps a {@link org.elasticsearch.index.engine.Engine.IndexCommitRef} that is released once this instances is completed by invoking
 * either its {@link #onResponse(ShardSnapshotResult)} or {@link #onFailure(Exception)} callback.
 */
public abstract class SnapshotShardContext extends DelegatingActionListener<ShardSnapshotResult, ShardSnapshotResult> {

    private final SnapshotId snapshotId;
    private final IndexId indexId;
    @Nullable
    private final String shardStateIdentifier;
    private final IndexShardSnapshotStatus snapshotStatus;
    private final IndexVersion repositoryMetaVersion;
    private final long snapshotStartTime;

    @SuppressWarnings("this-escape")
    protected SnapshotShardContext(
        SnapshotId snapshotId,
        IndexId indexId,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(new SubscribableListener<>());
        addListener(listener);
        this.snapshotId = snapshotId;
        this.indexId = indexId;
        this.shardStateIdentifier = shardStateIdentifier;
        this.snapshotStatus = snapshotStatus;
        this.repositoryMetaVersion = repositoryMetaVersion;
        this.snapshotStartTime = snapshotStartTime;
    }

    public SnapshotId snapshotId() {
        return snapshotId;
    }

    public IndexId indexId() {
        return indexId;
    }

    @Nullable
    public String stateIdentifier() {
        return shardStateIdentifier;
    }

    public IndexShardSnapshotStatus status() {
        return snapshotStatus;
    }

    public IndexVersion getRepositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public long snapshotStartTime() {
        return snapshotStartTime;
    }

    @Override
    public void onResponse(ShardSnapshotResult result) {
        delegate.onResponse(result);
    }

    public abstract ShardId shardId();

    public abstract Releasable withCommitRef();

    public abstract boolean isSearchableSnapshot();

    public abstract Store.MetadataSnapshot metadataSnapshot();

    public abstract Collection<String> fileNames();

    public abstract boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo);

    public abstract void failStoreIfCorrupted(Exception e);

    public abstract FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException;

    public interface FileReader extends Closeable {

        InputStream openInput(long limit) throws IOException;

        void verify() throws IOException;
    }

    public static class IndexInputFileReader implements FileReader {

        private final Releasable commitRefReleasable;
        private final IndexInput indexInput;

        public IndexInputFileReader(Releasable commitRefReleasable, IndexInput indexInput) {
            this.commitRefReleasable = commitRefReleasable;
            this.indexInput = indexInput;
        }

        @Override
        public InputStream openInput(long limit) throws IOException {
            return new InputStreamIndexInput(indexInput, limit);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexInput, commitRefReleasable);
        }

        @Override
        public void verify() throws IOException {
            Store.verify(indexInput);
        }
    }

    public void addListener(ActionListener<ShardSnapshotResult> listener) {
        ((SubscribableListener<ShardSnapshotResult>) this.delegate).addListener(listener);
    }
}
