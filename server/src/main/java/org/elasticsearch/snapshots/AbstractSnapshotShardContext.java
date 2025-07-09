/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.function.LongFunction;

public abstract class AbstractSnapshotShardContext extends DelegatingActionListener<ShardSnapshotResult, ShardSnapshotResult> {

    private final SnapshotId snapshotId;
    private final IndexId indexId;
    @Nullable
    private final String shardStateIdentifier;
    private final IndexShardSnapshotStatus snapshotStatus;
    private final IndexVersion repositoryMetaVersion;
    private final long snapshotStartTime;

    protected AbstractSnapshotShardContext(
        SnapshotId snapshotId,
        IndexId indexId,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(listener);
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

    protected abstract ShardId shardId();

    protected abstract Store store();

    protected abstract MapperService mapperService();

    protected abstract IndexCommit indexCommit();

    protected abstract Releasable withCommitRef();

    protected abstract boolean isSearchableSnapshot();

    protected abstract Store.MetadataSnapshot metadataSnapshot();

    protected abstract Collection<String> fileNames();

    protected abstract boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo);

    protected abstract void failStoreIfCorrupted(Exception e);

    protected abstract FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException;

    public interface FileReader extends LongFunction<InputStream>, Closeable {
        void verify() throws IOException;
    }

}
