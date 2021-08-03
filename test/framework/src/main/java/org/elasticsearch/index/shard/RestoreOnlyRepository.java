/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.repositories.RepositoryData.MISSING_UUID;

/** A dummy repository for testing which just needs restore overridden */
public abstract class RestoreOnlyRepository extends AbstractLifecycleComponent implements Repository {
    private final String indexName;

    public RestoreOnlyRepository(String indexName) {
        this.indexName = indexName;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return null;
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return null;
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
        return null;
    }

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        final IndexId indexId = new IndexId(indexName, "blah");
        listener.onResponse(new RepositoryData(
                MISSING_UUID,
                EMPTY_REPO_GEN,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(indexId, emptyList()),
                ShardGenerations.EMPTY,
                IndexMetaDataGenerations.EMPTY,
                MISSING_UUID));
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        finalizeSnapshotContext.onResponse(null);
    }

    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<RepositoryData> listener) {
        listener.onResponse(null);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return 0;
    }

    @Override
    public String startVerification() {
        return null;
    }

    @Override
    public void endVerification(String verificationToken) {
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return null;
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
    }

    @Override
    public void updateState(final ClusterState state) {
    }

    @Override
    public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask, String source,
                                             Consumer<Exception> onFailure) {
        throw new UnsupportedOperationException("Unsupported for restore-only repository");
    }

    @Override
    public void cloneShardSnapshot(SnapshotId source, SnapshotId target, RepositoryShardId repositoryShardId, String shardGeneration,
                                   ActionListener<ShardSnapshotResult> listener) {
        throw new UnsupportedOperationException("Unsupported for restore-only repository");
    }
}
