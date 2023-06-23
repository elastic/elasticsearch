/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotDeleteListener;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * An interface for interacting with a repository in snapshot and restore.
 * <p>
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 * <p>
 * To perform a snapshot:
 * <ul>
 * <li>Data nodes call {@link Repository#snapshotShard}
 * for each shard</li>
 * <li>When all shard calls return master calls {@link #finalizeSnapshot} with possible list of failures</li>
 * </ul>
 */
public interface Repository extends LifecycleComponent {

    /**
     * An factory interface for constructing repositories.
     * See {@link org.elasticsearch.plugins.RepositoryPlugin}.
     */
    interface Factory {
        /**
         * Constructs a repository.
         * @param metadata    metadata for the repository including name and settings
         */
        Repository create(RepositoryMetadata metadata) throws Exception;

        default Repository create(RepositoryMetadata metadata, Function<String, Repository.Factory> typeLookup) throws Exception {
            return create(metadata);
        }
    }

    /**
     * Returns metadata about this repository.
     */
    RepositoryMetadata getMetadata();

    /**
     * Reads snapshot descriptions from the repository.
     *
     * @param context get-snapshot-info-context
     */
    void getSnapshotInfo(GetSnapshotInfoContext context);

    /**
     * Reads a single snapshot description from the repository
     *
     * @param snapshotId snapshot id to read description for
     * @param listener   listener to resolve with snapshot description (is resolved on the {@link ThreadPool.Names#SNAPSHOT_META} pool)
     */
    default void getSnapshotInfo(SnapshotId snapshotId, ActionListener<SnapshotInfo> listener) {
        getSnapshotInfo(new GetSnapshotInfoContext(List.of(snapshotId), true, () -> false, (context, snapshotInfo) -> {
            assert Repository.assertSnapshotMetaThread();
            listener.onResponse(snapshotInfo);
        }, new ActionListener<>() {
            @Override
            public void onResponse(Void o) {
                // ignored
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Returns global metadata associated with the snapshot.
     *
     * @param snapshotId the snapshot id to load the global metadata from
     * @return the global metadata about the snapshot
     */
    Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId);

    /**
     * Returns the index metadata associated with the snapshot.
     *
     * @param repositoryData current {@link RepositoryData}
     * @param snapshotId the snapshot id to load the index metadata from
     * @param index      the {@link IndexId} to load the metadata from
     * @return the index metadata about the given index for the given snapshot
     */
    IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException;

    /**
     * Returns a {@link RepositoryData} to describe the data in the repository, including the snapshots
     * and the indices across all snapshots found in the repository.  Throws a {@link RepositoryException}
     * if there was an error in reading the data.
     * @param listener listener that may be resolved on different kinds of threads including transport and cluster state applier threads
     *                 and therefore must fork to a new thread for executing any long running actions
     */
    void getRepositoryData(ActionListener<RepositoryData> listener);

    /**
     * Finalizes snapshotting process
     * <p>
     * This method is called on master after all shards are snapshotted.
     *
     * @param finalizeSnapshotContext finalization context
     */
    void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext);

    /**
     * Deletes snapshots
     *
     * @param snapshotIds           snapshot ids
     * @param repositoryStateId     the unique id identifying the state of the repository when the snapshot deletion began
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param listener              completion listener
     */
    void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        SnapshotDeleteListener listener
    );

    /**
     * Returns snapshot throttle time in nanoseconds
     */
    long getSnapshotThrottleTimeInNanos();

    /**
     * Returns restore throttle time in nanoseconds
     */
    long getRestoreThrottleTimeInNanos();

    /**
     * Returns stats on the repository usage
     */
    default RepositoryStats stats() {
        return RepositoryStats.EMPTY_STATS;
    }

    /**
     * Verifies repository on the master node and returns the verification token.
     * <p>
     * If the verification token is not null, it's passed to all data nodes for verification. If it's null - no
     * additional verification is required
     *
     * @return verification token that should be passed to all Index Shard Repositories for additional verification or null
     */
    String startVerification();

    /**
     * Called at the end of repository verification process.
     * <p>
     * This method should perform all necessary cleanup of the temporary files created in the repository
     *
     * @param verificationToken verification request generated by {@link #startVerification} command
     */
    void endVerification(String verificationToken);

    /**
     * Verifies repository settings on data node.
     * @param verificationToken value returned by {@link org.elasticsearch.repositories.Repository#startVerification()}
     * @param localNode         the local node information, for inclusion in verification errors
     */
    void verify(String verificationToken, DiscoveryNode localNode);

    /**
     * Returns true if the repository supports only read operations
     * @return true if the repository is read/only
     */
    boolean isReadOnly();

    /**
     * Creates a snapshot of the shard referenced by the given {@link SnapshotShardContext}.
     * <p>
     * As snapshot process progresses, implementation of this method should update {@link IndexShardSnapshotStatus} object returned by
     * {@link SnapshotShardContext#status()} and check its {@link IndexShardSnapshotStatus#isAborted()} to see if the snapshot process
     * should be aborted.
     *
     * @param snapshotShardContext snapshot shard context that must be completed via {@link SnapshotShardContext#onResponse} or
     *                             {@link SnapshotShardContext#onFailure}
     */
    void snapshotShard(SnapshotShardContext snapshotShardContext);

    /**
     * Restores snapshot of the shard.
     * <p>
     * The index can be renamed on restore, hence different {@code shardId} and {@code snapshotShardId} are supplied.
     * @param store           the store to restore the index into
     * @param snapshotId      snapshot id
     * @param indexId         id of the index in the repository from which the restore is occurring
     * @param snapshotShardId shard id (in the snapshot)
     * @param recoveryState   recovery state
     * @param listener        listener to invoke once done
     */
    void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    );

    /**
     * Retrieve shard snapshot status for the stored snapshot
     *
     * @param snapshotId snapshot id
     * @param indexId    the snapshotted index id for the shard to get status for
     * @param shardId    shard id
     * @return snapshot status
     */
    IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId);

    /**
     * Check if this instances {@link Settings} can be changed to the provided updated settings without recreating the repository.
     *
     * @param updatedSettings new repository settings
     * @param ignoredSettings setting names to ignore even if changed
     * @return true if the repository can be updated in place
     */
    default boolean canUpdateInPlace(Settings updatedSettings, Set<String> ignoredSettings) {
        return getMetadata().settings().equals(updatedSettings);
    }

    /**
     * Update the repository with the incoming cluster state. This method is invoked from {@link RepositoriesService#applyClusterState} and
     * thus the same semantics as with {@link org.elasticsearch.cluster.ClusterStateApplier#applyClusterState} apply for the
     * {@link ClusterState} that is passed here.
     *
     * @param state new cluster state
     */
    void updateState(ClusterState state);

    /**
     * Clones a shard snapshot.
     *
     * @param source          source snapshot
     * @param target          target snapshot
     * @param shardId         shard id
     * @param shardGeneration shard generation in repo
     * @param listener        listener to complete with new shard generation once clone has completed
     */
    void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        @Nullable ShardGeneration shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    );

    /**
     * Block until all in-flight operations for this repository have completed. Must only be called after this instance has been closed
     * by a call to stop {@link #close()}.
     * Waiting for ongoing operations should be implemented here instead of in {@link #stop()} or {@link #close()} hooks of this interface
     * as these are expected to be called on the cluster state applier thread (which must not block) if a repository is removed from the
     * cluster. This method is intended to be called on node shutdown instead as a means to ensure no repository operations are leaked.
     */
    void awaitIdle();

    static boolean assertSnapshotMetaThread() {
        return ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT_META);
    }
}
