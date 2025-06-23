/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
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
         *
         * @param projectId the project-id for the repository or {@code null} if the repository is at the cluster level.
         * @param metadata  metadata for the repository including name and settings
         */
        Repository create(@Nullable ProjectId projectId, RepositoryMetadata metadata) throws Exception;

        /**
         * Constructs a repository.
         * @param projectId   the project-id for the repository or {@code null} if the repository is at the cluster level.
         * @param metadata    metadata for the repository including name and settings
         * @param typeLookup  a function that returns the repository factory for the given repository type.
         */
        default Repository create(
            @Nullable ProjectId projectId,
            RepositoryMetadata metadata,
            Function<String, Repository.Factory> typeLookup
        ) throws Exception {
            return create(projectId, metadata);
        }
    }

    /**
     * Get the project-id for the repository.
     *
     * @return the project-id, or {@code null} if the repository is at the cluster level.
     */
    @Nullable
    ProjectId getProjectId();

    /**
     * Returns metadata about this repository.
     */
    RepositoryMetadata getMetadata();

    /**
     * Reads a collection of {@link SnapshotInfo} instances from the repository.
     *
     * @param snapshotIds    The IDs of the snapshots whose {@link SnapshotInfo} instances should be retrieved.
     * @param abortOnFailure Whether to stop fetching further {@link SnapshotInfo} instances if a single fetch fails.
     * @param isCancelled    Supplies whether the enclosing task is cancelled, which should stop fetching {@link SnapshotInfo} instances.
     * @param consumer       A consumer for each {@link SnapshotInfo} retrieved. Called concurrently from multiple threads. If the consumer
     *                       throws an exception and {@code abortOnFailure} is {@code true} then the fetching will stop.
     * @param listener       If {@code abortOnFailure} is {@code true} and any operation fails then the failure is passed to this listener.
     *                       Also completed exceptionally on cancellation. Otherwise, completed once all requested {@link SnapshotInfo}
     *                       instances have been processed by the {@code consumer}.
     */
    void getSnapshotInfo(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        CheckedConsumer<SnapshotInfo, Exception> consumer,
        ActionListener<Void> listener
    );

    /**
     * Reads a single snapshot description from the repository
     *
     * @param snapshotId snapshot id to read description for
     * @param listener   listener to resolve with snapshot description (is resolved on the {@link ThreadPool.Names#SNAPSHOT_META} pool)
     */
    default void getSnapshotInfo(SnapshotId snapshotId, ActionListener<SnapshotInfo> listener) {
        getSnapshotInfo(List.of(snapshotId), true, () -> false, snapshotInfo -> {
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
        });
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
     * Fetches the {@link RepositoryData} and passes it into the listener. May completes the listener with a {@link RepositoryException} if
     * there is an error in reading the repository data.
     *
     * @param responseExecutor Executor to use to complete the listener if not using the calling thread. Using {@link
     *                         org.elasticsearch.common.util.concurrent.EsExecutors#DIRECT_EXECUTOR_SERVICE} means to complete the listener
     *                         on the thread which ultimately resolved the {@link RepositoryData}, which might be a low-latency transport or
     *                         cluster applier thread so make sure not to do anything slow or expensive in that case.
     * @param listener         Listener which is either completed on the calling thread (if the {@link RepositoryData} is immediately
     *                         available, e.g. from an in-memory cache), otherwise it is completed using {@code responseExecutor}.
     */
    void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener);

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
     * @param snapshotIds                  snapshot ids to delete
     * @param repositoryDataGeneration     the generation of the {@link RepositoryData} in the repository at the start of the deletion
     * @param minimumNodeVersion           the minimum {@link IndexVersion} across the nodes in the cluster, with which the repository
     *                                     format must remain compatible
     * @param repositoryDataUpdateListener listener completed when the {@link RepositoryData} is updated, or when the process fails
     *                                     without changing the repository contents - in either case, it is now safe for the next operation
     *                                     on this repository to proceed.
     * @param onCompletion                 action executed on completion of the cleanup actions that follow a successful
     *                                     {@link RepositoryData} update; not called if {@code repositoryDataUpdateListener} completes
     *                                     exceptionally.
     */
    void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
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
     * {@link SnapshotShardContext#status()} and call {@link IndexShardSnapshotStatus#ensureNotAborted()} to see if the snapshot process
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
    IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId);

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

    /**
     * @return a set of the names of the features that this repository instance uses, for reporting in the cluster stats for telemetry
     *         collection.
     */
    default Set<String> getUsageFeatures() {
        return Set.of();
    }

    static boolean assertSnapshotMetaThread() {
        return ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT_META);
    }
}
