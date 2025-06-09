/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.Type;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.CloseUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.CLOSED;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.DELETED;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.FAILURE;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.NO_LONGER_ASSIGNED;
import static org.elasticsearch.indices.cluster.IndexRemovalReason.REOPENED;

public class IndicesClusterStateService extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(IndicesClusterStateService.class);

    public static final Setting<TimeValue> SHARD_LOCK_RETRY_INTERVAL_SETTING = Setting.timeSetting(
        "indices.store.shard_lock_retry.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SHARD_LOCK_RETRY_TIMEOUT_SETTING = Setting.timeSetting(
        "indices.store.shard_lock_retry.timeout",
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * Maximum number of shards to try and close concurrently. Defaults to the smaller of {@code node.processors} and {@code 10}, but can be
     * set to any positive integer.
     */
    public static final Setting<Integer> CONCURRENT_SHARD_CLOSE_LIMIT = Setting.intSetting(
        "indices.store.max_concurrent_closing_shards",
        settings -> Integer.toString(Math.min(10, EsExecutors.NODE_PROCESSORS_SETTING.get(settings).roundUp())),
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope
    );

    final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final PeerRecoveryTargetService recoveryTargetService;
    private final ShardStateAction shardStateAction;

    private final Settings settings;
    // a list of shards that failed during recovery
    // we keep track of these shards in order to prevent repeated recovery of these shards on each cluster state update
    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();
    private final Map<ShardId, PendingShardCreation> pendingShardCreations = new HashMap<>();
    private final RepositoriesService repositoriesService;

    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final List<IndexEventListener> buildInIndexListener;
    private final PrimaryReplicaSyncer primaryReplicaSyncer;
    private final RetentionLeaseSyncer retentionLeaseSyncer;
    private final NodeClient client;
    private final TimeValue shardLockRetryInterval;
    private final TimeValue shardLockRetryTimeout;

    private final Executor shardCloseExecutor;

    @Inject
    public IndicesClusterStateService(
        final Settings settings,
        final IndicesService indicesService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final PeerRecoveryTargetService recoveryTargetService,
        final ShardStateAction shardStateAction,
        final RepositoriesService repositoriesService,
        final SearchService searchService,
        final PeerRecoverySourceService peerRecoverySourceService,
        final SnapshotShardsService snapshotShardsService,
        final PrimaryReplicaSyncer primaryReplicaSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final NodeClient client
    ) {
        this(
            settings,
            (AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>>) indicesService,
            clusterService,
            threadPool,
            recoveryTargetService,
            shardStateAction,
            repositoriesService,
            searchService,
            peerRecoverySourceService,
            snapshotShardsService,
            primaryReplicaSyncer,
            retentionLeaseSyncer,
            client
        );
    }

    // for tests
    IndicesClusterStateService(
        final Settings settings,
        final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final PeerRecoveryTargetService recoveryTargetService,
        final ShardStateAction shardStateAction,
        final RepositoriesService repositoriesService,
        final SearchService searchService,
        final PeerRecoverySourceService peerRecoverySourceService,
        final SnapshotShardsService snapshotShardsService,
        final PrimaryReplicaSyncer primaryReplicaSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final NodeClient client
    ) {
        this.settings = settings;
        this.buildInIndexListener = Arrays.asList(peerRecoverySourceService, recoveryTargetService, searchService, snapshotShardsService);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.repositoriesService = repositoriesService;
        this.primaryReplicaSyncer = primaryReplicaSyncer;
        this.retentionLeaseSyncer = retentionLeaseSyncer;
        this.client = client;
        this.shardLockRetryInterval = SHARD_LOCK_RETRY_INTERVAL_SETTING.get(settings);
        this.shardLockRetryTimeout = SHARD_LOCK_RETRY_TIMEOUT_SETTING.get(settings);
        this.shardCloseExecutor = new ShardCloseExecutor(settings, threadPool.generic());
    }

    @Override
    protected void doStart() {
        // Doesn't make sense to manage shards on non-data nodes
        if (DiscoveryNode.canContainData(settings)) {
            clusterService.addHighPriorityApplier(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.canContainData(settings)) {
            clusterService.removeApplier(this);
        }
    }

    @Override
    protected void doClose() {}

    /**
     * Completed when all the shards removed by earlier-applied cluster states have fully closed.
     * <p>
     * Kind of a hack tbh, we can't be sure the shard locks are fully released when this is completed so there's all sorts of retries and
     * other lenience to handle that. It'd be better to wait for the shard locks to be released and then delete the data. See #74149.
     */
    private volatile SubscribableListener<Void> lastClusterStateShardsClosedListener = SubscribableListener.nullSuccess();

    @Nullable // if not currently applying a cluster state
    private RefCountingListener currentClusterStateShardsClosedListeners;

    private ActionListener<Void> getShardsClosedListener() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        if (currentClusterStateShardsClosedListeners == null) {
            assert false : "not currently applying cluster state";
            return ActionListener.noop();
        } else {
            return currentClusterStateShardsClosedListeners.acquire();
        }
    }

    /**
     * @param action Action to run when all the shards removed by earlier-applied cluster states have fully closed. May run on the calling
     *               thread, or on the thread that completed the closing of the last such shard.
     */
    public void onClusterStateShardsClosed(Runnable action) {
        lastClusterStateShardsClosedListener.andThenAccept(ignored -> action.run());
    }

    @Override
    public synchronized void applyClusterState(final ClusterChangedEvent event) {
        final var previousShardsClosedListener = lastClusterStateShardsClosedListener;
        lastClusterStateShardsClosedListener = new SubscribableListener<>();
        currentClusterStateShardsClosedListeners = new RefCountingListener(lastClusterStateShardsClosedListener);
        try {
            previousShardsClosedListener.addListener(currentClusterStateShardsClosedListeners.acquire());
            doApplyClusterState(event);
        } finally {
            currentClusterStateShardsClosedListeners.close();
            currentClusterStateShardsClosedListeners = null;
        }
    }

    private void doApplyClusterState(final ClusterChangedEvent event) {
        if (lifecycle.started() == false) {
            return;
        }

        final ClusterState state = event.state();

        final DiscoveryNode currentMaster = state.nodes().getMasterNode();
        if (currentMaster != null && currentMaster.equals(event.previousState().nodes().getMasterNode()) == false) {
            // master node changed, clear request deduplicator so we send out new state update requests right away without waiting for
            // the in-flight ones to fail first
            shardStateAction.clearRemoteShardRequestDeduplicator();
        }

        // we need to clean the shards and indices we have on this node, since we
        // are going to recover them again once state persistence is disabled (no master / not recovered)
        // TODO: feels hacky, a block disables state persistence, and then we clean the allocated shards, maybe another flag in blocks?
        if (state.blocks().disableStatePersistence()) {
            for (AllocatedIndex<? extends Shard> indexService : indicesService) {
                // also cleans shards
                indicesService.removeIndex(
                    indexService.getIndexSettings().getIndex(),
                    NO_LONGER_ASSIGNED,
                    "cleaning index (disabled block persistence)",
                    shardCloseExecutor,
                    getShardsClosedListener()
                );
            }
            return;
        }

        updateFailedShardsCache(state);

        deleteIndices(event); // also deletes shards of deleted indices

        removeIndicesAndShards(event); // also removes shards of removed indices

        updateIndices(event); // can also fail shards, but these are then guaranteed to be in failedShardsCache

        createIndicesAndUpdateShards(state);
    }

    /**
     * Removes shard entries from the failed shards cache that are no longer allocated to this node by the master.
     * Sends shard failures for shards that are marked as actively allocated to this node but don't actually exist on the node.
     * Resends shard failures for shards that are still marked as allocated to this node but previously failed.
     *
     * @param state new cluster state
     */
    private void updateFailedShardsCache(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }

        DiscoveryNode masterNode = state.nodes().getMasterNode();

        // remove items from cache which are not in our routing table anymore and resend failures that have not executed on master yet
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext();) {
            ShardRouting failedShardRouting = iterator.next().getValue();
            ShardRouting matchedRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            if (matchedRouting == null || matchedRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            } else {
                if (masterNode != null) { // TODO: can we remove this? Is resending shard failures the responsibility of shardStateAction?
                    String message = "master " + masterNode + " has not removed previously failed shard. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", matchedRouting.shardId(), matchedRouting, message);
                    shardStateAction.localShardFailed(matchedRouting, message, null, ActionListener.noop(), state);
                }
            }
        }
    }

    // overrideable by tests
    protected void updateGlobalCheckpointForShard(final ShardId shardId) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            client.executeLocally(
                GlobalCheckpointSyncAction.TYPE,
                new GlobalCheckpointSyncAction.Request(shardId),
                ActionListener.wrap(r -> {}, e -> {
                    if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                        logger.info(() -> format("%s global checkpoint sync failed", shardId), e);
                    }
                })
            );
        }
    }

    /**
     * Deletes indices (with shard data).
     *
     * @param event cluster change event
     */
    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            final Optional<ProjectMetadata> project = previousState.metadata().lookupProject(index);
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            final SubscribableListener<Void> indexServiceClosedListener;
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                indexServiceClosedListener = SubscribableListener.newForked(
                    l -> indicesService.removeIndex(index, DELETED, "index no longer part of the metadata", shardCloseExecutor, l)
                );
            } else if (project.isPresent() && project.get().hasIndex(index)) {
                // The deleted index was part of the previous cluster state, but not loaded on the local node
                indexServiceClosedListener = SubscribableListener.nullSuccess();
                final IndexMetadata metadata = project.get().index(index);
                indexSettings = new IndexSettings(metadata, settings);
                final var projectId = project.get().id();
                indicesService.deleteUnassignedIndex(
                    "deleted index in project [" + projectId + "] was not assigned to local node",
                    metadata,
                    state.metadata().projects().get(projectId)
                );
            } else {
                // The previous cluster state's metadata also does not contain the index,
                // which is what happens on node startup when an index was deleted while the
                // node was not part of the cluster. In this case, try reading the index
                // metadata from disk. If its not there, there is nothing to delete.
                // First, though, verify the precondition for applying this case by
                // asserting that either this index is already in the graveyard, or the
                // previous cluster state is not initialized/recovered.
                assert state.metadata().projects().values().stream().anyMatch(p -> p.indexGraveyard().containsIndex(index))
                    || previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                indexServiceClosedListener = SubscribableListener.nullSuccess();
                final IndexMetadata metadata = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metadata != null) {
                    indexSettings = new IndexSettings(metadata, settings);
                } else {
                    indexSettings = null;
                }
            }
            if (indexSettings != null) {
                indexServiceClosedListener.andThenAccept(ignored -> threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> "[" + index + "] failed to complete pending deletion for index", e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        final TimeValue timeout = TimeValue.timeValueMinutes(30);
                        try {
                            // we are waiting until we can lock the index / all shards on the node and then we ack the delete of the store
                            // to the master. If we can't acquire the locks here immediately there might be a shard of this index still
                            // holding on to the lock due to a "currently canceled recovery" or so. The shard will delete itself BEFORE the
                            // lock is released so it's guaranteed to be deleted by the time we get the lock
                            indicesService.processPendingDeletes(index, indexSettings, timeout);
                        } catch (ShardLockObtainFailedException exc) {
                            logger.warn(
                                Strings.format("[%s] failed to lock all shards for index - timed out after [%s]]", index, timeout),
                                exc
                            );
                        } catch (InterruptedException e) {
                            logger.warn("[{}] failed to lock all shards for index - interrupted", index);
                        }
                    }

                    @Override
                    public String toString() {
                        return "processPendingDeletes[" + index + "]";
                    }
                }));
                indexServiceClosedListener.addListener(getShardsClosedListener());
            }
        }
    }

    /**
     * Removes indices that have no shards allocated to this node or indices whose state has changed. This does not delete the shard data
     * as we wait for enough shard copies to exist in the cluster before deleting shard data (triggered by
     * {@link org.elasticsearch.indices.store.IndicesStore}).
     * Also removes shards that are currently loaded by indicesService but have disappeared from the routing table of the current node.
     *
     * @param event the cluster changed event
     */
    private void removeIndicesAndShards(final ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;

        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.getIndexSettings().getIndex();
            final Optional<ProjectMetadata> project = state.metadata().lookupProject(index);
            final IndexMetadata indexMetadata = project.map(proj -> proj.index(index)).orElse(null);
            final IndexMetadata existingMetadata = indexService.getIndexSettings().getIndexMetadata();

            IndexRemovalReason reason = null;
            if (indexMetadata != null && indexMetadata.getState() != existingMetadata.getState()) {
                reason = indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : REOPENED;
            } else if (localRoutingNode == null || localRoutingNode.hasIndex(index) == false) {
                // if the cluster change indicates a brand new cluster, we only want
                // to remove the in-memory structures for the index and not delete the
                // contents on disk because the index will later be re-imported as a
                // dangling index
                assert indexMetadata != null || event.isNewCluster()
                    : "index "
                        + index
                        + " does not exist in the cluster state, it should either "
                        + "have been deleted or the cluster must be new";
                reason = indexMetadata != null && indexMetadata.getState() == IndexMetadata.State.CLOSE ? CLOSED : NO_LONGER_ASSIGNED;
            }

            if (reason != null) {
                logger.debug("{} removing index ({})", index, reason);
                indicesService.removeIndex(index, reason, "removing index (" + reason + ")", shardCloseExecutor, getShardsClosedListener());
            } else {
                // remove shards based on routing nodes (no deletion of data)
                for (Shard shard : indexService) {
                    ShardRouting currentRoutingEntry = shard.routingEntry();
                    ShardId shardId = currentRoutingEntry.shardId();
                    ShardRouting newShardRouting = localRoutingNode.getByShardId(shardId);
                    if (newShardRouting == null) {
                        // we can just remove the shard without cleaning it locally, since we will clean it in IndicesStore
                        // once all shards are allocated
                        logger.debug("{} removing shard (not allocated)", shardId);
                        indexService.removeShard(
                            shardId.id(),
                            "removing shard (not allocated)",
                            shardCloseExecutor,
                            getShardsClosedListener()
                        );
                    } else if (newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                        logger.debug(
                            "{} removing shard (stale allocation id, stale {}, new {})",
                            shardId,
                            currentRoutingEntry,
                            newShardRouting
                        );
                        indexService.removeShard(
                            shardId.id(),
                            "removing shard (stale copy)",
                            shardCloseExecutor,
                            getShardsClosedListener()
                        );
                    } else if (newShardRouting.initializing() && currentRoutingEntry.active()) {
                        // this can happen if the node was isolated/gc-ed, rejoins the cluster and a new shard with the same allocation id
                        // is assigned to it. Batch cluster state processing or if shard fetching completes before the node gets a new
                        // cluster state may result in a new shard being initialized while having the same allocation id as the currently
                        // started shard.
                        logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                        indexService.removeShard(
                            shardId.id(),
                            "removing shard (stale copy)",
                            shardCloseExecutor,
                            getShardsClosedListener()
                        );
                    } else if (newShardRouting.primary() && currentRoutingEntry.primary() == false && newShardRouting.initializing()) {
                        assert currentRoutingEntry.initializing() : currentRoutingEntry; // see above if clause
                        // this can happen when cluster state batching batches activation of the shard, closing an index, reopening it
                        // and assigning an initializing primary to this node
                        logger.debug("{} removing shard (not active, current {}, new {})", shardId, currentRoutingEntry, newShardRouting);
                        indexService.removeShard(
                            shardId.id(),
                            "removing shard (stale copy)",
                            shardCloseExecutor,
                            getShardsClosedListener()
                        );
                    }
                }
            }
        }
    }

    /**
     * Notifies master about shards that don't exist but are supposed to be active on this node, creates new shards that are supposed to
     * be initializing on this node and if needed updates the state of existing shards with the new cluster state.
     *
     * @param state new cluster state
     */
    private void createIndicesAndUpdateShards(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }

        // create map of indices to create with shards to fail if index creation fails or create or update shards if an existing index
        // service is found
        final Map<Index, List<ShardRouting>> indicesToCreate = new HashMap<>();
        for (ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId) == false) {
                final Index index = shardRouting.index();
                final var indexService = indicesService.indexService(index);
                if (shardRouting.initializing() == false && (indexService == null || indexService.getShardOrNull(shardId.id()) == null)) {
                    // the master thinks we are active, but we don't have this shard at all, mark it as failed
                    sendFailShard(
                        shardRouting,
                        "master marked shard as active, but shard has not been created, mark shard as failed",
                        null,
                        state
                    );
                } else {
                    if (indexService == null) {
                        indicesToCreate.computeIfAbsent(index, k -> new ArrayList<>()).add(shardRouting);
                    } else {
                        createOrUpdateShard(state, shardRouting, indexService);
                    }
                }
            }
        }

        for (Map.Entry<Index, List<ShardRouting>> entry : indicesToCreate.entrySet()) {
            final Index index = entry.getKey();

            AllocatedIndex<? extends Shard> indexService = null;
            try {
                final ProjectMetadata project = state.metadata().projectFor(index);
                final IndexMetadata indexMetadata = project.index(index);
                logger.debug("creating index [{}] in project [{}]", index, project.id());
                indexService = indicesService.createIndex(indexMetadata, buildInIndexListener, true);
                indexService.updateMapping(null, indexMetadata);
            } catch (Exception e) {
                final String failShardReason;
                if (indexService == null) {
                    failShardReason = "failed to create index";
                } else {
                    failShardReason = "failed to update mapping for index";
                    indicesService.removeIndex(
                        index,
                        FAILURE,
                        "removing index (mapping update failed)",
                        shardCloseExecutor,
                        getShardsClosedListener()
                    );
                }
                for (ShardRouting shardRouting : entry.getValue()) {
                    sendFailShard(shardRouting, failShardReason, e, state);
                }
                continue;
            }
            // we succeeded in creating the index service, so now we can create the missing shards assigned to this node
            for (ShardRouting shardRouting : entry.getValue()) {
                createOrUpdateShard(state, shardRouting, indexService);
            }
        }
    }

    private void createOrUpdateShard(ClusterState state, ShardRouting shardRouting, AllocatedIndex<? extends Shard> indexService) {
        Shard shard = indexService.getShardOrNull(shardRouting.shardId().id());
        if (shard == null) {
            assert shardRouting.initializing() : shardRouting + " should have been removed by failMissingShards";
            createShard(shardRouting, state);
        } else {
            updateShard(shardRouting, shard, state);
        }
    }

    private void updateIndices(ClusterChangedEvent event) {
        if (event.metadataChanged() == false) {
            return;
        }
        final ClusterState state = event.state();
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final IndexMetadata currentIndexMetadata = indexService.getIndexSettings().getIndexMetadata();
            final Index index = indexService.getIndexSettings().getIndex();
            final ProjectMetadata project = state.metadata().projectFor(index);
            final IndexMetadata newIndexMetadata = project.index(index);
            assert newIndexMetadata != null : "index " + index + " should have been removed by deleteIndices";
            if (ClusterChangedEvent.indexMetadataChanged(currentIndexMetadata, newIndexMetadata)) {
                String reason = null;
                try {
                    reason = "metadata update failed";
                    try {
                        indexService.updateMetadata(currentIndexMetadata, newIndexMetadata);
                    } catch (Exception e) {
                        assert false : e;
                        throw e;
                    }

                    reason = "mapping update failed";
                    indexService.updateMapping(currentIndexMetadata, newIndexMetadata);
                } catch (Exception e) {
                    indicesService.removeIndex(
                        index,
                        FAILURE,
                        "removing index (" + reason + ")",
                        shardCloseExecutor,
                        getShardsClosedListener()
                    );

                    // fail shards that would be created or updated by createOrUpdateShards
                    RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
                    if (localRoutingNode != null) {
                        for (final ShardRouting shardRouting : localRoutingNode) {
                            if (shardRouting.index().equals(index) && failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                                sendFailShard(shardRouting, "failed to update index (" + reason + ")", e, state);
                            }
                        }
                    }
                }
            }
        }
    }

    private void createShard(ShardRouting shardRouting, ClusterState state) {
        assert shardRouting.initializing() : "only allow shard creation for initializing shard but was " + shardRouting;
        final var shardId = shardRouting.shardId();

        try {
            final DiscoveryNode sourceNode;
            final ProjectMetadata project = state.metadata().projectFor(shardRouting.index());
            if (shardRouting.recoverySource().getType() == Type.PEER) {
                sourceNode = findSourceNodeForPeerRecovery(state.routingTable(project.id()), state.nodes(), shardRouting);
                if (sourceNode == null) {
                    logger.trace("ignoring initializing shard {} - no source node can be found.", shardId);
                    return;
                }
            } else if (shardRouting.recoverySource().getType() == Type.RESHARD_SPLIT_TARGET) {
                ShardId sourceShardId = ((RecoverySource.ReshardSplitTargetRecoverySource) shardRouting.recoverySource()).getSourceShardId();
                sourceNode = findSourceNodeForSplitTargetRecovery(state.routingTable(project.id()), state.nodes(), sourceShardId);
                if (sourceNode == null) {
                    logger.trace("ignoring initializing reshard target shard {} - no source node can be found.", shardId);
                    return;
                }
            } else {
                sourceNode = null;
            }
            final var primaryTerm = project.index(shardRouting.index()).primaryTerm(shardRouting.id());

            final var pendingShardCreation = createOrRefreshPendingShardCreation(shardId, state.stateUUID());
            createShardWhenLockAvailable(
                shardRouting,
                state,
                sourceNode,
                primaryTerm,
                0,
                0L,
                new RunOnce(() -> HotThreads.logLocalCurrentThreads(logger, Level.WARN, shardId + ": acquire shard lock for create")),
                ActionListener.runBefore(new ActionListener<>() {
                    @Override
                    public void onResponse(Boolean success) {
                        if (Boolean.TRUE.equals(success)) {
                            logger.debug("{} created shard with primary term [{}]", shardId, primaryTerm);
                        } else {
                            logger.debug("{} gave up while trying to create shard", shardId);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failAndRemoveShard(
                            shardRouting,
                            true,
                            "failed to create shard",
                            e,
                            state,
                            shardCloseExecutor,
                            ActionListener.noop() // on the failure path, did not create the shard, so don't need to wait for it to close
                        );
                    }
                }, () -> {
                    assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
                    pendingShardCreations.remove(shardId, pendingShardCreation);
                })
            );
        } catch (Exception e) {
            assert pendingShardCreations.get(shardId) == null
                || pendingShardCreations.get(shardId).clusterStateUUID().equals(state.stateUUID()) == false;
            failAndRemoveShard(shardRouting, true, "failed to create shard", e, state, shardCloseExecutor, getShardsClosedListener());
        }
    }

    private PendingShardCreation createOrRefreshPendingShardCreation(ShardId shardId, String clusterStateUUID) {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        final var currentPendingShardCreation = pendingShardCreations.get(shardId);
        final var newPendingShardCreation = new PendingShardCreation(
            clusterStateUUID,
            currentPendingShardCreation == null ? threadPool.relativeTimeInMillis() : currentPendingShardCreation.startTimeMillis()
        );
        pendingShardCreations.put(shardId, newPendingShardCreation);
        return newPendingShardCreation;
    }

    private void createShardWhenLockAvailable(
        ShardRouting shardRouting,
        ClusterState originalState,
        DiscoveryNode sourceNode,
        long primaryTerm,
        int iteration,
        long delayMillis,
        RunOnce dumpHotThreads,
        ActionListener<Boolean> listener
    ) {
        try {
            logger.debug("{} creating shard with primary term [{}], iteration [{}]", shardRouting.shardId(), primaryTerm, iteration);
            indicesService.createShard(
                shardRouting,
                recoveryTargetService,
                new RecoveryListener(shardRouting, primaryTerm),
                repositoriesService,
                failedShardHandler,
                this::updateGlobalCheckpointForShard,
                retentionLeaseSyncer,
                originalState.nodes().getLocalNode(),
                sourceNode,
                originalState.version()
            );
            listener.onResponse(true);
        } catch (ShardLockObtainFailedException e) {
            if (e.getCause() instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                logger.warn(format("interrupted while creating shard [%s]", shardRouting), e);
                listener.onFailure(e);
                return;
            }
            final Level level = (iteration + 25) % 30 == 0 ? Level.WARN : Level.DEBUG;
            logger.log(
                level,
                """
                    shard lock for [{}] has been unavailable for at least [{}/{}ms], \
                    attempting to create shard while applying cluster state [version={},uuid={}], will retry in [{}]: [{}]""",
                shardRouting,
                TimeValue.timeValueMillis(delayMillis),
                delayMillis,
                originalState.version(),
                originalState.stateUUID(),
                shardLockRetryInterval,
                e.getMessage()
            );
            if (level == Level.WARN) {
                dumpHotThreads.run();
            }
            // TODO could we instead subscribe to the shard lock and trigger the retry exactly when it is released rather than polling?
            threadPool.scheduleUnlessShuttingDown(
                shardLockRetryInterval,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                () -> clusterService.getClusterApplierService()
                    .runOnApplierThread("create shard " + shardRouting, Priority.NORMAL, currentState -> {

                        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
                        final var pendingShardCreation = pendingShardCreations.get(shardRouting.shardId());
                        if (pendingShardCreation == null) {
                            listener.onResponse(false);
                            return;
                        }

                        if (originalState.stateUUID().equals(currentState.stateUUID()) == false) {
                            logger.debug(
                                "cluster state updated from version [{}/{}] to version [{}/{}] before creation of shard {}",
                                originalState.version(),
                                originalState.stateUUID(),
                                currentState.version(),
                                currentState.stateUUID(),
                                shardRouting.shardId()
                            );
                            listener.onResponse(false);
                            return;
                        }
                        assert pendingShardCreation.clusterStateUUID().equals(currentState.stateUUID());

                        final var newDelayMillis = threadPool.relativeTimeInMillis() - pendingShardCreation.startTimeMillis();
                        if (newDelayMillis > shardLockRetryTimeout.millis()) {
                            logger.warn(
                                "timed out after [{}={}/{}ms] while waiting to acquire shard lock for {}",
                                SHARD_LOCK_RETRY_TIMEOUT_SETTING.getKey(),
                                shardLockRetryTimeout,
                                shardLockRetryTimeout.millis(),
                                shardRouting
                            );
                            dumpHotThreads.run();
                            listener.onFailure(
                                new ElasticsearchTimeoutException("timed out while waiting to acquire shard lock for " + shardRouting)
                            );
                            return;
                        }

                        final var indexService = indicesService.indexService(shardRouting.index());
                        if (indexService == null) {
                            final var message = "index service unexpectedly not found for " + shardRouting;
                            assert false : message;
                            listener.onFailure(new ElasticsearchException(message));
                            return;
                        }

                        if (indexService.getShardOrNull(shardRouting.shardId().id()) != null) {
                            final var message = "index shard unexpectedly found for " + shardRouting;
                            assert false : message;
                            listener.onFailure(new ElasticsearchException(message));
                            return;
                        }

                        createShardWhenLockAvailable(
                            shardRouting,
                            originalState,
                            sourceNode,
                            primaryTerm,
                            iteration + 1,
                            newDelayMillis,
                            dumpHotThreads,
                            listener
                        );

                    }, ActionListener.noop())
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void updateShard(ShardRouting shardRouting, Shard shard, ClusterState clusterState) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting)
            : "local shard has a different allocation id but wasn't cleaned by removeShards. "
                + "cluster state: "
                + shardRouting
                + " local: "
                + currentRoutingEntry;

        final long primaryTerm;
        try {
            final ProjectMetadata project = clusterState.metadata().projectFor(shard.shardId().getIndex());
            final IndexMetadata indexMetadata = project.index(shard.shardId().getIndex());
            primaryTerm = indexMetadata.primaryTerm(shard.shardId().id());
            final Set<String> inSyncIds = indexMetadata.inSyncAllocationIds(shard.shardId().id());
            final IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable(project.id())
                .shardRoutingTable(shardRouting.shardId());
            shard.updateShardState(
                shardRouting,
                primaryTerm,
                primaryReplicaSyncer::resync,
                clusterState.version(),
                inSyncIds,
                indexShardRoutingTable
            );
        } catch (Exception e) {
            failAndRemoveShard(
                shardRouting,
                true,
                "failed updating shard routing entry",
                e,
                clusterState,
                shardCloseExecutor,
                getShardsClosedListener()
            );
            return;
        }

        final IndexShardState state = shard.state();
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            // the master thinks we are initializing, but we are already started or on POST_RECOVERY and waiting
            // for master to confirm a shard started message (either master failover, or a cluster event before
            // we managed to tell the master we started), mark us as started
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "{} master marked shard as initializing, but shard has state [{}], resending shard started to {}",
                    shardRouting.shardId(),
                    state,
                    clusterState.nodes().getMasterNode()
                );
            }
            if (clusterState.nodes().getMasterNode() != null) {
                shardStateAction.shardStarted(
                    shardRouting,
                    primaryTerm,
                    "master "
                        + clusterState.nodes().getMasterNode()
                        + " marked shard as initializing, but shard state is ["
                        + state
                        + "], mark shard as started",
                    shard.getTimestampRange(),
                    shard.getEventIngestedRange(),
                    ActionListener.noop(),
                    clusterState
                );
            }
        }
    }

    /**
     * Finds the routing source node for peer recovery, return null if its not found. Note, this method expects the shard
     * routing to *require* peer recovery, use {@link ShardRouting#recoverySource()} to check if its needed or not.
     */
    private static DiscoveryNode findSourceNodeForPeerRecovery(RoutingTable routingTable, DiscoveryNodes nodes, ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        if (shardRouting.primary() == false) {
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            // only recover from started primary, if we can't find one, we will do it next round
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace(
                    "can't find relocation source node for shard {} because it is assigned to an unknown node [{}].",
                    shardRouting.shardId(),
                    shardRouting.relocatingNodeId()
                );
            }
        } else {
            throw new IllegalStateException(
                "trying to find source node for peer recovery when routing state means no peer recovery: " + shardRouting
            );
        }
        return sourceNode;
    }

    private static DiscoveryNode findSourceNodeForSplitTargetRecovery(
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        ShardId sourceShardId
    ) {
        ShardRouting sourceShardRouting = routingTable.shardRoutingTable(sourceShardId).primaryShard();

        if (sourceShardRouting.active() == false) {
            logger.trace("can't find reshard split source node because source shard {} is not active.", sourceShardRouting);
            return null;
        }

        DiscoveryNode sourceNode = nodes.get(sourceShardRouting.currentNodeId());
        if (sourceNode == null) {
            logger.trace(
                "can't find reshard split source node because source shard {} is assigned to an unknown node.",
                sourceShardRouting
            );
            return null;
        }
        return sourceNode;
    }

    private record PendingShardCreation(String clusterStateUUID, long startTimeMillis) {}

    private class RecoveryListener implements PeerRecoveryTargetService.RecoveryListener {

        /**
         * ShardRouting with which the shard was created
         */
        private final ShardRouting shardRouting;

        /**
         * Primary term with which the shard was created
         */
        private final long primaryTerm;

        private RecoveryListener(final ShardRouting shardRouting, final long primaryTerm) {
            this.shardRouting = shardRouting;
            this.primaryTerm = primaryTerm;
        }

        @Override
        public void onRecoveryDone(
            final RecoveryState state,
            ShardLongFieldRange timestampMillisFieldRange,
            ShardLongFieldRange eventIngestedMillisFieldRange
        ) {
            shardStateAction.shardStarted(
                shardRouting,
                primaryTerm,
                "after " + state.getRecoverySource(),
                timestampMillisFieldRange,
                eventIngestedMillisFieldRange,
                ActionListener.noop()
            );
        }

        @Override
        public void onRecoveryFailure(RecoveryFailedException e, boolean sendShardFailure) {
            handleRecoveryFailure(shardRouting, sendShardFailure, e);
        }
    }

    // package-private for testing
    synchronized void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Exception failure) {
        try {
            CloseUtils.executeDirectly(
                l -> failAndRemoveShard(
                    shardRouting,
                    sendShardFailure,
                    "failed recovery",
                    failure,
                    clusterService.state(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    l
                )
            );
        } catch (Exception e) {
            // should not be possible
            final var wrappedException = new IllegalStateException("unexpected failure in handleRecoveryFailure on " + shardRouting, e);
            logger.error(wrappedException.getMessage(), e);
            assert false : e;
        }
    }

    private void failAndRemoveShard(
        ShardRouting shardRouting,
        boolean sendShardFailure,
        String message,
        @Nullable Exception failure,
        ClusterState state,
        Executor shardCloseExecutor,
        ActionListener<Void> shardCloseListener
    ) {
        try (var listeners = new RefCountingListener(shardCloseListener)) {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                Shard shard = indexService.getShardOrNull(shardRouting.shardId().id());
                if (shard != null && shard.routingEntry().isSameAllocation(shardRouting)) {
                    indexService.removeShard(shardRouting.shardId().id(), message, shardCloseExecutor, listeners.acquire());
                }
            }
        } catch (ShardNotFoundException e) {
            // the node got closed on us, ignore it
        } catch (Exception inner) {
            inner.addSuppressed(failure);
            logger.warn(
                () -> format(
                    "[%s][%s] failed to remove shard after failure ([%s])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message
                ),
                inner
            );
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure, state);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Exception failure, ClusterState state) {
        try {
            logger.warn(() -> format("%s marking and sending shard failed due to [%s]", shardRouting.shardId(), message), failure);
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.localShardFailed(shardRouting, message, failure, ActionListener.noop(), state);
        } catch (Exception inner) {
            if (failure != null) inner.addSuppressed(failure);
            logger.warn(
                () -> format(
                    "[%s][%s] failed to mark shard as failed (because of [%s])",
                    shardRouting.getIndexName(),
                    shardRouting.getId(),
                    message
                ),
                inner
            );
        }
    }

    private class FailedShardHandler implements Consumer<IndexShard.ShardFailure> {
        @Override
        public void accept(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing();
            threadPool.generic().execute(() -> {
                synchronized (IndicesClusterStateService.this) {
                    ActionListener.run(ActionListener.assertOnce(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {}

                        @Override
                        public void onFailure(Exception e) {
                            final var wrappedException = new IllegalStateException(
                                "unexpected failure in FailedShardHandler on " + shardRouting,
                                e
                            );
                            logger.error(wrappedException.getMessage(), e);
                            assert false : e;
                        }
                    }),
                        l -> failAndRemoveShard(
                            shardRouting,
                            true,
                            "shard failure, reason [" + shardFailure.reason() + "]",
                            shardFailure.cause(),
                            clusterService.state(),
                            shardCloseExecutor,
                            l
                        )
                    );
                }
            });
        }
    }

    public interface Shard {

        /**
         * Returns the shard id of this shard.
         */
        ShardId shardId();

        /**
         * Returns the latest cluster routing entry received with this shard.
         */
        ShardRouting routingEntry();

        /**
         * Returns the latest internal shard state.
         */
        IndexShardState state();

        /**
         * Returns the recovery state associated with this shard.
         */
        RecoveryState recoveryState();

        /**
         * @return the range of the {@code @timestamp} field for this shard, or {@link ShardLongFieldRange#EMPTY} if this field is not
         * found, or {@link ShardLongFieldRange#UNKNOWN} if its range is not fixed.
         */
        @Nullable
        ShardLongFieldRange getTimestampRange();

        /**
         * @return the range of the {@code @event.ingested} field for this shard, or {@link ShardLongFieldRange#EMPTY} if this field is not
         * found, or {@link ShardLongFieldRange#UNKNOWN} if its range is not fixed.
         */
        @Nullable
        ShardLongFieldRange getEventIngestedRange();

        /**
         * Updates the shard state based on an incoming cluster state:
         * - Updates and persists the new routing value.
         * - Updates the primary term if this shard is a primary.
         * - Updates the allocation ids that are tracked by the shard if it is a primary.
         *   See {@link ReplicationTracker#updateFromMaster(long, Set, IndexShardRoutingTable)} for details.
         *
         * @param shardRouting                the new routing entry
         * @param primaryTerm                 the new primary term
         * @param primaryReplicaSyncer        the primary-replica resync action to trigger when a term is increased on a primary
         * @param applyingClusterStateVersion the cluster state version being applied when updating the allocation IDs from the master
         * @param inSyncAllocationIds         the allocation ids of the currently in-sync shard copies
         * @param routingTable                the shard routing table
         * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
         * @throws IOException                  if shard state could not be persisted
         */
        void updateShardState(
            ShardRouting shardRouting,
            long primaryTerm,
            BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
            long applyingClusterStateVersion,
            Set<String> inSyncAllocationIds,
            IndexShardRoutingTable routingTable
        ) throws IOException;
    }

    public interface AllocatedIndex<T extends Shard> extends Iterable<T> {

        /**
         * Returns the index settings of this index.
         */
        IndexSettings getIndexSettings();

        /**
         * Updates the metadata of this index. Changes become visible through {@link #getIndexSettings()}.
         *
         * @param currentIndexMetadata the current index metadata
         * @param newIndexMetadata the new index metadata
         */
        void updateMetadata(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata);

        /**
         * Updates the mappings by applying the incoming ones
         */
        void updateMapping(IndexMetadata currentIndexMetadata, IndexMetadata newIndexMetadata) throws IOException;

        /**
         * Returns shard with given id.
         */
        @Nullable
        T getShardOrNull(int shardId);

        /**
         * Removes shard with given id.
         */
        void removeShard(int shardId, String message, Executor closeExecutor, ActionListener<Void> closeListener);
    }

    public interface AllocatedIndices<T extends Shard, U extends AllocatedIndex<T>> extends Iterable<U> {

        /**
         * Creates a new {@link IndexService} for the given metadata.
         *
         * @param indexMetadata          the index metadata to create the index for
         * @param builtInIndexListener   a list of built-in lifecycle {@link IndexEventListener} that should should be used along side with
         *                               the per-index listeners
         * @param writeDanglingIndices   whether dangling indices information should be written
         * @throws ResourceAlreadyExistsException if the index already exists.
         */
        U createIndex(IndexMetadata indexMetadata, List<IndexEventListener> builtInIndexListener, boolean writeDanglingIndices)
            throws IOException;

        /**
         * Verify that the contents on disk for the given index is deleted; if not, delete the contents.
         * This method assumes that an index is already deleted in the cluster state and/or explicitly
         * through index tombstones.
         * @param index {@code Index} to make sure its deleted from disk
         * @param clusterState {@code ClusterState} to ensure the index is not part of it
         * @return IndexMetadata for the index loaded from disk
         */
        IndexMetadata verifyIndexIsDeleted(Index index, ClusterState clusterState);

        /**
         * Deletes an index that is not assigned to this node. This method cleans up all disk folders relating to the index
         * but does not deal with in-memory structures. For those call {@link #removeIndex}
         *
         * @param reason the reason why this index should be deleted
         * @param oldIndexMetadata the index metadata of the index that should be deleted
         * @param currentProject the <i>current</i> project metadata which is used to verify that the index does not exist in the project
         *                       anymore - can be null in case the whole project got deleted while there were still indices in it
         */
        void deleteUnassignedIndex(String reason, IndexMetadata oldIndexMetadata, @Nullable ProjectMetadata currentProject);

        /**
         * Removes the given index from this service and releases all associated resources. Persistent parts of the index
         * like the shards files, state and transaction logs are kept around in the case of a disaster recovery.
         *
         * @param index                the index to remove
         * @param reason               the reason to remove the index
         * @param extraInfo            extra information that will be used for logging and reporting
         * @param shardCloseExecutor   executor to use to close individual shards
         * @param shardsClosedListener listener which is completed when all shards have been closed
         */
        void removeIndex(
            Index index,
            IndexRemovalReason reason,
            String extraInfo,
            Executor shardCloseExecutor,
            ActionListener<Void> shardsClosedListener
        );

        /**
         * Returns an IndexService for the specified index if exists otherwise returns <code>null</code>.
         */
        @Nullable
        U indexService(Index index);

        /**
         * Creates a shard for the specified shard routing and starts recovery.
         *
         * @param shardRouting           the shard routing
         * @param recoveryTargetService  recovery service for the target
         * @param recoveryListener       a callback when recovery changes state (finishes or fails)
         * @param repositoriesService    service responsible for snapshot/restore
         * @param onShardFailure         a callback when this shard fails
         * @param globalCheckpointSyncer a callback when this shard syncs the global checkpoint
         * @param retentionLeaseSyncer   a callback when this shard syncs retention leases
         * @param targetNode             the node where this shard will be recovered
         * @param sourceNode             the source node to recover this shard from (it might be null)
         * @param clusterStateVersion    the cluster state version in which the shard was created
         * @throws IOException if an I/O exception occurs when creating the shard
         */
        void createShard(
            ShardRouting shardRouting,
            PeerRecoveryTargetService recoveryTargetService,
            PeerRecoveryTargetService.RecoveryListener recoveryListener,
            RepositoriesService repositoriesService,
            Consumer<IndexShard.ShardFailure> onShardFailure,
            GlobalCheckpointSyncer globalCheckpointSyncer,
            RetentionLeaseSyncer retentionLeaseSyncer,
            DiscoveryNode targetNode,
            @Nullable DiscoveryNode sourceNode,
            long clusterStateVersion
        ) throws IOException;

        /**
         * Returns shard for the specified id if it exists otherwise returns <code>null</code>.
         */
        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }

        void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException, InterruptedException,
            ShardLockObtainFailedException;
    }

    static class ShardCloseExecutor implements Executor {

        private final ThrottledTaskRunner throttledTaskRunner;

        ShardCloseExecutor(Settings settings, Executor delegate) {
            // Closing shards may involve IO so we don't want to do too many at once. We also currently have no backpressure mechanism so
            // could build up an unbounded queue of shards to close. We think it's unlikely in practice to see this: we won't see very many
            // of these tasks in nodes which are running normally, there's not that many shards moving off such nodes, and on a
            // shutting-down node we won't be starting up any new shards so the number of these tasks is bounded by the number of shards to
            // close. The bad case would be a normally-running node with very high churn in shards, starting up new shards so fast that it
            // can't close the old ones down fast enough. Maybe we could block or throttle new shards starting while old shards are still
            // shutting down, given that starting new shards is already async. Since this seems unlikely in practice, we opt for the simple
            // approach here.
            throttledTaskRunner = new ThrottledTaskRunner(
                IndicesClusterStateService.class.getCanonicalName(),
                CONCURRENT_SHARD_CLOSE_LIMIT.get(settings),
                delegate
            );
        }

        @Override
        public void execute(Runnable command) {
            throttledTaskRunner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        command.run();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // should be impossible, GENERIC pool doesn't reject anything
                    logger.error("unexpected failure running " + command.toString(), e);
                    assert false : new AssertionError("unexpected failure running " + command, e);
                }

                @Override
                public String toString() {
                    return command.toString();
                }
            });
        }
    }
}
