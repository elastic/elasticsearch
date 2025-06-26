/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse.RemoteClusterStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CancellableFanOut;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.RemoteClusterConnection;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Transport action implementing _cluster/stats API.
 */
public class TransportClusterStatsAction extends TransportNodesAction<
    ClusterStatsRequest,
    ClusterStatsResponse,
    TransportClusterStatsAction.ClusterStatsNodeRequest,
    ClusterStatsNodeResponse,
    SubscribableListener<TransportClusterStatsAction.AdditionalStats>> {

    public static final ActionType<ClusterStatsResponse> TYPE = new ActionType<>("cluster:monitor/stats");

    private static final CommonStatsFlags SHARD_STATS_FLAGS = new CommonStatsFlags(
        CommonStatsFlags.Flag.Docs,
        CommonStatsFlags.Flag.Store,
        CommonStatsFlags.Flag.FieldData,
        CommonStatsFlags.Flag.QueryCache,
        CommonStatsFlags.Flag.Completion,
        CommonStatsFlags.Flag.Segments,
        CommonStatsFlags.Flag.DenseVector,
        CommonStatsFlags.Flag.SparseVector
    );
    private static final Logger logger = LogManager.getLogger(TransportClusterStatsAction.class);

    private final Settings settings;
    private final NodeService nodeService;
    private final IndicesService indicesService;
    private final RepositoriesService repositoriesService;
    private final ProjectResolver projectResolver;
    private final SearchUsageHolder searchUsageHolder;
    private final CCSUsageTelemetry ccsUsageHolder;
    private final CCSUsageTelemetry esqlUsageHolder;

    private final Executor clusterStateStatsExecutor;
    private final MetadataStatsCache<MappingStats> mappingStatsCache;
    private final MetadataStatsCache<AnalysisStats> analysisStatsCache;
    private final RemoteClusterService remoteClusterService;

    @Inject
    public TransportClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        Client client,
        NodeService nodeService,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        ProjectResolver projectResolver,
        UsageService usageService,
        ActionFilters actionFilters,
        Settings settings
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            ClusterStatsNodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.nodeService = nodeService;
        this.indicesService = indicesService;
        this.repositoriesService = repositoriesService;
        this.projectResolver = projectResolver;
        this.searchUsageHolder = usageService.getSearchUsageHolder();
        this.ccsUsageHolder = usageService.getCcsUsageHolder();
        this.esqlUsageHolder = usageService.getEsqlUsageHolder();
        this.clusterStateStatsExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        this.mappingStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), MappingStats::of);
        this.analysisStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), AnalysisStats::of);
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.settings = settings;

        // register remote-cluster action with transport service only and not as a local-node Action that the Client can invoke
        new TransportRemoteClusterStatsAction(client, transportService, actionFilters);
    }

    @Override
    protected SubscribableListener<AdditionalStats> createActionContext(Task task, ClusterStatsRequest request) {
        assert task instanceof CancellableTask;
        final var cancellableTask = (CancellableTask) task;
        if (request.isRemoteStats() == false) {
            final var additionalStatsListener = new SubscribableListener<AdditionalStats>();
            final AdditionalStats additionalStats = new AdditionalStats();
            additionalStats.compute(cancellableTask, request, additionalStatsListener);
            return additionalStatsListener;
        } else {
            // For remote stats request, we don't need to compute anything
            return SubscribableListener.nullSuccess();
        }
    }

    @Override
    protected void newResponseAsync(
        final Task task,
        final ClusterStatsRequest request,
        final SubscribableListener<AdditionalStats> additionalStatsListener,
        final List<ClusterStatsNodeResponse> responses,
        final List<FailedNodeException> failures,
        final ActionListener<ClusterStatsResponse> listener
    ) {
        assert Transports.assertNotTransportThread(
            "Computation of mapping/analysis stats runs expensive computations on mappings found in "
                + "the cluster state that are too slow for a transport thread"
        );
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);

        additionalStatsListener.andThenApply(
            additionalStats -> request.isRemoteStats()
                // Return stripped down stats for remote clusters
                ? new ClusterStatsResponse(
                    System.currentTimeMillis(),
                    clusterService.state().metadata().clusterUUID(),
                    clusterService.getClusterName(),
                    responses,
                    List.of(),
                    null,
                    null,
                    null,
                    null,
                    Map.of()
                )
                : new ClusterStatsResponse(
                    System.currentTimeMillis(),
                    additionalStats.clusterUUID(),
                    clusterService.getClusterName(),
                    responses,
                    failures,
                    additionalStats.mappingStats(),
                    additionalStats.analysisStats(),
                    VersionStats.of(clusterService.state().metadata(), responses),
                    additionalStats.clusterSnapshotStats(),
                    additionalStats.getRemoteStats()
                )
        ).addListener(listener);
    }

    @Override
    protected ClusterStatsResponse newResponse(
        ClusterStatsRequest request,
        List<ClusterStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        assert false;
        throw new UnsupportedOperationException("use newResponseAsync instead");
    }

    @Override
    protected ClusterStatsNodeRequest newNodeRequest(ClusterStatsRequest request) {
        return new ClusterStatsNodeRequest();
    }

    @Override
    protected ClusterStatsNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    protected ClusterStatsNodeResponse nodeOperation(ClusterStatsNodeRequest nodeRequest, Task task) {
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, false, true, false, false, false);
        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            false,
            true,
            true,
            true,
            false,
            true,
            false,
            false,
            false,
            false,
            false,
            true,
            false,
            false,
            false,
            false
        );
        List<ShardStats> shardsStats = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                cancellableTask.ensureNotCancelled();
                if (indexShard.routingEntry() != null && indexShard.routingEntry().active()) {
                    // only report on fully started shards
                    CommitStats commitStats;
                    SeqNoStats seqNoStats;
                    RetentionLeaseStats retentionLeaseStats;
                    try {
                        commitStats = indexShard.commitStats();
                        seqNoStats = indexShard.seqNoStats();
                        retentionLeaseStats = indexShard.getRetentionLeaseStats();
                    } catch (final AlreadyClosedException e) {
                        // shard is closed - no stats is fine
                        commitStats = null;
                        seqNoStats = null;
                        retentionLeaseStats = null;
                    }
                    shardsStats.add(
                        new ShardStats(
                            indexShard.routingEntry(),
                            indexShard.shardPath(),
                            CommonStats.getShardLevelStats(indicesService.getIndicesQueryCache(), indexShard, SHARD_STATS_FLAGS),
                            commitStats,
                            seqNoStats,
                            retentionLeaseStats,
                            indexShard.isSearchIdle(),
                            indexShard.searchIdleTime()
                        )
                    );
                }
            }
        }

        final ClusterState clusterState = clusterService.state();

        @FixForMultiProject(description = "Should it be possible to execute this against the cluster rather than a specific project?")
        final ProjectMetadata project = projectResolver.getProjectMetadata(clusterState);
        final ClusterHealthStatus clusterStatus = clusterState.nodes().isLocalNodeElectedMaster()
            ? new ClusterStateHealth(clusterState, project.getConcreteAllIndices(), project.id()).getStatus()
            : null;

        final SearchUsageStats searchUsageStats = searchUsageHolder.getSearchUsageStats();

        final RepositoryUsageStats repositoryUsageStats = repositoriesService.getUsageStats();
        final CCSTelemetrySnapshot ccsTelemetry = ccsUsageHolder.getCCSTelemetrySnapshot();
        final CCSTelemetrySnapshot esqlTelemetry = esqlUsageHolder.getCCSTelemetrySnapshot();

        return new ClusterStatsNodeResponse(
            nodeInfo.getNode(),
            clusterStatus,
            nodeInfo,
            nodeStats,
            shardsStats.toArray(new ShardStats[shardsStats.size()]),
            searchUsageStats,
            repositoryUsageStats,
            ccsTelemetry,
            esqlTelemetry
        );
    }

    public static class ClusterStatsNodeRequest extends AbstractTransportRequest {

        ClusterStatsNodeRequest() {}

        public ClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    private static class MetadataStatsCache<T> extends CancellableSingleObjectCache<Metadata, Long, T> {
        private final BiFunction<Metadata, Runnable, T> function;

        MetadataStatsCache(ThreadContext threadContext, BiFunction<Metadata, Runnable, T> function) {
            super(threadContext);
            this.function = function;
        }

        @Override
        protected void refresh(
            Metadata metadata,
            Runnable ensureNotCancelled,
            BooleanSupplier supersedeIfStale,
            ActionListener<T> listener
        ) {
            ActionListener.completeWith(listener, () -> function.apply(metadata, ensureNotCancelled));
        }

        @Override
        protected Long getKey(Metadata indexMetadata) {
            return indexMetadata.version();
        }

        @Override
        protected boolean isFresh(Long currentKey, Long newKey) {
            return newKey <= currentKey;
        }
    }

    public final class AdditionalStats {

        private String clusterUUID;
        private MappingStats mappingStats;
        private AnalysisStats analysisStats;
        private ClusterSnapshotStats clusterSnapshotStats;
        private Map<String, RemoteClusterStats> remoteStats;

        void compute(CancellableTask task, ClusterStatsRequest request, ActionListener<AdditionalStats> listener) {
            clusterStateStatsExecutor.execute(ActionRunnable.wrap(listener, l -> {
                task.ensureNotCancelled();
                internalCompute(
                    task,
                    request,
                    clusterService.state(),
                    mappingStatsCache,
                    analysisStatsCache,
                    task::isCancelled,
                    clusterService.threadPool().absoluteTimeInMillis(),
                    l.map(ignored -> this)
                );
            }));
        }

        private void internalCompute(
            CancellableTask task,
            ClusterStatsRequest request,
            ClusterState clusterState,
            MetadataStatsCache<MappingStats> mappingStatsCache,
            MetadataStatsCache<AnalysisStats> analysisStatsCache,
            BooleanSupplier isCancelledSupplier,
            long absoluteTimeInMillis,
            ActionListener<Void> listener
        ) {
            try (var listeners = new RefCountingListener(listener)) {
                final var metadata = clusterState.metadata();
                clusterUUID = metadata.clusterUUID();
                mappingStatsCache.get(metadata, isCancelledSupplier, listeners.acquire(s -> mappingStats = s));
                analysisStatsCache.get(metadata, isCancelledSupplier, listeners.acquire(s -> analysisStats = s));
                clusterSnapshotStats = ClusterSnapshotStats.of(clusterState, absoluteTimeInMillis);
                if (doRemotes(request)) {
                    var remotes = remoteClusterService.getRegisteredRemoteClusterNames();
                    if (remotes.isEmpty()) {
                        remoteStats = Map.of();
                    } else {
                        new RemoteStatsFanout(task, transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)).start(
                            task,
                            remotes,
                            listeners.acquire(s -> remoteStats = s)
                        );
                    }
                }
            }
        }

        String clusterUUID() {
            return clusterUUID;
        }

        MappingStats mappingStats() {
            return mappingStats;
        }

        AnalysisStats analysisStats() {
            return analysisStats;
        }

        ClusterSnapshotStats clusterSnapshotStats() {
            return clusterSnapshotStats;
        }

        public Map<String, RemoteClusterStats> getRemoteStats() {
            return remoteStats;
        }
    }

    private boolean doRemotes(ClusterStatsRequest request) {
        return SearchService.CCS_COLLECT_TELEMETRY.get(settings) && request.doRemotes();
    }

    private class RemoteStatsFanout extends CancellableFanOut<String, RemoteClusterStatsResponse, Map<String, RemoteClusterStats>> {
        private final Executor requestExecutor;
        private final TaskId taskId;
        private Map<String, RemoteClusterStats> remoteClustersStats;

        RemoteStatsFanout(Task task, Executor requestExecutor) {
            this.requestExecutor = requestExecutor;
            this.taskId = new TaskId(clusterService.getNodeName(), task.getId());
        }

        @Override
        protected void sendItemRequest(String clusterAlias, ActionListener<RemoteClusterStatsResponse> listener) {
            var remoteClusterClient = remoteClusterService.getRemoteClusterClient(
                clusterAlias,
                requestExecutor,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
            );
            var remoteRequest = new RemoteClusterStatsRequest();
            remoteRequest.setParentTask(taskId);
            remoteClusterClient.getConnection(remoteRequest, listener.delegateFailureAndWrap((responseListener, connection) -> {
                if (connection.getTransportVersion().before(TransportVersions.V_8_16_0)) {
                    responseListener.onResponse(null);
                } else {
                    remoteClusterClient.execute(connection, TransportRemoteClusterStatsAction.REMOTE_TYPE, remoteRequest, responseListener);
                }
            }));
        }

        @Override
        protected void onItemResponse(String clusterAlias, RemoteClusterStatsResponse response) {
            if (response != null) {
                remoteClustersStats.computeIfPresent(clusterAlias, (k, v) -> v.acceptResponse(response));
            }
        }

        @Override
        protected void onItemFailure(String clusterAlias, Exception e) {
            logger.warn("Failed to get remote cluster stats for [{}]: {}", clusterAlias, e);
        }

        void start(Task task, Collection<String> remotes, ActionListener<Map<String, RemoteClusterStats>> listener) {
            this.remoteClustersStats = remotes.stream().collect(Collectors.toConcurrentMap(r -> r, this::makeRemoteClusterStats));
            super.run(task, remotes.iterator(), listener);
        }

        /**
         * Create static portion of RemoteClusterStats for a given cluster alias.
         */
        RemoteClusterStats makeRemoteClusterStats(String clusterAlias) {
            RemoteClusterConnection remoteConnection = remoteClusterService.getRemoteClusterConnection(clusterAlias);
            RemoteConnectionInfo remoteConnectionInfo = remoteConnection.getConnectionInfo();
            var compression = RemoteClusterService.REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings);
            return new RemoteClusterStats(
                remoteConnectionInfo.getModeInfo().modeName(),
                remoteConnection.isSkipUnavailable(),
                compression.toString()
            );
        }

        @Override
        protected Map<String, RemoteClusterStats> onCompletion() {
            return remoteClustersStats;
        }
    }

}
