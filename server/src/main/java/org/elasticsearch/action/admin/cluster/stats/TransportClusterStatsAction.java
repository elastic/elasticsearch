/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

public class TransportClusterStatsAction extends TransportNodesAction<
    ClusterStatsRequest,
    ClusterStatsResponse,
    TransportClusterStatsAction.ClusterStatsNodeRequest,
    ClusterStatsNodeResponse> {

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

    private final NodeService nodeService;
    private final IndicesService indicesService;
    private final RepositoriesService repositoriesService;
    private final SearchUsageHolder searchUsageHolder;
    private final CCSUsageTelemetry ccsUsageHolder;

    private final MetadataStatsCache<MappingStats> mappingStatsCache;
    private final MetadataStatsCache<AnalysisStats> analysisStatsCache;

    @Inject
    public TransportClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        UsageService usageService,
        ActionFilters actionFilters
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
        this.searchUsageHolder = usageService.getSearchUsageHolder();
        this.ccsUsageHolder = usageService.getCcsUsageHolder();
        this.mappingStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), MappingStats::of);
        this.analysisStatsCache = new MetadataStatsCache<>(threadPool.getThreadContext(), AnalysisStats::of);
    }

    @Override
    protected void newResponseAsync(
        final Task task,
        final ClusterStatsRequest request,
        final List<ClusterStatsNodeResponse> responses,
        final List<FailedNodeException> failures,
        final ActionListener<ClusterStatsResponse> listener
    ) {
        assert Transports.assertNotTransportThread(
            "Computation of mapping/analysis stats runs expensive computations on mappings found in "
                + "the cluster state that are too slow for a transport thread"
        );
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;
        final ClusterState state = clusterService.state();
        final Metadata metadata = state.metadata();
        final ClusterSnapshotStats clusterSnapshotStats = ClusterSnapshotStats.of(
            state,
            clusterService.threadPool().absoluteTimeInMillis()
        );

        final ListenableFuture<MappingStats> mappingStatsStep = new ListenableFuture<>();
        final ListenableFuture<AnalysisStats> analysisStatsStep = new ListenableFuture<>();
        mappingStatsCache.get(metadata, cancellableTask::isCancelled, mappingStatsStep);
        analysisStatsCache.get(metadata, cancellableTask::isCancelled, analysisStatsStep);
        mappingStatsStep.addListener(
            listener.delegateFailureAndWrap(
                (l, mappingStats) -> analysisStatsStep.addListener(
                    l.delegateFailureAndWrap(
                        (ll, analysisStats) -> ActionListener.completeWith(
                            ll,
                            () -> new ClusterStatsResponse(
                                System.currentTimeMillis(),
                                metadata.clusterUUID(),
                                clusterService.getClusterName(),
                                responses,
                                failures,
                                mappingStats,
                                analysisStats,
                                VersionStats.of(metadata, responses),
                                clusterSnapshotStats
                            )
                        )
                    )
                )
            )
        );
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
        final ClusterHealthStatus clusterStatus = clusterState.nodes().isLocalNodeElectedMaster()
            ? new ClusterStateHealth(clusterState).getStatus()
            : null;

        final SearchUsageStats searchUsageStats = searchUsageHolder.getSearchUsageStats();

        final RepositoryUsageStats repositoryUsageStats = repositoriesService.getUsageStats();
        final CCSTelemetrySnapshot ccsTelemetry = ccsUsageHolder.getCCSTelemetrySnapshot();

        return new ClusterStatsNodeResponse(
            nodeInfo.getNode(),
            clusterStatus,
            nodeInfo,
            nodeStats,
            shardsStats.toArray(new ShardStats[shardsStats.size()]),
            searchUsageStats,
            repositoryUsageStats,
            ccsTelemetry
        );
    }

    @UpdateForV9 // this can be replaced with TransportRequest.Empty in v9
    public static class ClusterStatsNodeRequest extends TransportRequest {

        ClusterStatsNodeRequest() {}

        public ClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
}
