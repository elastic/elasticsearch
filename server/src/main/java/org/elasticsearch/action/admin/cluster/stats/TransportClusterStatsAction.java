/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class TransportClusterStatsAction extends TransportNodesAction<ClusterStatsRequest, ClusterStatsResponse,
        TransportClusterStatsAction.ClusterStatsNodeRequest, ClusterStatsNodeResponse> {

    private static final CommonStatsFlags SHARD_STATS_FLAGS = new CommonStatsFlags(CommonStatsFlags.Flag.Docs, CommonStatsFlags.Flag.Store,
        CommonStatsFlags.Flag.FieldData, CommonStatsFlags.Flag.QueryCache,
        CommonStatsFlags.Flag.Completion, CommonStatsFlags.Flag.Segments);

    private final NodeService nodeService;
    private final IndicesService indicesService;

    private final MetadataStatsCache<MappingStats> mappingStatsCache = new MetadataStatsCache<>(MappingStats::of);
    private final MetadataStatsCache<AnalysisStats> analysisStatsCache = new MetadataStatsCache<>(AnalysisStats::of);

    @Inject
    public TransportClusterStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                       NodeService nodeService, IndicesService indicesService, ActionFilters actionFilters) {
        super(ClusterStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, ClusterStatsRequest::new,
                ClusterStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, ThreadPool.Names.MANAGEMENT, ClusterStatsNodeResponse.class);
        this.nodeService = nodeService;
        this.indicesService = indicesService;
    }

    @Override
    protected void newResponseAsync(
            final Task task,
            final ClusterStatsRequest request,
            final List<ClusterStatsNodeResponse> responses,
            final List<FailedNodeException> failures,
            final ActionListener<ClusterStatsResponse> listener) {
        assert Transports.assertNotTransportThread("Computation of mapping/analysis stats runs expensive computations on mappings found in "
                + "the cluster state that are too slow for a transport thread");
        assert Thread.currentThread().getName().contains("[" + ThreadPool.Names.MANAGEMENT + "]") : Thread.currentThread().getName();
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;
        final ClusterState state = clusterService.state();
        final Metadata metadata = state.metadata();

        final StepListener<MappingStats> mappingStatsStep = new StepListener<>();
        final StepListener<AnalysisStats> analysisStatsStep = new StepListener<>();
        mappingStatsCache.get(metadata, cancellableTask::isCancelled, mappingStatsStep);
        analysisStatsCache.get(metadata, cancellableTask::isCancelled, analysisStatsStep);
        mappingStatsStep.whenComplete(mappingStats -> analysisStatsStep.whenComplete(analysisStats -> ActionListener.completeWith(
                listener,
                () -> new ClusterStatsResponse(
                        System.currentTimeMillis(),
                        metadata.clusterUUID(),
                        clusterService.getClusterName(),
                        responses,
                        failures,
                        mappingStats,
                        analysisStats,
                        VersionStats.of(metadata, responses))
        ), listener::onFailure), listener::onFailure);
    }

    @Override
    protected ClusterStatsResponse newResponse(
            ClusterStatsRequest request,
            List<ClusterStatsNodeResponse> responses,
            List<FailedNodeException> failures) {
        assert false;
        throw new UnsupportedOperationException("use newResponseAsync instead");
    }

    @Override
    protected ClusterStatsNodeRequest newNodeRequest(ClusterStatsRequest request) {
        return new ClusterStatsNodeRequest(request);
    }

    @Override
    protected ClusterStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    protected ClusterStatsNodeResponse nodeOperation(ClusterStatsNodeRequest nodeRequest, Task task) {
        assert task instanceof CancellableTask;
        final CancellableTask cancellableTask = (CancellableTask) task;
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, true, false, false, false);
        NodeStats nodeStats = nodeService.stats(CommonStatsFlags.NONE,
                true, true, true, false, true, false, false, false, false, false, true, false, false, false);
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
                                    new CommonStats(indicesService.getIndicesQueryCache(), indexShard, SHARD_STATS_FLAGS),
                                    commitStats,
                                    seqNoStats,
                                    retentionLeaseStats));
                }
            }
        }

        ClusterHealthStatus clusterStatus = null;
        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            clusterStatus = new ClusterStateHealth(clusterService.state()).getStatus();
        }

        return new ClusterStatsNodeResponse(nodeInfo.getNode(), clusterStatus, nodeInfo, nodeStats,
            shardsStats.toArray(new ShardStats[shardsStats.size()]));

    }

    public static class ClusterStatsNodeRequest extends TransportRequest {

        ClusterStatsRequest request;

        public ClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new ClusterStatsRequest(in);
        }

        ClusterStatsNodeRequest(ClusterStatsRequest request) {
            this.request = request;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    private static class MetadataStatsCache<T> extends CancellableSingleObjectCache<Metadata, Long, T> {
        private final BiFunction<Metadata, Runnable, T> function;

        MetadataStatsCache(BiFunction<Metadata, Runnable, T> function) {
            this.function = function;
        }

        @Override
        protected void refresh(Metadata metadata, Runnable ensureNotCancelled, ActionListener<T> listener) {
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
