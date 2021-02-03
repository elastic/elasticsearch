/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.FailedNodeException;
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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportClusterStatsAction extends TransportNodesAction<ClusterStatsRequest, ClusterStatsResponse,
        TransportClusterStatsAction.ClusterStatsNodeRequest, ClusterStatsNodeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterStatsAction.class);

    private static final CommonStatsFlags SHARD_STATS_FLAGS = new CommonStatsFlags(CommonStatsFlags.Flag.Docs, CommonStatsFlags.Flag.Store,
        CommonStatsFlags.Flag.FieldData, CommonStatsFlags.Flag.QueryCache,
        CommonStatsFlags.Flag.Completion, CommonStatsFlags.Flag.Segments);

    private final NodeService nodeService;
    private final IndicesService indicesService;

    // guards #mappingStats, #analysisStats and #metaVersion
    private final Object statsMutex = new Object();

    private MappingStats mappingStats;
    private AnalysisStats analysisStats;
    private long metaVersion = -1L;

    @Inject
    public TransportClusterStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                       NodeService nodeService, IndicesService indicesService, ActionFilters actionFilters) {
        super(ClusterStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, ClusterStatsRequest::new,
                ClusterStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, ThreadPool.Names.MANAGEMENT, ClusterStatsNodeResponse.class);
        this.nodeService = nodeService;
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterStatsResponse newResponse(ClusterStatsRequest request,
                                               List<ClusterStatsNodeResponse> responses, List<FailedNodeException> failures) {
        assert Transports.assertNotTransportThread("Constructor of ClusterStatsResponse runs expensive computations on mappings found in" +
                " the cluster state that are too slow for a transport thread");
        final ClusterState state = clusterService.state();
        final Metadata metadata = state.metadata();
        MappingStats currentMappingStats = null;
        AnalysisStats currentAnalysisStats = null;
        // check if we already served a stats request for the current metadata version and have the stats cached
        synchronized (statsMutex) {
            if (metadata.version() == metaVersion) {
                logger.trace("Found cached mapping and analysis stats for metadata version [{}]", metadata.version());
                currentMappingStats = this.mappingStats;
                currentAnalysisStats = this.analysisStats;
            }
        }
        if (currentMappingStats == null) {
            // we didn't find any cached stats so we recompute them outside the mutex since the computation might be expensive for larger
            // cluster states
            logger.trace("Computing mapping and analysis stats for metadata version [{}]", metadata.version());
            currentMappingStats = MappingStats.of(metadata);
            currentAnalysisStats = AnalysisStats.of(metadata);
            synchronized (statsMutex) {
                // cache the computed stats unless they became outdated because of a concurrent cluster state update and a concurrent
                // stats request has already cached a newer version
                if (metadata.version() > metaVersion) {
                    logger.trace("Caching mapping and analysis stats for metadata version [{}]", metadata.version());
                    metaVersion = metadata.version();
                    this.mappingStats = currentMappingStats;
                    this.analysisStats = currentAnalysisStats;
                }
            }
        }
        VersionStats versionStats = VersionStats.of(metadata, responses);
        return new ClusterStatsResponse(
            System.currentTimeMillis(),
            state.metadata().clusterUUID(),
            clusterService.getClusterName(),
            responses,
            failures,
            currentMappingStats,
            currentAnalysisStats,
            versionStats);
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
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, true, false, false, false);
        NodeStats nodeStats = nodeService.stats(CommonStatsFlags.NONE,
                true, true, true, false, true, false, false, false, false, false, true, false, false, false);
        List<ShardStats> shardsStats = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
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
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
