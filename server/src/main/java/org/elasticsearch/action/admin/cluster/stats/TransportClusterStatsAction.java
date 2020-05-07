/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodeResponse.ShardStatsAndIndexPatterns;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TransportClusterStatsAction extends TransportNodesAction<ClusterStatsRequest, ClusterStatsResponse,
        TransportClusterStatsAction.ClusterStatsNodeRequest, ClusterStatsNodeResponse> {

    private static final CommonStatsFlags SHARD_STATS_FLAGS = new CommonStatsFlags(CommonStatsFlags.Flag.Docs, CommonStatsFlags.Flag.Store,
        CommonStatsFlags.Flag.FieldData, CommonStatsFlags.Flag.QueryCache,
        CommonStatsFlags.Flag.Completion, CommonStatsFlags.Flag.Segments);

    private final NodeService nodeService;
    private final IndicesService indicesService;


    @Inject
    public TransportClusterStatsAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                       NodeService nodeService, IndicesService indicesService, ActionFilters actionFilters) {
        super(ClusterStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            ClusterStatsRequest::new, ClusterStatsNodeRequest::new, ThreadPool.Names.MANAGEMENT, ClusterStatsNodeResponse.class);
        this.nodeService = nodeService;
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterStatsResponse newResponse(ClusterStatsRequest request,
                                               List<ClusterStatsNodeResponse> responses, List<FailedNodeException> failures) {
        ClusterState state = clusterService.state();
        return new ClusterStatsResponse(
            System.currentTimeMillis(),
            state.metadata().clusterUUID(),
            clusterService.getClusterName(),
            responses,
            failures,
            state);
    }

    @Override
    protected ClusterStatsNodeRequest newNodeRequest(ClusterStatsRequest request) {
        return new ClusterStatsNodeRequest(request);
    }

    @Override
    protected ClusterStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    private static Function<String, List<String>> createIndexPatternMapper(Map<String, List<String>> indexPatterns) {
        List<Tuple<String, CharacterRunAutomaton>> indexPatternAutomata = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : indexPatterns.entrySet()) {
            Automaton matcher = Regex.simpleMatchToAutomaton(entry.getValue().toArray(String[]::new));
            indexPatternAutomata.add(new Tuple<>(entry.getKey(), new CharacterRunAutomaton(matcher)));
        }
        return indexName -> {
            List<String> matchingPatterns = new ArrayList<>();
            for (Tuple<String, CharacterRunAutomaton> matcher : indexPatternAutomata) {
                if (matcher.v2().run(indexName)) {
                    matchingPatterns.add(matcher.v1());
                }
            }
            return matchingPatterns;
        };
    }

    @Override
    protected ClusterStatsNodeResponse nodeOperation(ClusterStatsNodeRequest nodeRequest, Task task) {
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, true, false, false);
        NodeStats nodeStats = nodeService.stats(CommonStatsFlags.NONE,
                true, true, true, false, true, false, false, false, false, false, true, false, false);
        List<ShardStatsAndIndexPatterns> shardsStats = new ArrayList<>();
        Function<String, List<String>> indexPatternMatcher = createIndexPatternMapper(nodeRequest.request.indexPatterns());
        for (IndexService indexService : indicesService) {
            final List<String> matchingIndexPatterns = indexPatternMatcher.apply(indexService.index().getName());
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
                    ShardStats shardStats = new ShardStats(
                            indexShard.routingEntry(),
                            indexShard.shardPath(),
                            new CommonStats(indicesService.getIndicesQueryCache(), indexShard, SHARD_STATS_FLAGS),
                            commitStats,
                            seqNoStats,
                            retentionLeaseStats);
                    shardsStats.add(new ShardStatsAndIndexPatterns(shardStats, matchingIndexPatterns));
                }
            }
        }

        ClusterHealthStatus clusterStatus = null;
        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            clusterStatus = new ClusterStateHealth(clusterService.state()).getStatus();
        }

        return new ClusterStatsNodeResponse(nodeInfo.getNode(), clusterStatus, nodeInfo, nodeStats,
            shardsStats.toArray(ShardStatsAndIndexPatterns[]::new));

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
