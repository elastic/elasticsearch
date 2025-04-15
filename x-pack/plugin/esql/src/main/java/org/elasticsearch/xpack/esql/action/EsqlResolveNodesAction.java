/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsqlResolveNodesAction extends HandledTransportAction<
    EsqlResolveNodesAction.ResolveNodesRequest,
    EsqlResolveNodesAction.ResolveNodesResponse> {

    public static final String NAME = "indices:data/read/esql/resolve_nodes";
    public static final ActionType<ResolveNodesResponse> TYPE = new ActionType<>(NAME);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;

    @Inject
    public EsqlResolveNodesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            NAME,
            transportService,
            actionFilters,
            ResolveNodesRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION)
        );
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, ResolveNodesRequest request, ActionListener<ResolveNodesResponse> listener) {
        ActionListener.completeWith(listener, () -> {
            var project = projectResolver.getProjectState(clusterService.state());
            var nodes = Maps.<ShardId, List<DiscoveryNode>>newMapWithExpectedSize(request.shardIds.size());
            for (var shardId : request.shardIds) {
                nodes.put(
                    shardId,
                    project.routingTable()
                        .shardRoutingTable(shardId)
                        .allShards()
                        .map(shard -> project.cluster().nodes().get(shard.currentNodeId()))
                        .toList()
                );
            }
            return new ResolveNodesResponse(nodes);
        });
    }

    public static class ResolveNodesRequest extends ActionRequest {

        private final Set<ShardId> shardIds;

        public ResolveNodesRequest(Set<ShardId> shardIds) {
            this.shardIds = shardIds;
        }

        public ResolveNodesRequest(StreamInput in) throws IOException {
            this.shardIds = in.readCollectionAsImmutableSet(ShardId::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(shardIds, StreamOutput::writeWriteable);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class ResolveNodesResponse extends ActionResponse {

        private final Map<ShardId, List<DiscoveryNode>> nodes;

        public ResolveNodesResponse(Map<ShardId, List<DiscoveryNode>> nodes) {
            this.nodes = nodes;
        }

        public ResolveNodesResponse(StreamInput in) throws IOException {
            this.nodes = in.readMap(ShardId::new, inner -> inner.readCollectionAsList(DiscoveryNode::new));
        }

        public Map<ShardId, List<DiscoveryNode>> nodes() {
            return nodes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(nodes, StreamOutput::writeWriteable, StreamOutput::writeCollection);
        }
    }
}
