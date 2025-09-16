/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TransportClosePointInTimeAction extends HandledTransportAction<ClosePointInTimeRequest, ClosePointInTimeResponse> {

    public static final ActionType<ClosePointInTimeResponse> TYPE = new ActionType<>("indices:data/read/close_point_in_time");
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportClosePointInTimeAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry,
        ProjectResolver projectResolver
    ) {
        super(TYPE.name(), transportService, actionFilters, ClosePointInTimeRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, ClosePointInTimeRequest request, ActionListener<ClosePointInTimeResponse> listener) {
        final SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, request.getId());
        final Map<ShardId, SearchContextIdForNode> shards = searchContextId.shards();
        DiscoveryNodes nodes = clusterService.state().nodes();
        logger.info("---> close PIT with [{}] contestIds: [{}] on nodes ", shards.size(), shards, nodes);
        closeContexts(
            clusterService,
            projectResolver,
            searchTransportService,
            shards,
            listener.map(freed -> new ClosePointInTimeResponse(freed == shards.size(), freed))
        );
    }

    /**
     * Closes the given context id and reports the number of freed contexts via the listener
     */
    public static void closeContexts(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        SearchTransportService searchTransportService,
        Map<ShardId, SearchContextIdForNode> shards,
        ActionListener<Integer> listener
    ) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        final Set<String> clusters = shards.values()
            .stream()
            .map(SearchContextIdForNode::getClusterAlias)
            .filter(clusterAlias -> Strings.isEmpty(clusterAlias) == false)
            .collect(Collectors.toSet());
        final ListenableFuture<BiFunction<String, String, DiscoveryNode>> lookupListener = new ListenableFuture<>();
        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> nodes.get(nodeId));
        } else {
            searchTransportService.getRemoteClusterService().collectNodes(clusters, lookupListener);
        }
        lookupListener.addListener(listener.delegateFailure((l, nodeLookup) -> {
            final var successes = new AtomicInteger();
            try (RefCountingRunnable refs = new RefCountingRunnable(() -> l.onResponse(successes.get()))) {
                for (ShardId shardId : shards.keySet()) {
                    SearchContextIdForNode contextId = shards.get(shardId);
                    if (contextId.getNode() == null) {
                        // the shardId was missing when creating the PIT, ignore.
                        continue;
                    }
                    // iterate over all nodes this shard can be located on currently
                    // TODO are we able to use this with remote clusters?
                    final ShardIterator shardIterator = OperationRouting.getShards(
                        clusterService.state().routingTable(projectResolver.getProjectId()),
                        shardId
                    );
                    List<ShardRouting> shardRoutings = shardIterator.getShardRoutings();

                    for (ShardRouting routing : shardRoutings) {
                        String nodeId = routing.currentNodeId();
                        final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), nodeId);
                        if (node != null) {
                            try {
                                searchTransportService.sendFreeContext(
                                    searchTransportService.getConnection(contextId.getClusterAlias(), node),
                                    contextId.getSearchContextId(),
                                    refs.acquireListener().map(r -> {
                                        if (r.isFreed()) {
                                            successes.incrementAndGet();
                                        }
                                        return null;
                                    })
                                );
                            } catch (Exception e) {
                                // ignored
                            }
                        }
                    }
                }
            }
        }));
    }
}
