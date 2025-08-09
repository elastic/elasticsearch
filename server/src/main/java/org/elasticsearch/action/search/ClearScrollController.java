/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.action.search.TransportSearchHelper.parseScrollId;

public final class ClearScrollController implements Runnable {
    private final DiscoveryNodes nodes;
    private final SearchTransportService searchTransportService;
    private final RefCountingRunnable refs = new RefCountingRunnable(this::finish);
    private final ActionListener<ClearScrollResponse> listener;
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicInteger freedSearchContexts = new AtomicInteger(0);
    private final Logger logger;
    private static final Logger staticLogger = LogManager.getLogger(ClearScrollController.class);
    private final Runnable runner;

    ClearScrollController(
        ClearScrollRequest request,
        ActionListener<ClearScrollResponse> listener,
        DiscoveryNodes nodes,
        Logger logger,
        SearchTransportService searchTransportService
    ) {
        this.nodes = nodes;
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.listener = listener;
        List<String> scrollIds = request.getScrollIds();
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            runner = this::cleanAllScrolls;
        } else {
            // TODO: replace this with #closeContexts
            List<SearchContextIdForNode> contexts = new ArrayList<>();
            for (String scrollId : request.getScrollIds()) {
                SearchContextIdForNode[] context = parseScrollId(scrollId).getContext();
                Collections.addAll(contexts, context);
            }
            runner = () -> cleanScrollIds(contexts);
        }
    }

    @Override
    public void run() {
        runner.run();
    }

    void cleanAllScrolls() {
        try {
            for (final DiscoveryNode node : nodes) {
                try {
                    Transport.Connection connection = searchTransportService.getConnection(null, node);
                    searchTransportService.sendClearAllScrollContexts(connection, ActionListener.releaseAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(TransportResponse response) {
                            onFreedContext(true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onFailedFreedContext(e, node);
                        }
                    }, refs.acquire()));
                } catch (Exception e) {
                    onFailedFreedContext(e, node);
                }
            }
        } finally {
            refs.close();
        }
    }

    void cleanScrollIds(List<SearchContextIdForNode> contextIds) {
        SearchScrollAsyncAction.collectNodesAndRun(
            contextIds,
            nodes,
            searchTransportService,
            listener.delegateFailureAndWrap((l, lookup) -> {
                try {
                    for (SearchContextIdForNode target : contextIds) {
                        final DiscoveryNode node = lookup.apply(target.getClusterAlias(), target.getNode());
                        if (node == null) {
                            onFreedContext(false);
                        } else {
                            try {
                                Transport.Connection connection = searchTransportService.getConnection(target.getClusterAlias(), node);
                                searchTransportService.sendFreeContext(
                                    connection,
                                    target.getSearchContextId(),
                                    ActionListener.releaseAfter(
                                        ActionListener.wrap(freed -> onFreedContext(freed.isFreed()), e -> onFailedFreedContext(e, node)),
                                        refs.acquire()
                                    )
                                );
                            } catch (Exception e) {
                                onFailedFreedContext(e, node);
                            }
                        }
                    }
                } finally {
                    refs.close();
                }
            })
        );
    }

    private void onFreedContext(boolean freed) {
        if (freed) {
            freedSearchContexts.incrementAndGet();
        }
    }

    private void onFailedFreedContext(Throwable e, DiscoveryNode node) {
        logger.warn(() -> "Clear SC failed on node[" + node + "]", e);
        hasFailed.set(true);
    }

    private void finish() {
        listener.onResponse(new ClearScrollResponse(hasFailed.get() == false, freedSearchContexts.get()));
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
                for (Entry<ShardId, SearchContextIdForNode> entry : shards.entrySet()) {
                    var contextId = entry.getValue();
                    if (contextId.getNode() == null) {
                        // the shard was missing when creating the PIT, ignore.
                        continue;
                    }
                    final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());

                    Set<DiscoveryNode> targetNodes;
                    if (node != null) {
                        targetNodes = Collections.singleton(node);
                    } else {
                        staticLogger.info("---> missing node when closing context: " + contextId.getNode());
                        // TODO we won't be able to use this with remote clusters
                        IndexShardRoutingTable indexShardRoutingTable = clusterService.state()
                            .routingTable(projectResolver.getProjectId())
                            .shardRoutingTable(entry.getKey());
                        targetNodes = indexShardRoutingTable.assignedUnpromotableShards()
                            .stream()
                            .map(ShardRouting::currentNodeId)
                            .map(nodeId -> nodeLookup.apply(contextId.getClusterAlias(), nodeId))
                            .collect(Collectors.toSet());
                        staticLogger.info("---> trying alternative nodes to close context: " + targetNodes);
                    }
                    for (DiscoveryNode targetNode : targetNodes) {
                        try {
                            searchTransportService.sendFreeContext(
                                searchTransportService.getConnection(contextId.getClusterAlias(), targetNode),
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
        }));
    }
}
