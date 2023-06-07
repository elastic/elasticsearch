/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
        DiscoveryNodes nodes,
        SearchTransportService searchTransportService,
        Collection<SearchContextIdForNode> contextIds,
        ActionListener<Integer> listener
    ) {
        final Set<String> clusters = contextIds.stream()
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
                for (SearchContextIdForNode contextId : contextIds) {
                    final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());
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
        }));
    }
}
