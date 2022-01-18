/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.CountDown;
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
    private final CountDown expectedOps;
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
        final int expectedOps;
        if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
            expectedOps = nodes.getSize();
            runner = this::cleanAllScrolls;
        } else {
            // TODO: replace this with #closeContexts
            List<SearchContextIdForNode> contexts = new ArrayList<>();
            for (String scrollId : request.getScrollIds()) {
                SearchContextIdForNode[] context = parseScrollId(scrollId).getContext();
                Collections.addAll(contexts, context);
            }
            if (contexts.isEmpty()) {
                expectedOps = 0;
                runner = () -> listener.onResponse(new ClearScrollResponse(true, 0));
            } else {
                expectedOps = contexts.size();
                runner = () -> cleanScrollIds(contexts);
            }
        }
        this.expectedOps = new CountDown(expectedOps);

    }

    @Override
    public void run() {
        runner.run();
    }

    void cleanAllScrolls() {
        for (final DiscoveryNode node : nodes) {
            try {
                Transport.Connection connection = searchTransportService.getConnection(null, node);
                searchTransportService.sendClearAllScrollContexts(connection, new ActionListener<TransportResponse>() {
                    @Override
                    public void onResponse(TransportResponse response) {
                        onFreedContext(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailedFreedContext(e, node);
                    }
                });
            } catch (Exception e) {
                onFailedFreedContext(e, node);
            }
        }
    }

    void cleanScrollIds(List<SearchContextIdForNode> contextIds) {
        SearchScrollAsyncAction.collectNodesAndRun(contextIds, nodes, searchTransportService, ActionListener.wrap(lookup -> {
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
                            ActionListener.wrap(freed -> onFreedContext(freed.isFreed()), e -> onFailedFreedContext(e, node))
                        );
                    } catch (Exception e) {
                        onFailedFreedContext(e, node);
                    }
                }
            }
        }, listener::onFailure));
    }

    private void onFreedContext(boolean freed) {
        if (freed) {
            freedSearchContexts.incrementAndGet();
        }
        if (expectedOps.countDown()) {
            boolean succeeded = hasFailed.get() == false;
            listener.onResponse(new ClearScrollResponse(succeeded, freedSearchContexts.get()));
        }
    }

    private void onFailedFreedContext(Throwable e, DiscoveryNode node) {
        logger.warn(() -> new ParameterizedMessage("Clear SC failed on node[{}]", node), e);
        /*
         * We have to set the failure marker before we count down otherwise we can expose the failure marker before we have set it to a
         * racing thread successfully freeing a context. This would lead to that thread responding that the clear scroll succeeded.
         */
        hasFailed.set(true);
        if (expectedOps.countDown()) {
            listener.onResponse(new ClearScrollResponse(false, freedSearchContexts.get()));
        }
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
        if (contextIds.isEmpty()) {
            listener.onResponse(0);
            return;
        }
        final Set<String> clusters = contextIds.stream()
            .filter(ctx -> Strings.isEmpty(ctx.getClusterAlias()) == false)
            .map(SearchContextIdForNode::getClusterAlias)
            .collect(Collectors.toSet());
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();
        if (clusters.isEmpty() == false) {
            searchTransportService.getRemoteClusterService().collectNodes(clusters, lookupListener);
        } else {
            lookupListener.onResponse((cluster, nodeId) -> nodes.get(nodeId));
        }
        lookupListener.whenComplete(nodeLookup -> {
            final GroupedActionListener<Boolean> groupedListener = new GroupedActionListener<>(
                listener.map(rs -> Math.toIntExact(rs.stream().filter(r -> r).count())),
                contextIds.size()
            );
            for (SearchContextIdForNode contextId : contextIds) {
                final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());
                if (node == null) {
                    groupedListener.onResponse(false);
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(contextId.getClusterAlias(), node);
                        searchTransportService.sendFreeContext(
                            connection,
                            contextId.getSearchContextId(),
                            ActionListener.wrap(r -> groupedListener.onResponse(r.isFreed()), e -> groupedListener.onResponse(false))
                        );
                    } catch (Exception e) {
                        groupedListener.onResponse(false);
                    }
                }
            }
        }, listener::onFailure);
    }
}
