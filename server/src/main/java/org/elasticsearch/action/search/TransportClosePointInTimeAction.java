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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TransportClosePointInTimeAction extends HandledTransportAction<ClosePointInTimeRequest, ClosePointInTimeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClosePointInTimeAction.class);

    public static final ActionType<ClosePointInTimeResponse> TYPE = new ActionType<>("indices:data/read/close_point_in_time");
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportClosePointInTimeAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(TYPE.name(), transportService, actionFilters, ClosePointInTimeRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, ClosePointInTimeRequest request, ActionListener<ClosePointInTimeResponse> listener) {
        final SearchContextId searchContextId = SearchContextId.decode(namedWriteableRegistry, request.getId());
        final Collection<SearchContextIdForNode> contextIds = searchContextId.shards().values();
        closeContexts(
            clusterService.state().nodes(),
            searchTransportService,
            contextIds,
            listener.map(freed -> new ClosePointInTimeResponse(freed == contextIds.size(), freed))
        );
    }

    /**
     * Marks each of the given PIT contexts as relocating on the node that holds it. This sends a
     * {@link SearchTransportService#MARK_CONTEXT_RELOCATING_ACTION_NAME} request to the owning
     * node so that the background Reaper can drain the context gracefully once in-flight
     * {@code markAsUsed} references are released, rather than closing it immediately and risking
     * a race in the phase-transition gap when no reference is held.
     * <p>
     * The operation is best-effort: failures are logged at TRACE level and otherwise ignored
     * because the contexts will expire naturally via their keep-alive TTL if the mark request
     * cannot be delivered.
     */
    public static void markContextsAsRelocating(
        DiscoveryNodes nodes,
        SearchTransportService searchTransportService,
        Collection<SearchContextIdForNode> contextIds
    ) {
        resolveContextNodes(nodes, searchTransportService, contextIds, ActionListener.wrap(nodeLookup -> {
            for (SearchContextIdForNode contextId : contextIds) {
                if (contextId.getNode() == null) {
                    // the shard was missing when creating the PIT, ignore.
                    continue;
                }
                final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());
                if (node != null) {
                    try {
                        searchTransportService.sendMarkContextAsRelocating(
                            searchTransportService.getConnection(contextId.getClusterAlias(), node),
                            contextId.getSearchContextId(),
                            ActionListener.wrap(r -> {}, e -> logger.trace("Failure while marking PIT context as relocating", e))
                        );
                    } catch (Exception e) {
                        logger.trace("Failure while marking PIT context as relocating", e);
                    }
                }
            }
        }, e -> logger.trace("Failure while marking PIT contexts as relocating", e)));
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
        resolveContextNodes(nodes, searchTransportService, contextIds, listener.delegateFailure((l, nodeLookup) -> {
            final var successes = new AtomicInteger();
            try (RefCountingRunnable refs = new RefCountingRunnable(() -> l.onResponse(successes.get()))) {
                for (SearchContextIdForNode contextId : contextIds) {
                    if (contextId.getNode() == null) {
                        // the shard was missing when creating the PIT, ignore.
                        continue;
                    }
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

    /**
     * Resolves the node-lookup function for a set of PIT context IDs, merging the local cluster
     * node map with remote cluster lookups where needed, and passes the result to
     * {@code listener}. Failures (e.g. a remote cluster being unreachable) are forwarded to
     * {@code listener.onFailure}.
     */
    private static void resolveContextNodes(
        DiscoveryNodes nodes,
        SearchTransportService searchTransportService,
        Collection<SearchContextIdForNode> contextIds,
        ActionListener<BiFunction<String, String, DiscoveryNode>> listener
    ) {
        final Set<String> clusters = contextIds.stream()
            .map(SearchContextIdForNode::getClusterAlias)
            .filter(clusterAlias -> Strings.isEmpty(clusterAlias) == false)
            .collect(Collectors.toSet());
        if (clusters.isEmpty()) {
            listener.onResponse((cluster, nodeId) -> nodes.get(nodeId));
        } else {
            searchTransportService.getRemoteClusterService()
                .collectNodes(
                    clusters,
                    ActionListener.wrap(
                        fn -> listener.onResponse(
                            (clusterAlias, nodeId) -> Strings.isEmpty(clusterAlias) ? nodes.get(nodeId) : fn.apply(clusterAlias, nodeId)
                        ),
                        listener::onFailure
                    )
                );
        }
    }
}
