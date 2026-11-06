/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Finds the node that a {@link FetchNode} names, for a coordinator that fetches documents or frees their contexts.
 * <p>
 * A node of the local cluster comes from the cluster state. A node of a remote cluster comes from the nodes of that
 * cluster, which the directory collects once and keeps for every query of this node, like the nodes {@code _search}
 * collects before it reaches the data nodes of a remote cluster. A node that isn't among them may have joined since, so
 * the directory collects them once more before it gives up. A collection that fails is forgotten, and the next request
 * starts another one.
 * <p>
 * Thread-safe. The listeners run in the thread context of their caller, on the thread that finds the node: the calling
 * thread when the directory knows it, a transport thread otherwise.
 */
public final class FetchNodeDirectory {
    /**
     * Collects the nodes of a remote cluster.
     */
    @FunctionalInterface
    public interface RemoteNodes {
        /**
         * Collects the nodes of the remote cluster {@code clusterAlias}, and completes {@code listener} with a lookup of
         * them by id, which returns {@code null} for an id that none of them has.
         */
        void collect(String clusterAlias, ActionListener<Function<String, DiscoveryNode>> listener);
    }

    private final Supplier<DiscoveryNodes> localNodes;
    private final RemoteNodes remoteNodes;
    private final ThreadContext threadContext;
    private final Map<String, SubscribableListener<Function<String, DiscoveryNode>>> collected = new ConcurrentHashMap<>();

    /**
     * @param localNodes the nodes of the local cluster, as of now
     */
    public FetchNodeDirectory(Supplier<DiscoveryNodes> localNodes, RemoteNodes remoteNodes, ThreadContext threadContext) {
        this.localNodes = localNodes;
        this.remoteNodes = remoteNodes;
        this.threadContext = threadContext;
    }

    /**
     * A directory that collects the nodes of remote clusters through the connections of {@code transportService}.
     */
    public static FetchNodeDirectory create(
        TransportService transportService,
        ThreadContext threadContext,
        Supplier<DiscoveryNodes> localNodes
    ) {
        return new FetchNodeDirectory(localNodes, (clusterAlias, listener) -> {
            RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
            remoteClusterService.collectNodes(Set.of(clusterAlias), listener.map(nodes -> nodeId -> nodes.apply(clusterAlias, nodeId)));
        }, threadContext);
    }

    /**
     * Starts collecting the nodes of the remote cluster {@code clusterAlias}, unless the directory knows them already, so
     * the first request to one of them doesn't wait for them. Does nothing for the local cluster.
     */
    public void prepare(String clusterAlias) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            collection(clusterAlias);
        }
    }

    /**
     * Finds the node {@code node} names. Fails with {@link NoNodeAvailableException} when its cluster doesn't have it
     * anymore, and with the failure of the collection when the nodes of its remote cluster can't be collected.
     */
    public void find(FetchNode node, ActionListener<DiscoveryNode> listener) {
        if (node.isLocal()) {
            DiscoveryNode found = localNodes.get().get(node.nodeId());
            if (found == null) {
                listener.onFailure(new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster"));
            } else {
                listener.onResponse(found);
            }
            return;
        }
        ActionListener<DiscoveryNode> inCallerContext = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        SubscribableListener<Function<String, DiscoveryNode>> known = collection(node.clusterAlias());
        known.addListener(inCallerContext.delegateFailureAndWrap((l, nodes) -> {
            DiscoveryNode found = nodes.apply(node.nodeId());
            if (found != null) {
                l.onResponse(found);
                return;
            }
            collectAgain(node.clusterAlias(), known).addListener(l.delegateFailureAndWrap((again, fresh) -> {
                DiscoveryNode joined = fresh.apply(node.nodeId());
                if (joined == null) {
                    again.onFailure(
                        new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster [" + node.clusterAlias() + "]")
                    );
                } else {
                    again.onResponse(joined);
                }
            }));
        }));
    }

    /**
     * The connection to {@code target}, the node that {@code node} names. A node of a remote cluster that this node isn't
     * connected to is reached through a gateway node of its cluster, which forwards the request, so its action must be
     * registered with {@link org.elasticsearch.transport.TransportActionProxy}.
     *
     * @throws org.elasticsearch.transport.NodeNotConnectedException when this node can't reach a node of its own cluster
     */
    public static Transport.Connection connection(TransportService transportService, FetchNode node, DiscoveryNode target) {
        return node.isLocal()
            ? transportService.getConnection(target)
            : transportService.getRemoteClusterService().getConnection(target, node.clusterAlias());
    }

    private SubscribableListener<Function<String, DiscoveryNode>> collection(String clusterAlias) {
        SubscribableListener<Function<String, DiscoveryNode>> existing = collected.get(clusterAlias);
        if (existing != null) {
            return existing;
        }
        SubscribableListener<Function<String, DiscoveryNode>> started = new SubscribableListener<>();
        existing = collected.putIfAbsent(clusterAlias, started);
        if (existing != null) {
            return existing;
        }
        start(clusterAlias, started);
        return started;
    }

    /**
     * Collects the nodes of {@code clusterAlias} again, unless another request already replaced {@code stale}.
     */
    private SubscribableListener<Function<String, DiscoveryNode>> collectAgain(
        String clusterAlias,
        SubscribableListener<Function<String, DiscoveryNode>> stale
    ) {
        SubscribableListener<Function<String, DiscoveryNode>> fresh = new SubscribableListener<>();
        SubscribableListener<Function<String, DiscoveryNode>> current = collected.compute(
            clusterAlias,
            (alias, existing) -> existing == null || existing == stale ? fresh : existing
        );
        if (current == fresh) {
            start(clusterAlias, fresh);
        }
        return current;
    }

    private void start(String clusterAlias, SubscribableListener<Function<String, DiscoveryNode>> collection) {
        collection.addListener(ActionListener.wrap(nodes -> {}, e -> collected.remove(clusterAlias, collection)));
        // the nodes serve the queries of every user, so none of their thread contexts takes part in collecting them
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            remoteNodes.collect(clusterAlias, collection);
        } catch (Exception e) {
            collection.onFailure(e);
        }
    }
}
