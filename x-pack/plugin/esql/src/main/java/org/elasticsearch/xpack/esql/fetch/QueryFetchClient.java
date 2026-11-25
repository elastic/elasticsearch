/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Sends the fetch requests of one {@link org.elasticsearch.xpack.esql.plan.physical.FetchExec} from the coordinator of its
 * query to the nodes of its cluster, and tells the tracker of the execution which reader contexts can go.
 * <p>
 * Each request is a child of the query's task, so cancelling the query cancels it. It carries the index expressions of the
 * query with the indices options of its data node requests. Security resolves them to what the user may read now, and the
 * node refuses a shard outside that, so the fetch reads no more than the query could.
 * The drivers that load the documents count as drivers of the query: their counters and profiles go to the query's compute
 * listener, once per request, whether it succeeds or not. Otherwise the query would wait for them forever.
 */
public final class QueryFetchClient {
    /**
     * Sends one fetch request. {@link FetchService#sendFetch} outside of tests.
     */
    @FunctionalInterface
    interface Sender {
        void send(DiscoveryNode target, FetchRequest request, Task parentTask, ActionListener<FetchResponse> listener);
    }

    private final Sender sender;
    private final ThreadContext threadContext;
    private final Supplier<DiscoveryNodes> nodes;
    private final QueryFetchScope scope;
    private final OriginalIndices indices;

    /**
     * @param nodes   the nodes of the local cluster, as of now
     * @param indices the index expressions the user wrote for the local cluster, with the indices options of the query
     */
    QueryFetchClient(
        Sender sender,
        ThreadContext threadContext,
        Supplier<DiscoveryNodes> nodes,
        QueryFetchScope scope,
        OriginalIndices indices
    ) {
        if (indices.indices().length == 0) {
            // security would read no expression as every index the user may read
            throw new IllegalArgumentException("a fetch needs the index expressions of its relation");
        }
        this.sender = sender;
        this.threadContext = threadContext;
        this.nodes = nodes;
        this.scope = scope;
        this.indices = indices;
    }

    /**
     * The thread context the responses arrive in, so the fetch operator can merge the response headers of every node.
     */
    ThreadContext threadContext() {
        return threadContext;
    }

    /**
     * Finds the node in the cluster state. Fails with {@link NoNodeAvailableException} once the node left the cluster, and
     * refuses a node of a remote cluster, which the fetch phase doesn't reach.
     */
    DiscoveryNode node(FetchNode node) {
        if (node.isLocal() == false) {
            throw new IllegalArgumentException(
                "the fetch phase reads the local cluster only, not node [" + node.nodeId() + "] of [" + node.clusterAlias() + "]"
            );
        }
        DiscoveryNode found = nodes.get().get(node.nodeId());
        if (found == null) {
            throw new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster");
        }
        return found;
    }

    /**
     * Loads {@code attributes} for the documents of {@code shards} from {@code node}, a node of the local cluster. The
     * listener runs on another thread, and must take the pages of the response before it returns. The drivers of the
     * request completed by then, so they all count once the fetch operator heard from every request.
     */
    void fetch(
        DiscoveryNode node,
        List<FetchRequest.ShardDocs> shards,
        List<Attribute> attributes,
        ActionListener<FetchResponse> listener
    ) {
        FetchRequest request = new FetchRequest(
            scope.sessionId(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            indices,
            shards,
            scope.configuration(),
            attributes
        );
        ActionListener<DriverCompletionInfo> drivers = scope.completionInfo().get();
        ActionListener<FetchResponse> countingDrivers = ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(FetchResponse response) {
                try {
                    drivers.onResponse(response.completionInfo());
                } finally {
                    scope.fetchContexts().freedByTheirNode(freedByTheNode(shards));
                    listener.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    // the failure fails the query through the fetch operator, so the drivers complete without counters
                    drivers.onResponse(DriverCompletionInfo.EMPTY);
                } finally {
                    listener.onFailure(e);
                }
            }
        });
        try {
            sender.send(node, request, scope.rootTask(), countingDrivers);
        } catch (Exception e) {
            countingDrivers.onFailure(e);
        }
    }

    /**
     * The contexts that the node freed once it answered: those the request read for the last time. A request that
     * answered opened all of its shards. The tracker keeps the contexts of a request that failed, which may never have
     * reached the node, and frees them once the fetch ended.
     */
    private static List<ShardSearchContextId> freedByTheNode(List<FetchRequest.ShardDocs> shards) {
        List<ShardSearchContextId> freed = new ArrayList<>();
        for (FetchRequest.ShardDocs shard : shards) {
            if (shard.freeAfterFetch()) {
                freed.add(shard.contextId());
            }
        }
        return freed;
    }

    /**
     * The input of the fetch ended, and its rows read only the reader contexts in {@code read}. The others can go.
     */
    void inputEnded(Set<ShardSearchContextId> read) {
        scope.fetchContexts().cutEnded(read);
    }

    /**
     * Nothing fetches anymore, so the reader contexts of the rows can go too.
     */
    void fetchEnded() {
        scope.fetchContexts().close();
    }
}
