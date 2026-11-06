/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sends the fetch requests of one {@link org.elasticsearch.xpack.esql.plan.physical.FetchExec} from the coordinator of its
 * query, to the nodes of its own cluster and of remote clusters.
 * <p>
 * Each request is a child of the query's task, so cancelling the query cancels it. It carries the index expressions that
 * the relation the documents come from names on the cluster of the node, so it is authorized like the query's other
 * requests to the data nodes of that cluster. The drivers that load the documents count as drivers of the query: their
 * counters and profiles go to the query's compute listener, once per request, whether it succeeds or not. Otherwise the
 * query would wait for them forever.
 */
final class QueryFetchClient implements FetchOperator.Client {
    /**
     * Sends one fetch request. {@link FetchService#sendFetch} outside of tests.
     */
    @FunctionalInterface
    interface Sender {
        void send(FetchNode node, DiscoveryNode target, FetchRequest request, Task parentTask, ActionListener<FetchResponse> listener);
    }

    private final Sender sender;
    private final ThreadContext threadContext;
    private final FetchNodeDirectory nodes;
    private final QueryFetchScope scope;
    private final Map<String, OriginalIndices> indices;

    /**
     * @param indexExpressions the index expressions of the relation the documents come from, by the alias of each cluster
     *                         it reads
     */
    QueryFetchClient(
        Sender sender,
        ThreadContext threadContext,
        FetchNodeDirectory nodes,
        QueryFetchScope scope,
        Map<String, List<String>> indexExpressions
    ) {
        if (indexExpressions.isEmpty() || indexExpressions.values().stream().anyMatch(List::isEmpty)) {
            // a request without index expressions is authorized for every index
            throw new IllegalArgumentException(
                "a fetch needs the index expressions of its relation on every cluster, not " + indexExpressions
            );
        }
        this.sender = sender;
        this.threadContext = threadContext;
        this.nodes = nodes;
        this.scope = scope;
        Map<String, OriginalIndices> indices = new HashMap<>();
        indexExpressions.forEach(
            (clusterAlias, expressions) -> indices.put(
                clusterAlias,
                new OriginalIndices(expressions.toArray(String[]::new), SearchRequest.DEFAULT_INDICES_OPTIONS)
            )
        );
        this.indices = Map.copyOf(indices);
    }

    @Override
    public ThreadContext threadContext() {
        return threadContext;
    }

    @Override
    public void node(FetchNode node, ActionListener<DiscoveryNode> listener) {
        nodes.find(node, listener);
    }

    @Override
    public void fetch(
        DiscoveryNode node,
        String clusterAlias,
        List<FetchRequest.ShardDocs> shards,
        PhysicalPlan fetchPlan,
        ActionListener<FetchResponse> listener
    ) {
        OriginalIndices clusterIndices = indices.get(clusterAlias);
        if (clusterIndices == null) {
            // the documents come from the relation, so the relation reads their cluster
            throw new IllegalStateException("the fetch has no index expressions for the cluster [" + clusterAlias + "]");
        }
        FetchRequest request = new FetchRequest(scope.sessionId(), clusterAlias, clusterIndices, shards, scope.configuration(), fetchPlan);
        FetchNode fetchNode = new FetchNode(clusterAlias, node.getId());
        ActionListener<DriverCompletionInfo> drivers = scope.completionInfo().get();
        ActionListener<FetchResponse> countingDrivers = ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(FetchResponse response) {
                try {
                    scope.fetchContexts().freedByTheirNode(fetchNode, freedByTheNode(shards, response));
                    listener.onResponse(response);
                } finally {
                    drivers.onResponse(response.completionInfo());
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    listener.onFailure(e);
                } finally {
                    // the fetch operator decides whether the failure fails the query, so the drivers complete without counters
                    drivers.onResponse(DriverCompletionInfo.EMPTY);
                }
            }
        });
        try {
            sender.send(fetchNode, node, request, scope.rootTask(), countingDrivers);
        } catch (Exception e) {
            countingDrivers.onFailure(e);
        }
    }

    /**
     * The contexts that the node freed once it answered: those the request read for the last time, of the shards that
     * loaded. A shard can fail before the node opened its context, for example when its index authorization dropped, and
     * then the node frees nothing. The lease keeps those, and the ones of a request that failed as a whole, which may never
     * have reached the node, and frees them once the fetch ended.
     */
    private static List<ShardSearchContextId> freedByTheNode(List<FetchRequest.ShardDocs> shards, FetchResponse response) {
        List<FetchResponse.ShardResult> results = response.shardResults();
        List<ShardSearchContextId> freed = new ArrayList<>();
        for (int s = 0; s < shards.size() && s < results.size(); s++) {
            FetchRequest.ShardDocs shard = shards.get(s);
            FetchResponse.ShardResult result = results.get(s);
            if (shard.afterFetch() == FetchRequest.ContextAfterFetch.FREE
                && result.failure() == null
                && result.shardId().equals(shard.shardId())) {
                freed.add(shard.contextId());
            }
        }
        return freed;
    }

    @Override
    public void cutEnded(Set<ShardSearchContextId> read) {
        scope.fetchContexts().cutEnded(read);
    }

    @Override
    public void fetchEnded() {
        scope.fetchContexts().fetchEnded();
    }
}
