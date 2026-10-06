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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.function.Supplier;

/**
 * Sends the fetch requests of one {@link org.elasticsearch.xpack.esql.plan.physical.FetchExec} from the coordinator of its
 * query.
 * <p>
 * Each request is a child of the query's task, so cancelling the query cancels it. It carries the index expressions of
 * the relation the documents come from, so it is authorized like the query's other requests to the data nodes. The
 * drivers that load the documents count as drivers of the query: their counters and profiles go to the query's compute
 * listener, once per request, whether it succeeds or not. Otherwise the query would wait for them forever.
 */
final class QueryFetchClient implements FetchOperator.Client {
    /**
     * Sends one fetch request. {@link FetchService#sendFetch} outside of tests.
     */
    @FunctionalInterface
    interface Sender {
        void send(DiscoveryNode node, FetchRequest request, Task parentTask, ActionListener<FetchResponse> listener);
    }

    private final Sender sender;
    private final Supplier<DiscoveryNodes> nodes;
    private final QueryFetchScope scope;
    private final OriginalIndices indices;

    /**
     * @param nodes            the nodes of the cluster, as of now
     * @param indexExpressions the index expressions of the relation the documents come from, on the local cluster
     */
    QueryFetchClient(Sender sender, Supplier<DiscoveryNodes> nodes, QueryFetchScope scope, List<String> indexExpressions) {
        if (indexExpressions.isEmpty()) {
            // a request without index expressions is authorized for every index
            throw new IllegalArgumentException("a fetch needs the index expressions of its relation");
        }
        this.sender = sender;
        this.nodes = nodes;
        this.scope = scope;
        this.indices = new OriginalIndices(indexExpressions.toArray(String[]::new), SearchRequest.DEFAULT_INDICES_OPTIONS);
    }

    @Override
    @Nullable
    public DiscoveryNode node(String clusterAlias, String nodeId) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            // the planner fetches from the local cluster only
            throw new IllegalStateException("can't fetch documents of cluster [" + clusterAlias + "]");
        }
        return nodes.get().get(nodeId);
    }

    @Override
    public void fetch(
        DiscoveryNode node,
        String clusterAlias,
        List<FetchRequest.ShardDocs> shards,
        PhysicalPlan fetchPlan,
        List<ShardSearchContextId> releaseAfter,
        ActionListener<FetchResponse> listener
    ) {
        FetchRequest request = new FetchRequest(
            scope.sessionId(),
            clusterAlias,
            indices,
            shards,
            scope.configuration(),
            fetchPlan,
            releaseAfter
        );
        ActionListener<DriverCompletionInfo> drivers = scope.completionInfo().get();
        ActionListener<FetchResponse> countingDrivers = ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(FetchResponse response) {
                try {
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
                    // the failure fails the fetch operator, and so the query
                    drivers.onResponse(DriverCompletionInfo.EMPTY);
                }
            }
        });
        try {
            sender.send(node, request, scope.rootTask(), countingDrivers);
        } catch (Exception e) {
            countingDrivers.onFailure(e);
        }
    }
}
