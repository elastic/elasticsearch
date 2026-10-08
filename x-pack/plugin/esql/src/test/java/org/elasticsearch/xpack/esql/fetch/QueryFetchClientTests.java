/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * One fetch request as the coordinator sends it: what it carries, and that the drivers of the fetch reach the query
 * exactly once, whatever happens to the request. A missed completion would hang the query, a second one would count the
 * drivers twice.
 */
public class QueryFetchClientTests extends ComputeTestCase {
    private static final DiscoveryNode NODE = DiscoveryNodeUtils.create("n1");
    private static final ShardSearchContextId CONTEXT = new ShardSearchContextId("session", 7);
    private static final FetchRequest.ShardDocs SHARD = new FetchRequest.ShardDocs(
        new ShardId("logs", "uuid", 0),
        CONTEXT,
        new int[] { 0, 0, 2 },
        new int[] { 3, 8, 1 }
    );
    private static final DriverCompletionInfo FETCH_DRIVERS = new DriverCompletionInfo(
        3,
        6,
        3,
        0,
        0,
        0,
        0,
        List.of(),
        List.of(),
        Map.of(),
        false,
        false,
        Set.of()
    );

    private final CancellableTask rootTask = new CancellableTask(
        1,
        "transport",
        "indices:data/read/esql",
        "",
        TaskId.EMPTY_TASK_ID,
        Map.of()
    );
    private final QueryDrivers drivers = new QueryDrivers();
    private final List<Freed> freed = new ArrayList<>();

    /**
     * The request is a child of the query's task, and is authorized with the index expressions of the relation.
     */
    public void testSendsTheRequestAsAChildOfTheQuery() {
        List<Sent> sent = new ArrayList<>();
        PhysicalPlan fetchPlan = FetchRequestTests.fetchPlan(List.of());
        client((node, request, parentTask, listener) -> sent.add(new Sent(node, request, parentTask))).fetch(
            NODE,
            "",
            List.of(SHARD),
            fetchPlan,
            List.of(CONTEXT),
            ActionListener.noop()
        );

        assertThat(sent.size(), equalTo(1));
        assertThat(sent.getFirst().node(), sameInstance(NODE));
        assertThat(sent.getFirst().parentTask(), sameInstance(rootTask));
        FetchRequest request = sent.getFirst().request();
        assertThat(request.sessionId(), equalTo("query-session"));
        assertThat(request.clusterAlias(), equalTo(""));
        assertThat(request.indices(), equalTo(new String[] { "logs-*", "-logs-old" }));
        assertThat(request.indicesOptions(), equalTo(SearchRequest.DEFAULT_INDICES_OPTIONS));
        assertThat(request.shards(), equalTo(List.of(SHARD)));
        assertThat(request.fetchPlan(), sameInstance(fetchPlan));
        assertThat(request.releaseAfter(), equalTo(List.of(CONTEXT)));
    }

    public void testAResponseCountsItsDrivers() {
        FetchResponse response = new FetchResponse(blockFactory(), List.of(), List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<FetchResponse> received = new ArrayList<>();
            client((node, request, parentTask, listener) -> listener.onResponse(response)).fetch(
                NODE,
                "",
                List.of(SHARD),
                null,
                List.of(),
                ActionListener.assertOnce(ActionListener.wrap(received::add, e -> {
                    throw new AssertionError(e);
                }))
            );
            assertThat(received, contains(sameInstance(response)));
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
            assertThat("a fetch that frees no context", freed, empty());
        } finally {
            response.decRef();
        }
    }

    /**
     * A node that answered frees the contexts the request told it to, so the lease of the query forgets them, before the
     * drivers count. The query can't end before that, so its lease never frees them a second time.
     */
    public void testAnAnsweredFetchForgetsTheContextsItsNodeFrees() {
        FetchResponse response = new FetchResponse(blockFactory(), List.of(), List.of(), FETCH_DRIVERS, 0, 0);
        try {
            client((node, request, parentTask, listener) -> listener.onResponse(response)).fetch(
                NODE,
                "",
                List.of(SHARD),
                null,
                List.of(CONTEXT),
                ActionListener.noop()
            );
            assertThat(freed, contains(new Freed("n1", List.of(CONTEXT), true)));
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
        } finally {
            response.decRef();
        }
    }

    /**
     * A request can fail before its node read it, so the contexts stay with the lease, which frees them when the query ends.
     */
    public void testAFailedFetchLeavesItsContextsToTheLease() {
        client((node, request, parentTask, listener) -> listener.onFailure(new IllegalStateException("rejected"))).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
            List.of(CONTEXT),
            ActionListener.noop()
        );
        assertThat(freed, empty());
        assertThat(drivers.completed, contains(DriverCompletionInfo.EMPTY));
    }

    public void testAFailedRequestCompletesItsDrivers() {
        List<Exception> failures = new ArrayList<>();
        IllegalStateException rejected = new IllegalStateException("rejected");
        client((node, request, parentTask, listener) -> listener.onFailure(rejected)).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
            List.of(),
            ActionListener.assertOnce(ActionListener.wrap(r -> fail("unexpected response"), failures::add))
        );
        assertThat(failures, contains(sameInstance(rejected)));
        assertThat(drivers.completed, contains(DriverCompletionInfo.EMPTY));
    }

    /**
     * A request that fails before it leaves the node still completes its listener and its drivers.
     */
    public void testARequestThatCannotBeSentCompletesItsDrivers() {
        List<Exception> failures = new ArrayList<>();
        QueryFetchClient.Sender cannotSend = (node, request, parentTask, listener) -> { throw new IllegalStateException("not sent"); };
        client(cannotSend).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
            List.of(),
            ActionListener.assertOnce(ActionListener.wrap(r -> fail("unexpected response"), failures::add))
        );
        assertThat(failures.size(), equalTo(1));
        assertThat(failures.getFirst().getMessage(), equalTo("not sent"));
        assertThat(drivers.completed, contains(DriverCompletionInfo.EMPTY));
    }

    /**
     * When a listener throws, the transport completes it a second time, with the failure. The drivers count once.
     */
    public void testASecondCompletionIsIgnored() {
        FetchResponse response = new FetchResponse(blockFactory(), List.of(), List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<Exception> failures = new ArrayList<>();
            client((node, request, parentTask, listener) -> {
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }).fetch(NODE, "", List.of(SHARD), null, List.of(), new ActionListener<>() {
                @Override
                public void onResponse(FetchResponse fetchResponse) {
                    throw new IllegalStateException("can't read the response");
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(e);
                }
            });
            assertThat(failures, empty());
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
        } finally {
            response.decRef();
        }
    }

    public void testFindsTheNodesOfTheLocalCluster() {
        QueryFetchClient client = client((node, request, parentTask, listener) -> fail("nothing to send"));
        assertThat(client.node("", "n1"), sameInstance(NODE));
        assertThat("a node that left", client.node("", "n2"), nullValue());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> client.node("remote", "n1"));
        assertThat(e.getMessage(), equalTo("can't fetch documents of cluster [remote]"));
    }

    /**
     * A request without index expressions would be authorized for every index.
     */
    public void testNeedsTheIndexExpressionsOfTheRelation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryFetchClient((node, request, parentTask, listener) -> {}, this::forget, this::nodes, scope(), List.of())
        );
        assertThat(e.getMessage(), containsString("index expressions"));
    }

    private QueryFetchClient client(QueryFetchClient.Sender sender) {
        return new QueryFetchClient(sender, this::forget, this::nodes, scope(), List.of("logs-*", "-logs-old"));
    }

    private void forget(String nodeId, List<ShardSearchContextId> contextIds) {
        freed.add(new Freed(nodeId, contextIds, drivers.completed.isEmpty()));
    }

    private QueryFetchScope scope() {
        return new QueryFetchScope(rootTask, "query-session", EsqlTestUtils.TEST_CFG, 1, drivers);
    }

    private DiscoveryNodes nodes() {
        return DiscoveryNodes.builder().add(NODE).build();
    }

    private record Sent(DiscoveryNode node, FetchRequest request, Task parentTask) {}

    /**
     * Contexts the lease forgot, and whether that happened before the drivers of the request counted.
     */
    private record Freed(String nodeId, List<ShardSearchContextId> contextIds, boolean beforeTheDrivers) {}

    /**
     * Stands in for the compute listener of the query, which counts the drivers of each listener it hands out.
     */
    private static class QueryDrivers implements Supplier<ActionListener<DriverCompletionInfo>> {
        private final List<DriverCompletionInfo> completed = new ArrayList<>();

        @Override
        public ActionListener<DriverCompletionInfo> get() {
            return ActionListener.assertOnce(ActionListener.wrap(completed::add, e -> { throw new AssertionError(e); }));
        }
    }
}
