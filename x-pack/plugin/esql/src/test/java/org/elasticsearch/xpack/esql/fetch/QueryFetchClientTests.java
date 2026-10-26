/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * One fetch request as the coordinator sends it: what it carries, and that the drivers of the fetch reach the query
 * exactly once, whatever happens to the request. A missed completion would hang the query, a second one would count the
 * drivers twice.
 */
public class QueryFetchClientTests extends ComputeTestCase {
    private static final DiscoveryNode NODE = DiscoveryNodeUtils.create("n1");
    private static final FetchRequest.ShardDocs SHARD = new FetchRequest.ShardDocs(
        new ShardId("logs", "uuid", 0),
        new ShardSearchContextId("session", 7),
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
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final QueryDrivers drivers = new QueryDrivers();

    /**
     * The request is a child of the query's task, and is authorized with the index expressions of the relation.
     */
    public void testSendsTheRequestAsAChildOfTheQuery() {
        List<Sent> sent = new ArrayList<>();
        PhysicalPlan fetchPlan = FetchRequestTests.fetchPlan(List.of());
        QueryFetchClient client = client((node, request, parentTask, listener) -> sent.add(new Sent(node, request, parentTask)));
        client.fetch(NODE, "", List.of(SHARD), fetchPlan, ActionListener.noop());

        assertThat(client.threadContext(), sameInstance(threadContext));
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
                ActionListener.assertOnce(ActionListener.wrap(received::add, e -> {
                    throw new AssertionError(e);
                }))
            );
            assertThat(received, contains(sameInstance(response)));
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
        } finally {
            response.decRef();
        }
    }

    public void testAFailedRequestCompletesItsDrivers() {
        List<Exception> failures = new ArrayList<>();
        IllegalStateException rejected = new IllegalStateException("rejected");
        client((node, request, parentTask, listener) -> listener.onFailure(rejected)).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
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
            }).fetch(NODE, "", List.of(SHARD), null, new ActionListener<>() {
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
     * The ends of the cut and of the fetch reach the lease of the query, for the execution of the client: the end of the
     * cut frees the context no row read, and the end of the fetch the one they read.
     */
    public void testTheEndsOfTheCutAndTheFetchReachTheLeaseOfItsExecution() {
        // there is no node to free a context on, so the free action of _search only records what the lease frees
        SearchTransportService searchTransport = mock(SearchTransportService.class);
        when(searchTransport.transportService()).thenReturn(mock(TransportService.class));
        FetchContextLease lease = new FetchContextLease(
            new FetchContextService(null, searchTransport),
            rootTask.getId(),
            threadContext.newRestorableContext(false)
        );
        FetchContextLease.Execution fetchContexts = lease.newExecution();
        QueryFetchClient client = new QueryFetchClient(
            (node, request, parentTask, listener) -> fail("nothing to send"),
            threadContext,
            this::nodes,
            scope(fetchContexts),
            List.of("logs-*")
        );
        ShardSearchContextId read = new ShardSearchContextId("session", 1);
        ShardSearchContextId unread = new ShardSearchContextId("session", 2);
        fetchContexts.add(NODE, List.of(read, unread));

        client.cutEnded(Set.of(read));
        verify(searchTransport).sendFreeContext(any(), eq(unread), any());
        verify(searchTransport, never()).sendFreeContext(any(), eq(read), any());

        client.fetchEnded();
        verify(searchTransport).sendFreeContext(any(), eq(read), any());
        lease.close();
    }

    /**
     * A request without index expressions would be authorized for every index.
     */
    public void testNeedsTheIndexExpressionsOfTheRelation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryFetchClient((node, request, parentTask, listener) -> {}, threadContext, this::nodes, scope(), List.of())
        );
        assertThat(e.getMessage(), containsString("index expressions"));
    }

    private QueryFetchClient client(QueryFetchClient.Sender sender) {
        return new QueryFetchClient(sender, threadContext, this::nodes, scope(), List.of("logs-*", "-logs-old"));
    }

    private QueryFetchScope scope() {
        // the client holds no context in these tests, so the lease never sends a free request
        return scope(
            new FetchContextLease(new FetchContextService(null, null), rootTask.getId(), threadContext.newRestorableContext(false))
                .newExecution()
        );
    }

    private QueryFetchScope scope(FetchContextLease.Execution fetchContexts) {
        return new QueryFetchScope(rootTask, "query-session", EsqlTestUtils.TEST_CFG, drivers, new FetchFailures(false), fetchContexts);
    }

    private DiscoveryNodes nodes() {
        return DiscoveryNodes.builder().add(NODE).build();
    }

    private record Sent(DiscoveryNode node, FetchRequest request, Task parentTask) {}

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
