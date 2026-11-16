/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The coordinator's lease on the fetch contexts of one query, and the requests that free them. The contexts are real
 * contexts of the {@link SearchService} of the node. {@link MockSearchService} tracks every registered context, and the
 * check after each test fails on any context a test left open.
 */
public class FetchContextLeaseTests extends ESSingleNodeTestCase {
    private static final String INDEX = "fetch-contexts";
    private static final int SHARDS = 3;

    private FetchContextLeases leases;

    @Before
    public void createLeases() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        leases = new FetchContextLeases(
            searchService(),
            getInstanceFromNode(TransportService.class),
            getInstanceFromNode(SearchTransportService.class),
            new FetchNodeDirectory(() -> clusterService.state().nodes())
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // MockTransportService counts the free requests the lease sends
        return List.of(MockSearchService.TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    @After
    public void clearTransportRules() {
        ((MockTransportService) getInstanceFromNode(TransportService.class)).clearAllRules();
    }

    public void testCloseFreesEveryContext() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        arrive(leases.leaseFor(task).newExecution(), keepEveryShard());
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        leases.closeLease(task.getId());

        assertAllFreed();
    }

    /**
     * The lease frees each context with the free action of {@code _search}, like {@code _search} frees the contexts of its
     * query phase.
     */
    public void testFreesEachContextWithTheFreeActionOfSearch() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        arrive(leases.leaseFor(task).newExecution(), keepEveryShard());
        AtomicInteger freeRequests = countFreeRequests();

        leases.closeLease(task.getId());

        assertAllFreed();
        assertBusy(() -> assertThat(freeRequests.get(), equalTo(SHARDS)));
    }

    /**
     * An administrator can cancel the query of another user. The lease still frees its contexts as the user who ran the
     * query, who opened them.
     */
    public void testAFreeRunsAsTheUserWhoRanTheQuery() throws Exception {
        createIndexWithDocs();
        List<String> authentications = captureFreeAuthentications();
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication administrator = AuthenticationTestHelper.builder().user(new User("administrator")).build();
        CancellableTask task = rootTask();
        as(owner, () -> {
            arrive(leases.leaseFor(task).newExecution(), keepEveryShard());
            return null;
        });

        as(administrator, () -> {
            TaskCancelHelper.cancel(task, "an administrator cancelled the query");
            return null;
        });

        assertAllFreed();
        assertBusy(() -> assertThat(authentications, hasSize(SHARDS)));
        assertThat(authentications, everyItem(equalTo(owner.encode())));
    }

    /**
     * Rows that arrive after the query ended still name kept contexts. Nobody else would free them before the reaper does.
     */
    public void testContextsThatArriveAfterCloseAreFreedAtOnce() throws Exception {
        createIndexWithDocs();
        FetchContextLease lease = leases.leaseFor(rootTask());
        FetchContextLease.Execution execution = lease.newExecution();
        lease.close();

        arrive(execution, keepEveryShard());

        assertAllFreed();
    }

    /**
     * Once the cut of an execution ended, the lease frees the contexts its rows don't read, and keeps the others for the
     * fetch.
     */
    public void testTheEndOfTheCutFreesTheContextsItsRowsDontRead() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = leases.leaseFor(task).newExecution();
        List<ShardSearchContextId> kept = keepEveryShard();
        arrive(execution, kept);
        Set<ShardSearchContextId> read = Set.copyOf(randomSubsetOf(between(1, SHARDS - 1), kept));

        execution.cutEnded(read);

        for (ShardSearchContextId id : kept) {
            assertBusy(() -> assertThat(isOpen(id), equalTo(read.contains(id))));
        }
        leases.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * Rows that arrive after the cut ended have their contexts freed at once, unless the rows of the cut read them.
     */
    public void testAContextThatArrivesAfterTheCutIsFreedUnlessItsRowsReadIt() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = leases.leaseFor(task).newExecution();
        List<ShardSearchContextId> kept = keepEveryShard();
        Set<ShardSearchContextId> read = Set.copyOf(randomSubsetOf(between(1, SHARDS - 1), kept));
        execution.cutEnded(read);

        arrive(execution, kept);

        for (ShardSearchContextId id : kept) {
            assertBusy(() -> assertThat(isOpen(id), equalTo(read.contains(id))));
        }
        leases.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * Once the fetch of an execution ended, the lease frees every context the execution kept.
     */
    public void testTheEndOfTheFetchFreesTheContextsOfItsRows() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = leases.leaseFor(task).newExecution();
        List<ShardSearchContextId> kept = keepEveryShard();
        arrive(execution, kept);
        // a fetch that closes before its input ended never saw the end of its cut
        if (randomBoolean()) {
            execution.cutEnded(Set.copyOf(kept));
        }
        assertThat("the fetch still reads them", searchService().getActiveContexts(), equalTo(SHARDS));

        execution.fetchEnded();

        assertAllFreed();
        leases.closeLease(task.getId());
    }

    /**
     * Rows that arrive after the fetch ended have their contexts freed at once.
     */
    public void testAContextThatArrivesAfterTheFetchIsFreedAtOnce() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = leases.leaseFor(task).newExecution();
        execution.fetchEnded();

        arrive(execution, keepEveryShard());

        assertAllFreed();
        leases.closeLease(task.getId());
    }

    /**
     * The end of the cut or of the fetch of one execution frees none of the contexts of another execution.
     */
    public void testAnExecutionFreesOnlyItsOwnContexts() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        FetchContextLease lease = leases.leaseFor(task);
        FetchContextLease.Execution ending = lease.newExecution();
        FetchContextLease.Execution running = lease.newExecution();
        List<ShardSearchContextId> endingContexts = keepEveryShard();
        List<ShardSearchContextId> runningContexts = keepEveryShard();
        arrive(ending, endingContexts);
        arrive(running, runningContexts);

        if (randomBoolean()) {
            ending.cutEnded(Set.of());
        } else {
            ending.fetchEnded();
        }

        for (ShardSearchContextId id : endingContexts) {
            assertBusy(() -> assertFalse(isOpen(id)));
        }
        for (ShardSearchContextId id : runningContexts) {
            assertTrue("the other execution still reads [" + id + "]", isOpen(id));
        }
        leases.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * Every execution of a query shares its lease, so the task of the query frees what any execution left.
     */
    public void testOneLeasePerQuery() {
        CancellableTask task = rootTask();
        FetchContextLease lease = leases.leaseFor(task);
        assertThat(leases.leaseFor(task), sameInstance(lease));
        CancellableTask otherTask = rootTask();
        assertThat(leases.leaseFor(otherTask), not(sameInstance(lease)));

        leases.closeLease(task.getId());
        assertThat("a closed lease belongs to no query", leases.leaseFor(task), not(sameInstance(lease)));
        leases.closeLease(task.getId());
        leases.closeLease(otherTask.getId());
    }

    public void testCancellingTheQueryFreesItsContexts() throws Exception {
        createIndexWithDocs();
        CancellableTask task = rootTask();
        arrive(leases.leaseFor(task).newExecution(), keepEveryShard());

        TaskCancelHelper.cancel(task, "the user cancelled the query");

        assertAllFreed();
    }

    /**
     * Many rows arrive through the same context. The execution holds it once, and frees it once.
     */
    public void testRowsOfTheSameContextAreHeldOnce() {
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = leaseThrough(rootTask(), transport);
        FetchContextLease.Execution execution = lease.newExecution();
        int pages = between(2, 10);
        for (int p = 0; p < pages; p++) {
            execution.arrived(FetchNode.local("n1"), contextId(1));
        }

        lease.close();

        assertThat(transport.frees, contains(new RecordingFetchContextTransport.Sent(FetchNode.local("n1"), List.of(contextId(1)))));
    }

    /**
     * Rows of a context keep arriving after the execution stopped reading it. One free is enough.
     */
    public void testRowsThatArriveAfterTheEndFreeTheirContextOnce() {
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = leaseThrough(rootTask(), transport);
        FetchContextLease.Execution execution = lease.newExecution();
        execution.fetchEnded();
        int pages = between(2, 10);

        for (int p = 0; p < pages; p++) {
            execution.arrived(FetchNode.local("n1"), contextId(1));
        }

        assertThat(transport.frees, hasSize(1));
        lease.close();
    }

    /**
     * The lease keeps the contexts of each node apart by the cluster and the id of the node, so a node of another cluster
     * with the same id is another node. It frees them through its transport, with one call per node.
     */
    public void testFreesTheContextsOfEachClusterAndNodeThroughItsTransport() {
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = leaseThrough(rootTask(), transport);
        FetchContextLease.Execution execution = lease.newExecution();
        Map<FetchNode, List<ShardSearchContextId>> held = Map.of(
            FetchNode.local("n1"),
            List.of(contextId(1), contextId(2)),
            FetchNode.local("n2"),
            List.of(contextId(3)),
            new FetchNode("remote", "n1"),
            List.of(contextId(4), contextId(5))
        );
        held.forEach((node, ids) -> ids.forEach(id -> execution.arrived(node, id)));

        lease.close();

        assertThat(transport.frees, hasSize(held.size()));
        for (RecordingFetchContextTransport.Sent free : transport.frees) {
            assertThat(free.ids(), containsInAnyOrder(held.get(free.node()).toArray()));
        }
    }

    /**
     * A node frees a context once it answered the fetch that read it for the last time. The lease doesn't free it again.
     */
    public void testTheLeaseDoesntFreeTheContextsTheirNodeFreed() {
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = leaseThrough(rootTask(), transport);
        FetchContextLease.Execution execution = lease.newExecution();
        FetchNode node = FetchNode.local("n1");
        List<ShardSearchContextId> kept = List.of(contextId(1), contextId(2), contextId(3));
        kept.forEach(id -> execution.arrived(node, id));
        List<ShardSearchContextId> freedByTheNode = randomSubsetOf(between(1, kept.size()), kept);

        execution.freedByTheirNode(node, freedByTheNode);
        execution.fetchEnded();

        Set<ShardSearchContextId> stillHeld = new HashSet<>(kept);
        freedByTheNode.forEach(stillHeld::remove);
        assertThat(transport.freed(), equalTo(stillHeld));
        lease.close();
    }

    /**
     * A node that freed contexts itself names only its own. The contexts of the node with the same id in another cluster
     * stay with the lease, even under the same context id.
     */
    public void testANodeThatFreedItsContextsLetsGoOfItsOwnOnly() {
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = leaseThrough(rootTask(), transport);
        FetchContextLease.Execution execution = lease.newExecution();
        FetchNode local = FetchNode.local("n1");
        FetchNode remote = new FetchNode("remote", "n1");
        execution.arrived(local, contextId(1));
        execution.arrived(remote, contextId(1));

        execution.freedByTheirNode(local, List.of(contextId(1)));
        execution.fetchEnded();

        assertThat(transport.frees, contains(new RecordingFetchContextTransport.Sent(remote, List.of(contextId(1)))));
        lease.close();
    }

    /**
     * A node that left the cluster took its contexts with it, so freeing them sends nothing, and fails nothing. A free of
     * this node after it is the only request any node gets.
     */
    public void testAFreeForANodeThatLeftTheClusterSendsNothing() throws Exception {
        createIndexWithDocs();
        List<ShardSearchContextId> kept = keepEveryShard();
        AtomicInteger freeRequests = countFreeRequests();

        leases.free(FetchNode.local("a-node-that-left"), kept);
        leases.free(localFetchNode(), kept.subList(0, 1));

        assertBusy(() -> assertFalse(isOpen(kept.getFirst())));
        assertThat("only the free of this node", freeRequests.get(), equalTo(1));
        assertTrue("the free for the node that left reached no node", isOpen(kept.get(1)));
        kept.forEach(id -> searchService().freeReaderContext(id));
        assertAllFreed();
    }

    /**
     * Freeing a context twice, or one the reaper already freed, does nothing.
     */
    public void testFreeingAMissingContextDoesNothing() throws Exception {
        createIndexWithDocs();
        List<ShardSearchContextId> kept = keepEveryShard();
        leases.free(localFetchNode(), kept);
        assertAllFreed();

        AtomicInteger freeRequests = countFreeRequests();
        leases.free(localFetchNode(), kept);
        assertBusy(() -> assertThat(freeRequests.get(), equalTo(SHARDS)));
        assertAllFreed();
    }

    /**
     * The fetch phase reads the local cluster only, so a node of a remote cluster is out of reach, even with the id of this
     * node. Freeing its contexts sends nothing, and fails nothing.
     */
    public void testAFreeForANodeOfARemoteClusterSendsNothing() throws Exception {
        createIndexWithDocs();
        List<ShardSearchContextId> kept = keepEveryShard();
        AtomicInteger freeRequests = countFreeRequests();

        leases.free(new FetchNode("remote", localFetchNode().nodeId()), kept);
        leases.free(localFetchNode(), kept.subList(0, 1));

        assertBusy(() -> assertFalse(isOpen(kept.getFirst())));
        assertThat("only the free of this node", freeRequests.get(), equalTo(1));
        assertTrue("the free for the remote node reached no node", isOpen(kept.get(1)));
        kept.forEach(id -> searchService().freeReaderContext(id));
        assertAllFreed();
    }

    /**
     * The data nodes keep the contexts of a query for the default keep-alive of {@code _search} on the coordinator.
     */
    public void testALeaseKeepsTheDefaultKeepAliveOfSearch() {
        FetchContextLease lease = leases.leaseFor(rootTask());

        assertThat(lease.newExecution().keepAlive(), equalTo(TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis())));
        lease.close();
    }

    /**
     * Rows read through each of {@code ids} on this node reached the coordinator.
     */
    private void arrive(FetchContextLease.Execution execution, List<ShardSearchContextId> ids) {
        ids.forEach(id -> execution.arrived(localFetchNode(), id));
    }

    /**
     * Keeps a context of each shard, the way a data node request whose rows all left the node does.
     */
    private List<ShardSearchContextId> keepEveryShard() throws IOException {
        NodeFetchContexts contexts = new NodeFetchContexts(
            searchService(),
            null,
            TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis())
        );
        List<ShardSearchContextId> kept = new ArrayList<>();
        try {
            for (int shard = 0; shard < SHARDS; shard++) {
                ShardSearchRequest shardRequest = new ShardSearchRequest(
                    new ShardId(resolveIndex(INDEX), shard),
                    System.currentTimeMillis(),
                    AliasFilter.EMPTY,
                    null,
                    SplitShardCountSummary.IRRELEVANT
                );
                // a data node closes the search context of the query phase before the request ends
                try (SearchContext searchContext = contexts.open(shardRequest)) {
                    kept.add(contexts.originOf(searchContext).contextId());
                }
            }
        } finally {
            contexts.close();
        }
        return kept;
    }

    private void createIndexWithDocs() {
        createIndex(INDEX, Settings.builder().put("index.number_of_shards", SHARDS).put("index.number_of_replicas", 0).build());
        for (int i = 0; i < 20; i++) {
            prepareIndex(INDEX).setSource("field", i).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }

    private FetchNode localFetchNode() {
        return FetchNode.local(getInstanceFromNode(ClusterService.class).localNode().getId());
    }

    /**
     * Whether the context {@code id} is still registered.
     */
    private boolean isOpen(ShardSearchContextId id) {
        try {
            searchService().findReaderContext(id, new LookupRequest(), null);
            return true;
        } catch (SearchContextMissingException e) {
            return false;
        }
    }

    /**
     * Waits until no context is registered. A data node drops its references on the generic pool.
     */
    private void assertAllFreed() throws Exception {
        assertBusy(() -> assertThat(searchService().getActiveContexts(), equalTo(0)));
    }

    /**
     * Runs {@code body} in a thread context authenticated as {@code authentication}.
     */
    private <T> T as(Authentication authentication, CheckedSupplier<T, Exception> body) throws Exception {
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            authentication.writeToContext(threadContext);
            return body.get();
        }
    }

    private SearchService searchService() {
        return getInstanceFromNode(SearchService.class);
    }

    /**
     * Any request that looks a context up, like a fetch request.
     */
    private static class LookupRequest extends AbstractTransportRequest {}

    private static ShardSearchContextId contextId(long id) {
        return new ShardSearchContextId("session", id);
    }

    private static CancellableTask rootTask() {
        return new CancellableTask(randomNonNegativeLong(), "transport", "indices:data/read/esql", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    /**
     * A lease that isn't registered for its task, and sends its requests to {@code transport}.
     */
    private FetchContextLease leaseThrough(CancellableTask task, FetchContextTransport transport) {
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        return new FetchContextLease(leases, transport, task, threadContext.newRestorableContext(false), TimeValue.timeValueMinutes(5));
    }

    /**
     * The users that the free requests this node receives from now on come as. It serves them as usual.
     *
     * @return the encoded authentication header of each request, {@code null} for one without it
     */
    private List<String> captureFreeAuthentications() {
        List<String> received = new CopyOnWriteArrayList<>();
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        ((MockTransportService) getInstanceFromNode(TransportService.class)).<TransportRequest>addRequestHandlingBehavior(
            SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
            (handler, request, channel, task) -> {
                received.add(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                handler.messageReceived(request, channel, task);
            }
        );
        return received;
    }

    /**
     * Counts the free requests that this node receives from now on.
     */
    private AtomicInteger countFreeRequests() {
        AtomicInteger freeRequests = new AtomicInteger();
        ((MockTransportService) getInstanceFromNode(TransportService.class)).<TransportRequest>addRequestHandlingBehavior(
            SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
            (handler, request, channel, task) -> {
                freeRequests.incrementAndGet();
                handler.messageReceived(request, channel, task);
            }
        );
        return freeRequests;
    }
}
