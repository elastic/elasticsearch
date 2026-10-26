/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The coordinator's lease on the fetch contexts of one query, and the requests that free them.
 */
public class FetchContextLeaseTests extends FetchContextsTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // counts the free requests the lease sends
        return CollectionUtils.appendToCopy(super.getPlugins(), MockTransportService.TestPlugin.class);
    }

    @After
    public void clearTransportRules() {
        ((MockTransportService) getInstanceFromNode(TransportService.class)).clearAllRules();
    }

    public void testCloseFreesEveryContext() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        service.leaseFor(task).newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        service.closeLease(task.getId());

        assertAllFreed();
    }

    /**
     * Each context goes in its own request, to the free action of {@code _search}.
     */
    public void testFreesEachContextWithTheFreeActionOfSearch() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        service.leaseFor(task).newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));
        AtomicInteger freeRequests = countFreeRequests();

        service.closeLease(task.getId());

        assertAllFreed();
        assertThat(freeRequests.get(), equalTo(SHARDS));
    }

    /**
     * A response that arrives after the query ended still lists open contexts. Nobody else would free them before the
     * reaper does.
     */
    public void testContextsListedAfterCloseAreFreedAtOnce() throws Exception {
        createIndexWithDocs(INDEX);
        FetchContextLease lease = service.leaseFor(rootTask());
        FetchContextLease.Execution execution = lease.newExecution(QUERY_INDICES);
        lease.close();

        execution.add(localNode(), respond(INDEX));

        assertAllFreed();
    }

    /**
     * Once the cut of an execution ended, the lease frees the contexts its rows don't read, and keeps the others for the
     * fetch.
     */
    public void testTheEndOfTheCutFreesTheContextsItsRowsDontRead() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = service.leaseFor(task).newExecution(QUERY_INDICES);
        List<ShardSearchContextId> open = respond(INDEX);
        execution.add(localNode(), open);
        Set<ShardSearchContextId> read = Set.copyOf(randomSubsetOf(between(1, SHARDS - 1), open));
        AtomicInteger freeRequests = countFreeRequests();

        execution.cutEnded(read);

        assertBusy(() -> assertThat(freeRequests.get(), equalTo(SHARDS - read.size())));
        for (ShardSearchContextId id : open) {
            assertBusy(() -> assertThat(isOpen(id), equalTo(read.contains(id))));
        }
        service.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * A response that arrives after the cut ended has its contexts freed at once, unless the rows of the cut read them.
     */
    public void testAContextListedAfterTheCutIsFreedUnlessItsRowsReadIt() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = service.leaseFor(task).newExecution(QUERY_INDICES);
        List<ShardSearchContextId> open = respond(INDEX);
        Set<ShardSearchContextId> read = Set.copyOf(randomSubsetOf(between(1, SHARDS - 1), open));
        execution.cutEnded(read);
        AtomicInteger freeRequests = countFreeRequests();

        execution.add(localNode(), open);

        assertBusy(() -> assertThat(freeRequests.get(), equalTo(SHARDS - read.size())));
        for (ShardSearchContextId id : open) {
            assertBusy(() -> assertThat(isOpen(id), equalTo(read.contains(id))));
        }
        service.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * Once the fetch of an execution ended, the lease frees every context the execution kept.
     */
    public void testTheEndOfTheFetchFreesTheContextsOfItsRows() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = service.leaseFor(task).newExecution(QUERY_INDICES);
        List<ShardSearchContextId> open = respond(INDEX);
        execution.add(localNode(), open);
        // a fetch that closes before its input ended never saw the end of its cut
        if (randomBoolean()) {
            execution.cutEnded(Set.copyOf(open));
        }
        assertThat("the fetch still reads them", searchService().getActiveContexts(), equalTo(SHARDS));

        execution.fetchEnded();

        assertAllFreed();
        service.closeLease(task.getId());
    }

    /**
     * A response that arrives after the fetch ended has its contexts freed at once.
     */
    public void testAContextListedAfterTheFetchIsFreedAtOnce() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease.Execution execution = service.leaseFor(task).newExecution(QUERY_INDICES);
        execution.fetchEnded();

        execution.add(localNode(), respond(INDEX));

        assertAllFreed();
        service.closeLease(task.getId());
    }

    /**
     * The end of the cut or of the fetch of one execution frees none of the contexts of another execution.
     */
    public void testAnExecutionFreesOnlyItsOwnContexts() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        FetchContextLease.Execution ending = lease.newExecution(QUERY_INDICES);
        FetchContextLease.Execution running = lease.newExecution(QUERY_INDICES);
        List<ShardSearchContextId> endingContexts = respond(INDEX);
        List<ShardSearchContextId> runningContexts = respond(INDEX);
        ending.add(localNode(), endingContexts);
        running.add(localNode(), runningContexts);

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
        service.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * A request without index expressions would be authorized for every index.
     */
    public void testAnExecutionNeedsTheIndexExpressionsOfItsQuery() {
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        OriginalIndices none = new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> lease.newExecution(none));

        assertThat(e.getMessage(), equalTo("an execution needs the index expressions of its query"));
        service.closeLease(task.getId());
    }

    /**
     * A refresh sends one keep-alive request per execution and node, which names every context the execution holds there.
     */
    public void testARefreshNamesEveryHeldContextOncePerNode() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        FetchContextLease.Execution first = lease.newExecution(QUERY_INDICES);
        FetchContextLease.Execution second = lease.newExecution(QUERY_INDICES);
        List<ShardSearchContextId> firstContexts = respond(INDEX);
        List<ShardSearchContextId> secondContexts = respond(INDEX);
        first.add(localNode(), firstContexts);
        second.add(localNode(), secondContexts);

        lease.refresh();

        assertBusy(() -> assertThat(received, hasSize(2)));
        service.closeLease(task.getId());
        assertAllFreed();
        assertThat(
            received.stream().map(r -> Set.copyOf(r.request().contextIds())).toList(),
            containsInAnyOrder(Set.copyOf(firstContexts), Set.copyOf(secondContexts))
        );
    }

    /**
     * Each keep-alive request carries the index expressions of its execution, so it is authorized like the fetch requests.
     */
    public void testARefreshCarriesTheIndexExpressionsOfItsExecution() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        OriginalIndices otherIndices = new OriginalIndices(new String[] { "fetch-*", "-fetch-old" }, IndicesOptions.lenientExpandOpen());
        List<ShardSearchContextId> queryContexts = respond(INDEX);
        List<ShardSearchContextId> otherContexts = respond(INDEX);
        lease.newExecution(QUERY_INDICES).add(localNode(), queryContexts);
        lease.newExecution(otherIndices).add(localNode(), otherContexts);

        lease.refresh();

        assertBusy(() -> assertThat(received, hasSize(2)));
        service.closeLease(task.getId());
        assertAllFreed();
        for (ReceivedKeepAlive keepAlive : received) {
            OriginalIndices expected = keepAlive.request().contextIds().containsAll(queryContexts) ? QUERY_INDICES : otherIndices;
            assertThat(keepAlive.request().indices(), equalTo(expected.indices()));
            assertThat(keepAlive.request().indicesOptions(), equalTo(expected.indicesOptions()));
        }
    }

    /**
     * The keep-alive of a query is the default keep-alive of {@code _search} on its coordinator.
     */
    public void testARefreshAsksForTheKeepAliveOfTheQuery() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        FetchContextLease.Execution execution = lease.newExecution(QUERY_INDICES);
        execution.add(localNode(), respond(INDEX));

        lease.refresh();

        assertBusy(() -> assertThat(received, hasSize(1)));
        TimeValue keepAlive = TimeValue.timeValueMillis(searchService().getDefaultKeepAliveInMillis());
        assertThat(execution.keepAlive(), equalTo(keepAlive));
        assertThat(received.getFirst().request().keepAlive(), equalTo(keepAlive));
        service.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * The contexts the lease already freed, at the end of a cut or of a fetch, aren't refreshed.
     */
    public void testARefreshSkipsTheContextsTheLeaseFreed() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        FetchContextLease.Execution cutEnded = lease.newExecution(QUERY_INDICES);
        FetchContextLease.Execution fetchEnded = lease.newExecution(QUERY_INDICES);
        List<ShardSearchContextId> open = respond(INDEX);
        cutEnded.add(localNode(), open);
        ShardSearchContextId read = randomFrom(open);
        cutEnded.cutEnded(Set.of(read));
        fetchEnded.add(localNode(), respond(INDEX));
        fetchEnded.fetchEnded();

        lease.refresh();

        assertBusy(() -> assertThat(received, hasSize(1)));
        service.closeLease(task.getId());
        assertAllFreed();
        assertThat("one request for one execution", received, hasSize(1));
        assertThat(received.getFirst().request().contextIds(), contains(read));
    }

    /**
     * Each keep-alive request is a child of the task of the query, sent as the user who ran the query, also when another
     * thread context triggers the refresh.
     */
    public void testARefreshIsAChildOfTheQueryAndRunsAsItsUser() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication administrator = AuthenticationTestHelper.builder().user(new User("administrator")).build();
        CancellableTask task = rootTask();
        FetchContextLease lease = as(owner, () -> service.leaseFor(task));
        lease.newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));

        as(administrator, () -> {
            lease.refresh();
            return null;
        });

        assertBusy(() -> assertThat(received, hasSize(1)));
        assertThat(received.getFirst().request().getParentTask(), equalTo(new TaskId(localNode().getId(), task.getId())));
        assertThat(received.getFirst().authentication(), equalTo(owner.encode()));
        service.closeLease(task.getId());
        assertAllFreed();
    }

    /**
     * Two refreshes in a row can fail or come late before a context expires. A fixed delay separates two refreshes, so
     * their requests arrive at least about a third of the keep-alive apart.
     */
    public void testTheLeaseRefreshesEveryThirdOfTheKeepAlive() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        TimeValue keepAlive = TimeValue.timeValueMillis(1500);
        long interval = keepAlive.nanos() / 3;
        FetchContextLease lease = leaseWithKeepAlive(task, keepAlive);
        long held = System.nanoTime();
        lease.newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));

        assertBusy(() -> assertThat(received.size(), greaterThanOrEqualTo(3)));
        lease.close();
        assertAllFreed();
        // the receipt of a request can come late, which shortens the gap after it, so the bounds leave room for that
        assertThat(received.getFirst().nanos() - held, greaterThanOrEqualTo(interval * 9 / 10));
        assertThat(received.get(2).nanos() - received.getFirst().nanos(), greaterThanOrEqualTo(2 * interval * 9 / 10));
    }

    /**
     * Once the lease holds no context it stops refreshing, and the next context it holds starts it again.
     */
    public void testTheLeaseRefreshesOnlyWhileItHoldsContexts() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        TimeValue keepAlive = TimeValue.timeValueMillis(300);
        FetchContextLease lease = leaseWithKeepAlive(task, keepAlive);
        FetchContextLease.Execution first = lease.newExecution(QUERY_INDICES);
        first.add(localNode(), respond(INDEX));
        assertBusy(() -> assertThat(received, not(empty())));

        first.fetchEnded();
        assertAllFreed();
        // a refresh that left before the fetch ended can still arrive
        safeSleep(keepAlive);
        int settled = received.size();
        safeSleep(keepAlive.millis() * 2);
        assertThat("nothing is refreshed while nothing is held", received, hasSize(settled));

        lease.newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));
        assertBusy(() -> assertThat(received.size(), greaterThan(settled)));
        lease.close();
        assertAllFreed();
    }

    /**
     * Once the query ended, nobody refreshes its contexts, so a context whose free got lost still expires.
     */
    public void testClosingStopsTheRefresh() throws Exception {
        createIndexWithDocs(INDEX);
        List<ReceivedKeepAlive> received = captureKeepAlives();
        CancellableTask task = rootTask();
        TimeValue keepAlive = TimeValue.timeValueMillis(300);
        FetchContextLease lease = leaseWithKeepAlive(task, keepAlive);
        List<ShardSearchContextId> open = respond(INDEX);
        lease.newExecution(QUERY_INDICES).add(localNode(), open);
        assertBusy(() -> assertThat(received, not(empty())));
        loseFreeRequests();

        lease.close();

        // a refresh that left before the lease closed can still arrive
        safeSleep(keepAlive);
        int settled = received.size();
        safeSleep(keepAlive.millis() * 2);
        assertThat("nothing is refreshed once the query ended", received, hasSize(settled));
        assertThat("the frees got lost", searchService().getActiveContexts(), equalTo(SHARDS));
        open.forEach(id -> searchService().freeReaderContext(id));
        assertAllFreed();
    }

    /**
     * Every execution of a query shares its lease, so the task of the query frees what any execution left.
     */
    public void testOneLeasePerQuery() {
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        assertThat(service.leaseFor(task), sameInstance(lease));
        CancellableTask otherTask = rootTask();
        assertThat(service.leaseFor(otherTask), not(sameInstance(lease)));

        service.closeLease(task.getId());
        assertThat("a closed lease belongs to no query", service.leaseFor(task), not(sameInstance(lease)));
        service.closeLease(task.getId());
        service.closeLease(otherTask.getId());
    }

    public void testCancellingTheQueryFreesItsContexts() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        service.leaseFor(task).newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));

        TaskCancelHelper.cancel(task, "the user cancelled the query");

        assertAllFreed();
    }

    /**
     * An administrator can cancel the query of another user. The lease still frees the contexts, as the user who ran the
     * query.
     */
    public void testCancelledByAnotherUserFreesTheContexts() throws Exception {
        createIndexWithDocs(INDEX);
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication administrator = AuthenticationTestHelper.builder().user(new User("administrator")).build();
        CancellableTask task = rootTask();
        as(owner, () -> {
            service.leaseFor(task).newExecution(QUERY_INDICES).add(localNode(), respond(INDEX));
            return null;
        });

        as(administrator, () -> {
            TaskCancelHelper.cancel(task, "an administrator cancelled the query");
            return null;
        });

        assertAllFreed();
    }

    /**
     * Freeing a context twice, or one the reaper already freed, does nothing.
     */
    public void testFreeingAMissingContextDoesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> open = respond(INDEX);
        open.forEach(id -> service.sendFree(localNode(), id));
        assertAllFreed();

        AtomicInteger freeRequests = countFreeRequests();
        open.forEach(id -> service.sendFree(localNode(), id));
        assertBusy(() -> assertThat(freeRequests.get(), equalTo(SHARDS)));
        assertAllFreed();
    }

    private static CancellableTask rootTask() {
        return new CancellableTask(randomNonNegativeLong(), "transport", "indices:data/read/esql", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    /**
     * A lease that isn't registered for its task, with a keep-alive short enough for a test to see it refresh.
     */
    private FetchContextLease leaseWithKeepAlive(CancellableTask task, TimeValue keepAlive) {
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        return new FetchContextLease(service, task, threadContext.newRestorableContext(false), keepAlive);
    }

    /**
     * A keep-alive request this node received, the user it came as, and when it arrived.
     *
     * @param authentication the encoded authentication header of the request, {@code null} without one
     */
    private record ReceivedKeepAlive(FetchContextKeepAliveRequest request, @Nullable String authentication, long nanos) {}

    /**
     * The keep-alive requests that this node receives from now on. It serves them as usual.
     */
    private List<ReceivedKeepAlive> captureKeepAlives() {
        registerKeepAliveHandler();
        List<ReceivedKeepAlive> received = new CopyOnWriteArrayList<>();
        ThreadContext threadContext = getInstanceFromNode(ThreadPool.class).getThreadContext();
        ((MockTransportService) getInstanceFromNode(TransportService.class)).<FetchContextKeepAliveRequest>addRequestHandlingBehavior(
            FetchContextService.KEEP_ALIVE_ACTION_NAME,
            (handler, request, channel, task) -> {
                received.add(
                    new ReceivedKeepAlive(request, threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), System.nanoTime())
                );
                handler.messageReceived(request, channel, task);
            }
        );
        return received;
    }

    /**
     * Answers every request of the free action of {@code _search} with a failure, and frees nothing.
     */
    private void loseFreeRequests() {
        ((MockTransportService) getInstanceFromNode(TransportService.class)).<TransportRequest>addRequestHandlingBehavior(
            SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new ElasticsearchException("the free got lost"))
        );
    }

    /**
     * Counts the requests of the free action of {@code _search} that this node receives from now on.
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
