/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
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
        FetchContextLease lease = service.leaseFor(task);
        lease.add(localNode(), respond(INDEX));
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
        service.leaseFor(task).add(localNode(), respond(INDEX));
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
        lease.close();

        lease.add(localNode(), respond(INDEX));

        assertAllFreed();
    }

    /**
     * Every execution of a query shares its lease, because a later stage can fetch rows an earlier one made.
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
        service.leaseFor(task).add(localNode(), respond(INDEX));

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
            service.leaseFor(task).add(localNode(), respond(INDEX));
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
