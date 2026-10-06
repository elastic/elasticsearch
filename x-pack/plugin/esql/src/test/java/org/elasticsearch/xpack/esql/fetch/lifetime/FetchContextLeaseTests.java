/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The coordinator's lease on the fetch contexts of one query, and the requests that free them.
 */
public class FetchContextLeaseTests extends FetchContextsTestCase {
    public void testCloseFreesEveryContext() throws Exception {
        createIndexWithDocs(INDEX);
        CancellableTask task = rootTask();
        FetchContextLease lease = service.leaseFor(task);
        lease.add(localNode(), originalIndices(INDEX), respond(INDEX));
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        service.closeLease(task.getId());

        assertAllFreed();
    }

    /**
     * A response that arrives after the query ended still lists open contexts. Nobody else would free them before the
     * reaper does.
     */
    public void testContextsListedAfterCloseAreFreedAtOnce() throws Exception {
        createIndexWithDocs(INDEX);
        FetchContextLease lease = service.leaseFor(rootTask());
        lease.close();

        lease.add(localNode(), originalIndices(INDEX), respond(INDEX));

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
        service.leaseFor(task).add(localNode(), originalIndices(INDEX), respond(INDEX));

        TaskCancelHelper.cancel(task, "the user cancelled the query");

        assertAllFreed();
    }

    /**
     * An administrator can cancel the query of another user. The lease still frees the contexts as their owner, because
     * the data nodes only let the owner free them.
     */
    public void testLeaseFreesAsTheOwnerOfTheQuery() throws Exception {
        createIndexWithDocs(INDEX);
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication administrator = AuthenticationTestHelper.builder().user(new User("administrator")).build();
        CancellableTask task = rootTask();
        as(owner, () -> {
            service.leaseFor(task).add(localNode(), originalIndices(INDEX), respond(INDEX));
            return null;
        });

        as(administrator, () -> {
            TaskCancelHelper.cancel(task, "an administrator cancelled the query");
            return null;
        });

        assertAllFreed();
    }

    public void testOnlyTheOwnerFreesAContext() throws Exception {
        createIndexWithDocs(INDEX);
        Authentication owner = AuthenticationTestHelper.builder().user(new User("owner")).build();
        Authentication other = AuthenticationTestHelper.builder().user(new User("other")).build();
        List<OpenContextInfo> open = as(owner, () -> respond(INDEX));

        as(other, () -> sendFree(open));
        assertThat(searchService().getActiveContexts(), equalTo(SHARDS));

        as(owner, () -> sendFree(open));
        assertAllFreed();
    }

    /**
     * Freeing a context twice, or one the reaper already freed, does nothing.
     */
    public void testFreeingAMissingContextDoesNothing() throws Exception {
        createIndexWithDocs(INDEX);
        List<OpenContextInfo> open = respond(INDEX);
        sendFree(open);
        assertAllFreed();

        sendFree(open);
        assertAllFreed();
    }

    /**
     * A free request only frees fetch contexts, so it can't free a scroll or a point in time whose id it learned.
     */
    public void testFreesOnlyFetchContexts() throws Exception {
        createIndexWithDocs(INDEX);
        ShardSearchRequest shardRequest = shardRequest(new ShardId(resolveIndex(INDEX), 0));
        ReaderContext other = searchService().openOwnedReaderContext(shardRequest, TimeValue.timeValueMinutes(5), null);
        try {
            sendFreeIds(List.of(other.id()));
            assertThat(searchService().getActiveContexts(), equalTo(1));
        } finally {
            searchService().freeReaderContext(other.id());
        }
        assertAllFreed();
    }

    /**
     * An id that this node can't look up doesn't keep the request from freeing the other contexts.
     */
    public void testAnUnreadableIdDoesNotKeepTheOthersOpen() throws Exception {
        createIndexWithDocs(INDEX);
        List<ShardSearchContextId> ids = new ArrayList<>(respond(INDEX).stream().map(OpenContextInfo::contextId).toList());
        // a lookup without a session id fails with an IllegalArgumentException
        ids.add(between(0, ids.size()), new ShardSearchContextId("", randomNonNegativeLong()));

        sendFreeIds(ids);

        assertAllFreed();
    }

    private static CancellableTask rootTask() {
        return new CancellableTask(randomNonNegativeLong(), "transport", "indices:data/read/esql", "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private Void sendFree(List<OpenContextInfo> open) {
        return sendFreeIds(open.stream().map(OpenContextInfo::contextId).toList());
    }

    /**
     * Sends a free request to this node in the current thread context, and waits for its response.
     */
    private Void sendFreeIds(List<ShardSearchContextId> ids) {
        PlainActionFuture<ActionResponse.Empty> response = new PlainActionFuture<>();
        getInstanceFromNode(TransportService.class).sendRequest(
            localNode(),
            FetchContextService.FREE_ACTION_NAME,
            new FetchFreeRequest(originalIndices(INDEX), ids),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(response, in -> ActionResponse.Empty.INSTANCE, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
        response.actionGet();
        return null;
    }
}
