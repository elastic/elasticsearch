/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.esql.fetch.FetchService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Queries with the fetch phase, cancelled while a shard still searches, between the query phase and the fetch, while a
 * node loads the documents of the winners, and at a random moment. Afterwards every reader context the query opened is
 * closed, and none closed on a transport thread, where dropping the last reference to a reader can block on IO.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 3)
public class EsqlFetchCancellationIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "fetch_cancellation";
    private static final int DOCS = 400;

    private final Set<ShardSearchContextId> opened = ConcurrentHashMap.newKeySet();
    private final Set<ShardSearchContextId> closed = ConcurrentHashMap.newKeySet();
    private final List<String> closedOnTransportThreads = new CopyOnWriteArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // holds a fetch request between the phases
        plugins.add(MockTransportService.TestPlugin.class);
        // holds a shard while it searches, or a node while it loads the winners
        plugins.add(SimplePauseFieldPlugin.class);
        return plugins;
    }

    @Before
    public void setupIndexAndHooks() {
        assumeTrue("the fetch phase needs its feature flag", EsqlFlags.FETCH_PHASE_FEATURE_FLAG.isEnabled());
        assumeTrue("the fetch_phase pragma needs a snapshot build", canUseQueryPragmas());
        SimplePauseFieldPlugin.resetPlugin();
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(between(1, 4), 0)).setMapping("""
            {
              "runtime": { "pause": { "type": "long", "script": { "source": "", "lang": "pause" } } },
              "properties": { "ts": { "type": "long" }, "payload": { "type": "keyword" } }
            }"""));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(prepareIndex(INDEX).setSource("ts", i, "payload", "payload-" + i));
        }
        assertFalse(bulk.get().hasFailures());
        ensureGreen(INDEX);
        // the eager path reads through unregistered contexts, so these are the contexts of the fetch phase
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            ((MockSearchService) searchService).setOnPutContext(context -> {
                opened.add(context.id());
                context.addOnClose(() -> {
                    closed.add(context.id());
                    if (Transports.isTransportThread(Thread.currentThread())) {
                        closedOnTransportThreads.add(context.id() + " on " + Thread.currentThread().getName());
                    }
                });
            });
        }
    }

    @After
    public void releaseAndClearHooks() {
        SimplePauseFieldPlugin.release();
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            ((MockSearchService) searchService).setOnPutContext(context -> {});
        }
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).clearAllRules();
        }
    }

    /**
     * Cancelled while a shard searches. The data nodes free the contexts they opened, also those of the shards that already
     * sent rows.
     */
    public void testCancelWhileAShardStillSearches() throws Exception {
        ActionFuture<EsqlQueryResponse> query = start("FROM " + INDEX + " | WHERE pause >= 0 | SORT ts DESC | LIMIT 10 | KEEP ts, payload");
        try {
            assertTrue("a shard searches", SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
            cancel();
        } finally {
            SimplePauseFieldPlugin.release();
        }
        assertCancelled(query);
        assertEveryContextClosedOffTransportThreads();
    }

    /**
     * Cancelled once the data nodes answered and the coordinator sent its fetch. The coordinator frees the contexts it holds,
     * and the fetch that arrives afterwards finds them gone or its task cancelled.
     */
    public void testCancelBetweenTheQueryPhaseAndTheFetch() throws Exception {
        CountDownLatch fetchArrived = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> {
                    fetchArrived.countDown();
                    safeAwait(cancelled);
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        ActionFuture<EsqlQueryResponse> query = start("FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP ts, payload");
        try {
            safeAwait(fetchArrived);
            cancel();
        } finally {
            cancelled.countDown();
        }
        assertCancelled(query);
        assertEveryContextClosedOffTransportThreads();
    }

    /**
     * Cancelled while a node loads the documents of the winners. The coordinator frees the contexts, and the fetch drops
     * its own references once its drivers stop.
     */
    public void testCancelWhileANodeLoadsTheWinners() throws Exception {
        CountDownLatch fetchArrived = new CountDownLatch(1);
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).<TransportRequest>addRequestHandlingBehavior(
                FetchService.FETCH_ACTION_NAME,
                (handler, request, channel, task) -> {
                    fetchArrived.countDown();
                    handler.messageReceived(request, channel, task);
                }
            );
        }
        // only the fetch loads pause, so its script holds the fetch and not the query phase
        ActionFuture<EsqlQueryResponse> query = start("FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP ts, payload, pause");
        try {
            assertTrue("a node loads the winners", SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS));
            assertThat("the script runs in the fetch", fetchArrived.getCount(), equalTo(0L));
            cancel();
        } finally {
            SimplePauseFieldPlugin.release();
        }
        assertCancelled(query);
        assertEveryContextClosedOffTransportThreads();
    }

    /**
     * Cancelled at a random moment of a query that runs freely, before the query phase starts, during it, during the
     * fetch or once the query ended.
     */
    public void testCancelAtARandomMoment() throws Exception {
        SimplePauseFieldPlugin.release();
        List<String> queries = List.of(
            "FROM " + INDEX + " | SORT ts DESC | LIMIT 10 | KEEP ts, payload",
            "FROM " + INDEX + " | LIMIT 100 | KEEP ts, payload",
            "FROM " + INDEX + " | WHERE pause >= 0 | SORT ts DESC | LIMIT 50 | KEEP ts, payload, pause",
            "FROM " + INDEX + " | KEEP ts, payload"
        );
        for (int round = 0; round < 10; round++) {
            ActionFuture<EsqlQueryResponse> query = start(randomFrom(queries));
            safeSleep(between(0, 20));
            cancel();
            try {
                query.actionGet(30, TimeUnit.SECONDS).close();
            } catch (Exception e) {
                assertTrue("round " + round + " failed with " + e, failedBecauseCancelled(e));
            }
            assertEveryContextClosedOffTransportThreads();
        }
        assertThat("some rounds opened fetch contexts", opened, not(empty()));
    }

    private ActionFuture<EsqlQueryResponse> start(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.FETCH_PHASE.getKey(), true)
                    .put(QueryPragmas.NODE_LEVEL_REDUCTION.getKey(), randomBoolean())
                    .build()
            )
        );
        return client().execute(EsqlQueryAction.INSTANCE, request);
    }

    /**
     * Cancels the query that runs, if it still runs.
     */
    private void cancel() {
        List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
        for (TaskInfo task : tasks) {
            CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(task.taskId()).setReason("test cancel");
            request.setWaitForCompletion(false);
            client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
        }
    }

    /**
     * The query failed because it was cancelled, after it opened fetch contexts.
     */
    private void assertCancelled(ActionFuture<EsqlQueryResponse> query) {
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS).close());
        assertTrue(e.toString(), failedBecauseCancelled(e));
        assertThat("the query opened fetch contexts", opened, not(empty()));
    }

    /**
     * Whether {@code e} is how a cancelled query fails. A query cancelled while the coordinator sends its node requests can
     * also fail with "no shard copies found", with the fetch phase on or off.
     */
    private static boolean failedBecauseCancelled(Exception e) {
        if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
            return true;
        }
        Throwable noShardCopies = ExceptionsHelper.unwrap(e, NoShardAvailableActionException.class);
        return noShardCopies != null && noShardCopies.getMessage().contains("no shard copies found");
    }

    /**
     * Every reader context the queries opened is freed and closed, and none closed on a transport thread.
     */
    private void assertEveryContextClosedOffTransportThreads() throws Exception {
        ensureAllSearchContextsReleased();
        assertBusy(() -> assertThat(closed, equalTo(opened)));
        assertThat("reader contexts closed on transport threads", closedOnTransportThreads, empty());
    }
}
