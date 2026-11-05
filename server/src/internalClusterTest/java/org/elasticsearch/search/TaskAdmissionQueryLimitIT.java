/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAdmissionPolicy;
import org.elasticsearch.tasks.TaskAdmissionPolicyProvider;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end check that a {@link TaskAdmissionPolicyProvider} can install a coordinating-query
 * policy with its own action names and limit, covering search, scroll, open PIT, and a
 * plugin action (the same shape serverless will use for ES|QL).
 */
public class TaskAdmissionQueryLimitIT extends ESSingleNodeTestCase {

    static final int QUERY_LIMIT = 2;
    static final ActionType<ActionResponse.Empty> TEST_QUERY_ACTION = new ActionType<>("indices:data/read/test_query");

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    public void testAcceptedAndRejectedAcrossSearchScrollPitAndPluginAction() throws Exception {
        createIndex("idx");
        prepareIndex("idx").setId("1").setSource("f", "v").get();
        indicesAdmin().prepareRefresh("idx").get();

        assertHitCount(client().prepareSearch("idx"), 1L);
        client().execute(TEST_QUERY_ACTION, new TestQueryRequest()).actionGet();

        BytesReference pitId = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest("idx").keepAlive(TimeValue.timeValueMinutes(1))
        ).actionGet().getPointInTimeId();
        String scrollId;
        {
            SearchResponse scrollStart = client().prepareSearch("idx").setSize(1).setScroll(TimeValue.timeValueMinutes(1)).get();
            try {
                assertHitCount(scrollStart, 1L);
                scrollId = scrollStart.getScrollId();
            } finally {
                scrollStart.decRef();
            }
        }

        List<ActionFuture<SearchResponse>> heldSearches = new ArrayList<>();
        ActionFuture<ActionResponse.Empty> heldPluginAction = null;
        try {
            TestPlugin.startBlockingQueries(QUERY_LIMIT);
            for (int i = 0; i < QUERY_LIMIT; i++) {
                heldSearches.add(client().prepareSearch("idx").execute());
            }
            TestPlugin.awaitQueriesStarted();
            TestPlugin.stopBlockingNewQueries();

            assertAllQueryActionsRejected(scrollId);

            // Close PIT is not in the query group, so it still succeeds while searches occupy the limit.
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).get();

            TestPlugin.releaseOneQuery();
            assertBusy(() -> assertTrue(heldSearches.stream().anyMatch(ActionFuture::isDone)));
            ActionFuture<SearchResponse> completedSearch = heldSearches.stream().filter(ActionFuture::isDone).findFirst().orElseThrow();
            heldSearches.remove(completedSearch);
            assertHitCount(completedSearch, 1L);

            TestPlugin.startBlockingAction();
            heldPluginAction = client().execute(TEST_QUERY_ACTION, new TestQueryRequest());
            TestPlugin.awaitActionStarted();

            assertAllQueryActionsRejected(scrollId);
        } finally {
            TestPlugin.releaseAllQueries();
            TestPlugin.releaseAction();
        }

        for (ActionFuture<SearchResponse> heldSearch : heldSearches) {
            assertHitCount(heldSearch, 1L);
        }
        assertNotNull(heldPluginAction);
        heldPluginAction.actionGet();

        SearchResponse scrollContinue = client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).get();
        try {
            assertHitCount(scrollContinue, 1L);
            scrollId = scrollContinue.getScrollId();
        } finally {
            scrollContinue.decRef();
        }
        pitId = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest("idx").keepAlive(TimeValue.timeValueMinutes(1))
        ).actionGet().getPointInTimeId();
        client().execute(TEST_QUERY_ACTION, new TestQueryRequest()).actionGet();
        assertHitCount(client().prepareSearch("idx"), 1L);

        client().prepareClearScroll().addScrollId(scrollId).get();
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
    }

    private void assertAllQueryActionsRejected(String scrollId) {
        assertRejected(
            expectThrows(EsRejectedExecutionException.class, () -> client().prepareSearch("idx").get()),
            TransportSearchAction.TYPE.name()
        );
        assertRejected(
            expectThrows(
                EsRejectedExecutionException.class,
                () -> client().prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(1)).get()
            ),
            TransportSearchScrollAction.TYPE.name()
        );
        assertRejected(
            expectThrows(
                EsRejectedExecutionException.class,
                () -> client().execute(
                    TransportOpenPointInTimeAction.TYPE,
                    new OpenPointInTimeRequest("idx").keepAlive(TimeValue.timeValueMinutes(1))
                ).actionGet()
            ),
            TransportOpenPointInTimeAction.TYPE.name()
        );
        assertRejected(
            expectThrows(EsRejectedExecutionException.class, () -> client().execute(TEST_QUERY_ACTION, new TestQueryRequest()).actionGet()),
            TEST_QUERY_ACTION.name()
        );
    }

    private static void assertRejected(EsRejectedExecutionException e, String action) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(e.getMessage(), containsString("rejected execution of action [" + action + "]"));
        assertThat(e.getMessage(), containsString("too many in-flight requests [" + (QUERY_LIMIT + 1) + "], limit [" + QUERY_LIMIT + "]"));
    }

    /**
     * Present so {@link LimitedQueryAdmissionPolicyProvider} is only constructed for this test
     * node. Other internal-cluster tests share the same classloader and would otherwise pick up
     * a no-arg SPI provider.
     */
    public static class TestPlugin extends Plugin implements ActionPlugin {
        static final AtomicBoolean blockNewQueries = new AtomicBoolean();
        static final AtomicReference<CountDownLatch> queryStarted = new AtomicReference<>();
        static final ConcurrentLinkedQueue<CountDownLatch> queryReleases = new ConcurrentLinkedQueue<>();
        static final AtomicReference<CountDownLatch> actionStarted = new AtomicReference<>();
        static final AtomicReference<CountDownLatch> releaseAction = new AtomicReference<>();

        static void startBlockingQueries(int count) {
            queryReleases.clear();
            queryStarted.set(new CountDownLatch(count));
            blockNewQueries.set(true);
        }

        static void awaitQueriesStarted() throws InterruptedException {
            assertTrue(queryStarted.get().await(10, TimeUnit.SECONDS));
        }

        static void stopBlockingNewQueries() {
            blockNewQueries.set(false);
        }

        static void releaseOneQuery() {
            CountDownLatch latch = queryReleases.poll();
            assertThat(latch, notNullValue());
            latch.countDown();
        }

        static void releaseAllQueries() {
            blockNewQueries.set(false);
            queryStarted.set(null);
            CountDownLatch latch;
            while ((latch = queryReleases.poll()) != null) {
                latch.countDown();
            }
        }

        static void startBlockingAction() {
            actionStarted.set(new CountDownLatch(1));
            releaseAction.set(new CountDownLatch(1));
        }

        static void awaitActionStarted() throws InterruptedException {
            assertTrue(actionStarted.get().await(10, TimeUnit.SECONDS));
        }

        static void releaseAction() {
            CountDownLatch latch = releaseAction.getAndSet(null);
            actionStarted.set(null);
            if (latch != null) {
                latch.countDown();
            }
        }

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(TEST_QUERY_ACTION, TestQueryTransportAction.class));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    if (blockNewQueries.get() == false) {
                        return;
                    }
                    CountDownLatch myRelease = new CountDownLatch(1);
                    queryReleases.add(myRelease);
                    CountDownLatch started = queryStarted.get();
                    if (started != null) {
                        started.countDown();
                    }
                    try {
                        assertTrue(myRelease.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });
        }
    }

    /**
     * Loaded via {@code META-INF/services} only when {@link TestPlugin} is installed.
     */
    public static class LimitedQueryAdmissionPolicyProvider implements TaskAdmissionPolicyProvider {
        public LimitedQueryAdmissionPolicyProvider(TestPlugin plugin) {}

        @Override
        public TaskAdmissionPolicy create() {
            return new TaskAdmissionPolicy(
                List.of(
                    TransportSearchAction.TYPE.name(),
                    TransportSearchScrollAction.TYPE.name(),
                    TransportOpenPointInTimeAction.TYPE.name(),
                    TEST_QUERY_ACTION.name()
                ),
                QUERY_LIMIT
            );
        }
    }

    public static class TestQueryRequest extends UntypedActionRequest {
        public TestQueryRequest() {}

        public TestQueryRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class TestQueryTransportAction extends HandledTransportAction<TestQueryRequest, ActionResponse.Empty> {
        @Inject
        public TestQueryTransportAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool) {
            super(TEST_QUERY_ACTION.name(), transportService, actionFilters, TestQueryRequest::new, threadPool.generic());
        }

        @Override
        protected void doExecute(Task task, TestQueryRequest request, ActionListener<ActionResponse.Empty> listener) {
            CountDownLatch started = TestPlugin.actionStarted.get();
            CountDownLatch release = TestPlugin.releaseAction.get();
            if (started != null && release != null) {
                started.countDown();
                try {
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(e);
                    return;
                }
            }
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }
    }
}
