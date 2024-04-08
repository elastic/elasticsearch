/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService.ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AsyncSearchIntegTestCase extends ESIntegTestCase {
    interface SearchResponseIterator extends Iterator<AsyncSearchResponse>, Closeable {}

    public static class SearchTestPlugin extends Plugin implements SearchPlugin {
        public SearchTestPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return Arrays.asList(new QuerySpec<>(BlockingQueryBuilder.NAME, BlockingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            }),
                new QuerySpec<>(
                    ThrowingQueryBuilder.NAME,
                    ThrowingQueryBuilder::new,
                    p -> { throw new IllegalStateException("not implemented"); }
                )
            );
        }

        @Override
        public List<AggregationSpec> getAggregations() {
            return Collections.singletonList(
                new AggregationSpec(
                    CancellingAggregationBuilder.NAME,
                    CancellingAggregationBuilder::new,
                    (ContextParser<String, CancellingAggregationBuilder>) (p, c) -> {
                        throw new IllegalStateException("not implemented");
                    }
                ).addResultReader(InternalFilter::new)
            );
        }
    }

    @Before
    public void unpauseMaintenanceService() {
        for (AsyncTaskMaintenanceService service : internalCluster().getDataNodeInstances(AsyncTaskMaintenanceService.class)) {
            if (service.unpause()) {
                // force the service to start again
                ClusterState state = internalCluster().clusterService().state();
                service.clusterChanged(new ClusterChangedEvent("noop", state, state));
            }
        }
    }

    @After
    public void pauseMaintenanceService() {
        for (AsyncTaskMaintenanceService service : internalCluster().getDataNodeInstances(AsyncTaskMaintenanceService.class)) {
            service.pause();
        }
    }

    @After
    public void releaseQueryLatch() {
        BlockingQueryBuilder.releaseQueryLatch();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateCompositeXPackPlugin.class,
            AsyncSearch.class,
            AsyncResultsIndexPlugin.class,
            SearchTestPlugin.class,
            ReindexPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(0, otherSettings))
            .put(ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100))
            .build();
    }

    /**
     * Restart the node that runs the {@link TaskId} decoded from the provided {@link AsyncExecutionId}.
     */
    protected void restartTaskNode(String id, String indexName) throws Exception {
        AsyncExecutionId searchId = AsyncExecutionId.decode(id);
        final ClusterStateResponse clusterState = clusterAdmin().prepareState().clear().setNodes(true).get();
        DiscoveryNode node = clusterState.getState().nodes().get(searchId.getTaskId().getNodeId());

        // Temporarily stop garbage collection, making sure to wait for any in-flight tasks to complete
        pauseMaintenanceService();
        ensureAllSearchContextsReleased();

        internalCluster().restartNode(node.getName(), new InternalTestCluster.RestartCallback() {
        });
        unpauseMaintenanceService();
        ensureYellow(ASYNC_RESULTS_INDEX, indexName);
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id, TimeValue keepAlive) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id).setKeepAlive(keepAlive)).get();
    }

    protected AsyncStatusResponse getAsyncStatus(String id) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id)).get();
    }

    protected AsyncStatusResponse getAsyncStatus(String id, TimeValue keepAlive) throws ExecutionException, InterruptedException {
        return client().execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id).setKeepAlive(keepAlive)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id)).get();
    }

    /**
     * Wait the removal of the document decoded from the provided {@link AsyncExecutionId}.
     */
    protected void ensureTaskRemoval(String id) throws Exception {
        AsyncExecutionId searchId = AsyncExecutionId.decode(id);
        assertBusy(() -> {
            GetResponse resp = client().prepareGet().setIndex(ASYNC_RESULTS_INDEX).setId(searchId.getDocId()).get();
            assertFalse(resp.isExists());
        });
    }

    protected void ensureTaskNotRunning(String id) throws Exception {
        assertBusy(() -> {
            try {
                AsyncSearchResponse resp = getAsyncSearch(id);
                try {
                    assertFalse(resp.isRunning());
                } finally {
                    resp.decRef();
                }
            } catch (Exception exc) {
                if (ExceptionsHelper.unwrapCause(exc.getCause()) instanceof ResourceNotFoundException == false) {
                    throw exc;
                }
            }
        });
    }

    /**
     * Wait the completion of the {@link TaskId} decoded from the provided {@link AsyncExecutionId}.
     */
    protected void ensureTaskCompletion(String id) throws Exception {
        assertBusy(() -> {
            TaskId taskId = AsyncExecutionId.decode(id).getTaskId();
            try {
                GetTaskResponse resp = clusterAdmin().prepareGetTask(taskId).get();
                assertNull(resp.getTask());
            } catch (Exception exc) {
                if (exc.getCause() instanceof ResourceNotFoundException == false) {
                    throw exc;
                }
            }
        });
    }

    /**
     * Returns a {@link SearchResponseIterator} that blocks query shard executions
     * until {@link SearchResponseIterator#next()} is called. That allows to randomly
     * generate partial results that can be consumed in order.
     */
    protected SearchResponseIterator assertBlockingIterator(
        String indexName,
        int numShards,
        SearchSourceBuilder source,
        int numFailures,
        int progressStep
    ) throws Exception {
        final String pitId;
        final SubmitAsyncSearchRequest request;
        if (randomBoolean()) {
            OpenPointInTimeRequest openPIT = new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(between(5, 10)));
            pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPIT).actionGet().getPointInTimeId();
            final PointInTimeBuilder pit = new PointInTimeBuilder(pitId);
            if (randomBoolean()) {
                pit.setKeepAlive(TimeValue.timeValueMillis(randomIntBetween(1, 3600)));
            }
            source.pointInTimeBuilder(pit);
            request = new SubmitAsyncSearchRequest(source);
        } else {
            pitId = null;
            request = new SubmitAsyncSearchRequest(source, indexName);
        }
        request.setBatchedReduceSize(progressStep);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        BlockingQueryBuilder.QueryLatch queryLatch = BlockingQueryBuilder.acquireQueryLatch(numFailures);
        request.getSearchRequest().source().query(new BlockingQueryBuilder(random().nextLong()));

        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(numShards));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));

        return new SearchResponseIterator() {
            private AsyncSearchResponse response = initial;
            private boolean isFirst = true;
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public boolean hasNext() {
                return response.isRunning();
            }

            @Override
            public AsyncSearchResponse next() {
                try {
                    return doNext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            private AsyncSearchResponse doNext() throws Exception {
                if (isFirst) {
                    isFirst = false;
                    return response;
                }
                queryLatch.countDownAndReset();
                AsyncSearchResponse newResponse = client().execute(
                    GetAsyncSearchAction.INSTANCE,
                    new GetAsyncResultRequest(response.getId()).setWaitForCompletionTimeout(TimeValue.timeValueMillis(10))
                ).get();

                if (newResponse.isRunning()) {
                    assertThat(newResponse.status(), equalTo(RestStatus.OK));
                    assertTrue(newResponse.isPartial());
                    assertNull(newResponse.getFailure());
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(numShards));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, lessThanOrEqualTo(numFailures));
                } else if (numFailures == numShards) {
                    assertThat(newResponse.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
                    assertNotNull(newResponse.getFailure());
                    assertTrue(newResponse.isPartial());
                    assertNotNull(newResponse.getSearchResponse());
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(numShards));
                    assertThat(newResponse.getSearchResponse().getSuccessfulShards(), equalTo(0));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, equalTo(numFailures));
                    assertNull(newResponse.getSearchResponse().getAggregations());
                    assertNotNull(newResponse.getSearchResponse().getHits().getTotalHits());
                    assertThat(newResponse.getSearchResponse().getHits().getTotalHits().value, equalTo(0L));
                    assertThat(
                        newResponse.getSearchResponse().getHits().getTotalHits().relation,
                        equalTo(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO)
                    );
                } else {
                    assertThat(newResponse.status(), equalTo(RestStatus.OK));
                    assertNotNull(newResponse.getSearchResponse());
                    assertFalse(newResponse.isPartial());
                    assertThat(newResponse.status(), equalTo(RestStatus.OK));
                    assertThat(newResponse.getSearchResponse().getTotalShards(), equalTo(numShards));
                    assertThat(newResponse.getSearchResponse().getShardFailures().length, equalTo(numFailures));
                    assertThat(
                        newResponse.getSearchResponse().getSuccessfulShards(),
                        equalTo(numShards - newResponse.getSearchResponse().getShardFailures().length)
                    );
                }
                return response = newResponse;
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    if (pitId != null) {
                        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
                    }
                    queryLatch.close();
                }
            }
        };
    }
}
