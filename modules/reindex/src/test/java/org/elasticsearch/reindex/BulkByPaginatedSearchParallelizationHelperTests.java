/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.executeSlicedAction;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.initTaskState;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.sliceIntoSubRequests;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BulkByPaginatedSearchParallelizationHelperTests extends ESTestCase {

    private ThreadPool threadPool;
    private TaskManager taskManager;
    private NamedXContentRegistry searchRegistry;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (searchRegistry == null) {
            searchRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        }
        return searchRegistry;
    }

    @Before
    public void setUpTaskManager() {
        threadPool = new TestThreadPool(getTestName());
        taskManager = new TaskManager(Settings.EMPTY, threadPool, emptySet());
    }

    @After
    public void tearDownTaskManager() {
        terminate(threadPool);
    }

    public void testSliceIntoSubRequests() {
        SearchRequest searchRequest = randomSearchRequest(
            () -> randomSearchSourceBuilder(() -> null, () -> null, () -> null, Collections::emptyList, () -> null, () -> null)
        );
        if (searchRequest.source() != null) {
            // Clear the slice builder if there is one set. We can't call sliceIntoSubRequests if it is.
            searchRequest.source().slice(null);
        }
        int times = between(2, 100);
        int currentSliceId = 0;
        for (SearchRequest slice : sliceIntoSubRequests(searchRequest, times)) {
            assertNull(slice.source().slice().getField());
            assertEquals(currentSliceId, slice.source().slice().getId());
            assertEquals(times, slice.source().slice().getMax());

            // If you clear the slice then the slice should be the same request as the parent request
            slice.source().slice(null);
            if (searchRequest.source() == null) {
                // Except that adding the slice might have added an empty builder
                searchRequest.source(new SearchSourceBuilder());
            }
            assertEquals(searchRequest, slice);
            currentSliceId++;
        }
    }

    /**
     * Sliced sub-requests must keep the parent search's {@link PointInTimeBuilder} (id and keep-alive)
     * so all slices search against the same PIT while applying distinct {@link SliceBuilder} settings.
     */
    public void testSliceIntoSubRequestsPreservesPointInTimeBuilderOnEachSlice() {
        BytesReference pitId = new BytesArray(randomAlphaOfLengthBetween(8, 24));
        TimeValue keepAlive = TimeValue.timeValueMinutes(randomIntBetween(1, 30));
        int times = randomIntBetween(2, 8);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(keepAlive)));
        SearchRequest[] slices = sliceIntoSubRequests(request, times);
        assertThat(slices.length, equalTo(times));
        for (int i = 0; i < times; i++) {
            PointInTimeBuilder pit = slices[i].source().pointInTimeBuilder();
            assertNotNull(pit);
            assertThat(pit.getEncodedId(), equalTo(pitId));
            assertThat(pit.getKeepAlive(), equalTo(keepAlive));
            SliceBuilder slice = slices[i].source().slice();
            assertThat(slice.getField(), nullValue());
            assertThat(slice.getId(), equalTo(i));
            assertThat(slice.getMax(), equalTo(times));
        }
    }

    public void testAutoSlicesInitTaskStateForwardsSliceRoutingProvenance() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        ReindexRequest request = new ReindexRequest();
        request.getSearchRequest().indices("source-index");
        request.setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES);
        request.getSearchRequest().routing("slice-a,slice-b");
        request.getSearchRequest().searchSlice("slice-a,slice-b");

        BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
        AtomicReference<ClusterSearchShardsRequest> capturedSearchShardsRequest = new AtomicReference<>();
        Client client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request actionRequest,
                ActionListener<Response> listener
            ) {
                assertThat(action, sameInstance(TransportClusterSearchShardsAction.TYPE));
                capturedSearchShardsRequest.set((ClusterSearchShardsRequest) actionRequest);
                listener.onResponse(
                    (Response) new ClusterSearchShardsResponse(
                        new org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup[0],
                        new DiscoveryNode[0],
                        Map.of()
                    )
                );
            }
        };

        PlainActionFuture<Void> listener = new PlainActionFuture<>();
        initTaskState(task, request, client, listener);
        listener.actionGet();

        ClusterSearchShardsRequest forwarded = capturedSearchShardsRequest.get();
        assertNotNull(forwarded);
        assertThat(forwarded.routing(), equalTo("slice-a,slice-b"));
        assertThat(forwarded.searchSlice(), equalTo("slice-a,slice-b"));
        assertTrue(forwarded.isRoutingFromSlice());
    }

    /**
     * When the task is a worker, executeSlicedAction invokes the worker action with the given remote version.
     */
    public void testExecuteSlicedActionWithWorkerAndNonNullVersion() {
        ReindexRequest request = new ReindexRequest();
        BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorker(request.getRequestsPerSecond(), null);

        Version version = Version.CURRENT;
        AtomicReference<Version> capturedVersion = new AtomicReference<>();
        ActionListener<BulkByPaginatedSearchResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, version, capturedVersion::set);

        assertThat(capturedVersion.get(), sameInstance(version));
    }

    /**
     * When the task is a worker and remote version is null (local reindex), the worker action receives null.
     */
    public void testExecuteSlicedActionWithWorkerAndNullVersion() {
        ReindexRequest request = new ReindexRequest();
        BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorker(request.getRequestsPerSecond(), null);

        AtomicReference<Version> capturedVersion = new AtomicReference<>(Version.CURRENT);
        ActionListener<BulkByPaginatedSearchResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, null, capturedVersion::set);

        assertThat(capturedVersion.get(), nullValue());
    }

    /**
     * When the task is neither a leader nor a worker (not initialized), executeSlicedAction throws.
     */
    public void testExecuteSlicedActionThrowsWhenTaskNotInitialized() {
        ReindexRequest request = new ReindexRequest();
        BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
        // Do not call setWorker or setWorkerCount

        ActionListener<BulkByPaginatedSearchResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        AssertionError e = expectThrows(
            AssertionError.class,
            () -> executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, null, v -> {})
        );
        assertThat(e.getMessage(), containsString("initialized"));
    }

    /**
     * When resuming a sliced task with completed slices, each incomplete child gets
     * capturedRPS / incompleteSlices rather than capturedRPS / totalSlices.
     */
    public void testResumedSlicedTaskDistributesRpsAmongIncompleteSlices() {
        final float capturedRps = 60f;
        final int totalSlices = 4;
        final int completedSliceCount = 2;
        final int incompleteSliceCount = totalSlices - completedSliceCount;

        // Build resume info: slices 0,1 completed; slices 2,3 incomplete
        Map<Integer, ResumeInfo.SliceStatus> slices = new LinkedHashMap<>();
        for (int i = 0; i < completedSliceCount; i++) {
            BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
                i,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                TimeValue.ZERO,
                0f,
                null,
                TimeValue.ZERO
            );
            BulkByPaginatedSearchResponse sliceResponse = new BulkByPaginatedSearchResponse(
                TimeValue.ZERO,
                status,
                List.of(),
                List.of(),
                false
            );
            slices.put(i, new ResumeInfo.SliceStatus(i, null, new ResumeInfo.WorkerResult(sliceResponse, null)));
        }
        for (int i = completedSliceCount; i < totalSlices; i++) {
            ResumeInfo.ScrollWorkerResumeInfo workerInfo = new ResumeInfo.ScrollWorkerResumeInfo(
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                new BulkByPaginatedSearchTask.Status(i, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimeValue.ZERO, 0f, null, TimeValue.ZERO),
                null
            );
            slices.put(i, new ResumeInfo.SliceStatus(i, workerInfo, null));
        }

        ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new org.elasticsearch.tasks.TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
        ResumeInfo resumeInfo = new ResumeInfo(origin, null, slices);

        ReindexRequest request = new ReindexRequest();
        request.setRequestsPerSecond(capturedRps);
        request.setSlices(totalSlices);
        request.setResumeInfo(resumeInfo);

        BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorkerCount(totalSlices, capturedRps);

        List<ReindexRequest> capturedChildRequests = new ArrayList<>();
        Client client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request childRequest,
                ActionListener<Response> listener
            ) {
                capturedChildRequests.add((ReindexRequest) childRequest);
            }
        };

        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();
        executeSlicedAction(task, request, ReindexAction.INSTANCE, ActionListener.noop(), client, node, null, v -> {});

        // Only incomplete slices should generate client.execute calls
        assertThat(capturedChildRequests.size(), equalTo(incompleteSliceCount));
        float expectedChildRps = capturedRps / incompleteSliceCount;
        for (ReindexRequest childRequest : capturedChildRequests) {
            assertThat(childRequest.getRequestsPerSecond(), equalTo(expectedChildRps));
        }
    }

    /**
     * When the shard-count lookup fails during {@code slices=auto} initialisation, the parse-time
     * breaker charge on the search source must be released. The {@code closingListener} installed
     * in {@code startSlicedAction} before {@code initTaskState} is responsible for this.
     */
    public void testBreakerReleasedOnAutoSlicesInitFailure() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            ReindexRequest request = new ReindexRequest();
            request.getSearchRequest().indices("source-index");
            request.setSlices(AbstractBulkByPaginatedSearchRequest.AUTO_SLICES);
            request.getSearchRequest().source(new SearchSourceBuilder());
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"query\":{\"match_all\":{}}}")) {
                request.getSearchRequest().source().parseXContent(parser, false, nf -> false);
            }
            assertThat("breaker must be charged after parse", breaker.getUsed(), greaterThan(0L));

            BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
            Client failingClient = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request actionRequest,
                    ActionListener<Response> listener
                ) {
                    listener.onFailure(new RuntimeException("index not found"));
                }
            };
            DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();
            BulkByPaginatedSearchParallelizationHelper.startSlicedAction(
                request,
                task,
                ReindexAction.INSTANCE,
                ActionListener.noop(),
                failingClient,
                node,
                wrappedListener -> {
                    throw new AssertionError("worker action must not be invoked on init failure");
                }
            );
            assertEquals("breaker must be fully released after init failure", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    /**
     * In the multi-slice (leader) path, the parse-time breaker charge must remain positive while
     * any slice is outstanding and must drop to zero only when the final slice completes.
     */
    public void testBreakerHeldUntilFinalSliceCompletes() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            ReindexRequest request = new ReindexRequest();
            request.getSearchRequest().indices("source-index");
            request.setSlices(2);
            request.getSearchRequest().source(new SearchSourceBuilder());
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"query\":{\"match_all\":{}}}")) {
                request.getSearchRequest().source().parseXContent(parser, false, nf -> false);
            }
            assertThat("breaker must be charged after parse", breaker.getUsed(), greaterThan(0L));

            BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
            task.setWorkerCount(2, request.getRequestsPerSecond());

            List<ActionListener<BulkByPaginatedSearchResponse>> sliceListeners = new ArrayList<>();
            Client client = new NoOpClient(threadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Req extends ActionRequest, Resp extends ActionResponse> void doExecute(
                    ActionType<Resp> action,
                    Req req,
                    ActionListener<Resp> listener
                ) {
                    sliceListeners.add((ActionListener<BulkByPaginatedSearchResponse>) listener);
                }
            };

            DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();
            executeSlicedAction(task, request, ReindexAction.INSTANCE, ActionListener.noop(), client, node, null, v -> {});

            assertEquals("two slice requests must be dispatched", 2, sliceListeners.size());
            assertThat("breaker must be held while slices are outstanding", breaker.getUsed(), greaterThan(0L));

            sliceListeners.get(0).onResponse(emptySliceResponse(0));
            assertThat("breaker must still be held after only the first slice completes", breaker.getUsed(), greaterThan(0L));

            sliceListeners.get(1).onResponse(emptySliceResponse(1));
            assertEquals("breaker must be released after the final slice completes", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    /**
     * In the worker (single-slice) path, the parse-time breaker charge must remain positive while
     * the worker search is in flight and must drop to zero only when the worker's listener fires —
     * including via cancellation detected mid-search.
     */
    public void testBreakerHeldDuringWorkerExecution() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            ReindexRequest request = new ReindexRequest();
            request.getSearchRequest().indices("source-index");
            request.setSlices(1);
            request.getSearchRequest().source(new SearchSourceBuilder());
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"query\":{\"match_all\":{}}}")) {
                request.getSearchRequest().source().parseXContent(parser, false, nf -> false);
            }
            assertThat("breaker must be charged after parse", breaker.getUsed(), greaterThan(0L));

            BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
            DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

            // Worker stores the wrapped listener without calling it, simulating a search in flight.
            AtomicReference<Runnable> triggerCancellation = new AtomicReference<>();
            BulkByPaginatedSearchParallelizationHelper.startSlicedAction(
                request,
                task,
                ReindexAction.INSTANCE,
                ActionListener.noop(),
                null,
                node,
                wrappedListener -> triggerCancellation.set(
                    () -> wrappedListener.onFailure(new RuntimeException("task cancelled mid-reindex"))
                )
            );

            assertThat("breaker must be held while the worker search is outstanding", breaker.getUsed(), greaterThan(0L));
            triggerCancellation.get().run();
            assertEquals("breaker must be released after the worker search completes", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    private static BulkByPaginatedSearchResponse emptySliceResponse(int sliceId) {
        BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            sliceId,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            TimeValue.ZERO,
            0f,
            null,
            TimeValue.ZERO
        );
        return new BulkByPaginatedSearchResponse(TimeValue.ZERO, status, List.of(), List.of(), false);
    }

    /**
     * When a worker task is cancelled before its first search (i.e., the worker action calls
     * {@code wrappedListener.onFailure} without ever reaching {@code TransportSearchAction}), the
     * parse-time breaker charge on the original search source must be released via the wrapped listener
     * that {@code startSlicedAction} passes to the worker.
     */
    public void testBreakerReleasedOnWorkerCancellationBeforeSearch() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE / 2));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            ReindexRequest request = new ReindexRequest();
            request.getSearchRequest().indices("source-index");
            request.setSlices(1);
            request.getSearchRequest().source(new SearchSourceBuilder());
            try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"query\":{\"match_all\":{}}}")) {
                request.getSearchRequest().source().parseXContent(parser, false, nf -> false);
            }
            assertThat("breaker must be charged after parse", breaker.getUsed(), greaterThan(0L));

            BulkByPaginatedSearchTask task = (BulkByPaginatedSearchTask) taskManager.register("reindex", ReindexAction.NAME, request);
            DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();
            // Worker action immediately fails (simulates cancellation before the first search call).
            BulkByPaginatedSearchParallelizationHelper.startSlicedAction(
                request,
                task,
                ReindexAction.INSTANCE,
                ActionListener.noop(),
                null,
                node,
                wrappedListener -> wrappedListener.onFailure(new RuntimeException("task cancelled"))
            );
            assertEquals("breaker must be fully released after worker cancellation", 0L, breaker.getUsed());
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }
}
