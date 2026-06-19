/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ObjIntConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchActionTests extends ESTestCase {

    private Settings settings;
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
    }

    @After
    public void terminateThreadPool() {
        assertTrue(ESTestCase.terminate(threadPool));
    }

    public void testParentTaskId() throws Exception {
        String localNodeId = randomAlphaOfLengthBetween(3, 10);
        int numSearchRequests = randomIntBetween(1, 100);
        MultiSearchRequest multiSearchRequest = newMultiSearchRequest(numSearchRequests);
        AtomicInteger counter = new AtomicInteger(0);
        Task task = multiSearchRequest.createTask(randomLong(), "type", "action", null, Collections.emptyMap());
        NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                assertEquals(task.getId(), request.getParentTask().getId());
                assertEquals(localNodeId, request.getParentTask().getNodeId());
                counter.incrementAndGet();
                respondWithEmptySearchResponse(listener);
            }

            @Override
            public String getLocalNodeId() {
                return localNodeId;
            }
        };
        TransportMultiSearchAction action = createAction(client);

        PlainActionFuture<MultiSearchResponse> future = new PlainActionFuture<>();
        action.execute(task, multiSearchRequest, future);
        future.get();
        assertEquals(numSearchRequests, counter.get());
    }

    public void testBatchExecute() throws ExecutionException, InterruptedException {
        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = scaledRandomIntBetween(1, 16);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<AssertionError> errorHolder = new AtomicReference<>();
        // randomize whether or not requests are executed asynchronously
        final List<Executor> executorServices = Arrays.asList(threadPool.generic(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        Randomness.shuffle(executorServices);
        final Executor commonExecutor = executorServices.get(0);
        final Executor rarelyExecutor = executorServices.get(1);
        final Set<SearchRequest> requests = Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
        NodeClient client = new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                requests.add(request);
                int currentConcurrentSearches = counter.incrementAndGet();
                if (currentConcurrentSearches > maxAllowedConcurrentSearches) {
                    errorHolder.set(
                        new AssertionError(
                            "Current concurrent search ["
                                + currentConcurrentSearches
                                + "] is higher than is allowed ["
                                + maxAllowedConcurrentSearches
                                + "]"
                        )
                    );
                }
                final Executor executorService = rarely() ? rarelyExecutor : commonExecutor;
                executorService.execute(() -> {
                    counter.decrementAndGet();
                    respondWithEmptySearchResponse(listener);
                });
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };

        TransportMultiSearchAction action = createAction(client);

        // Execute the multi search api and fail if we find an error after executing:
        /*
         * Allow for a large number of search requests in a single batch as previous implementations could stack overflow if the number
         * of requests in a single batch was large
         */
        int numSearchRequests = scaledRandomIntBetween(1, 8192);
        MultiSearchRequest multiSearchRequest = newMultiSearchRequest(numSearchRequests);
        multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        ActionTestUtils.execute(action, multiSearchRequest, future.delegateFailure((l, response) -> {
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            assertThat(requests.size(), equalTo(numSearchRequests));
            assertThat(errorHolder.get(), nullValue());
            l.onResponse(null);
        }));
        future.get();
    }

    public void testDefaultMaxConcurrentSearches() {
        int numDataNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numDataNodes; i++) {
            builder.add(DiscoveryNodeUtils.builder("_id" + i).roles(Collections.singleton(DiscoveryNodeRole.DATA_ROLE)).build());
        }
        builder.add(DiscoveryNodeUtils.builder("master").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build());
        builder.add(DiscoveryNodeUtils.builder("ingest").roles(Collections.singleton(DiscoveryNodeRole.INGEST_ROLE)).build());

        ClusterState state = ClusterState.builder(new ClusterName("_name")).nodes(builder).build();
        int result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(10 * numDataNodes));

        state = ClusterState.builder(new ClusterName("_name")).build();
        result = TransportMultiSearchAction.defaultMaxConcurrentSearches(10, state);
        assertThat(result, equalTo(1));
    }

    public void testMultiSearchAggregatesSearchMetricsHeaders() throws Exception {
        int numSearchRequests = randomIntBetween(2, 8);

        NodeClient client = forkingSearchClient((listener, seq) -> {
            addStoreBytesReadHeader(seq * 1000L);
            respondWithEmptySearchResponse(listener);
        });
        TransportMultiSearchAction action = createAction(client);

        List<String> headerValues = executeAndCaptureSearchMetricsHeader(action, newMultiSearchRequest(numSearchRequests));

        assertThat(headerValues, notNullValue());
        assertThat(headerValues, hasSize(numSearchRequests));
        assertThat(sumStoreBytesRead(headerValues), equalTo(expectedTotalBytesRead(numSearchRequests)));
    }

    public void testMultiSearchAggregatesSearchMetricsHeadersOnlyForSuccessfulSubSearches() throws Exception {
        int numSuccessful = randomIntBetween(1, 4);
        int numFailing = randomIntBetween(1, 4);

        NodeClient client = forkingSearchClient((listener, seq) -> {
            if (seq <= numSuccessful) {
                addStoreBytesReadHeader(seq * 1000L);
                respondWithEmptySearchResponse(listener);
            } else {
                listener.onFailure(new IllegalStateException("simulated sub-search failure"));
            }
        });
        TransportMultiSearchAction action = createAction(client);

        List<String> headerValues = executeAndCaptureSearchMetricsHeader(action, newMultiSearchRequest(numSuccessful + numFailing));

        assertThat(headerValues, notNullValue());
        assertThat(headerValues, hasSize(numSuccessful));
        assertThat(sumStoreBytesRead(headerValues), equalTo(expectedTotalBytesRead(numSuccessful)));
    }

    private TransportMultiSearchAction createAction(NodeClient client) {
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
                .applySettings(settings)
                .address(boundAddress.publishAddress())
                .build(),
            null,
            Collections.emptySet()
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
        return new TransportMultiSearchAction(
            actionFilters,
            transportService,
            clusterService,
            10,
            System::nanoTime,
            client,
            DefaultProjectResolver.INSTANCE
        );
    }

    private NodeClient forkingSearchClient(ObjIntConsumer<ActionListener<SearchResponse>> onSearch) {
        AtomicInteger requestSeq = new AtomicInteger();
        return new NodeClient(settings, threadPool, TestProjectResolvers.alwaysThrow()) {
            @Override
            public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                final int seq = requestSeq.incrementAndGet();
                threadPool.generic().execute(() -> onSearch.accept(listener, seq));
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };
    }

    private void addStoreBytesReadHeader(long bytesRead) {
        threadPool.getThreadContext()
            .addResponseHeader(AbstractSearchAsyncAction.RESPONSE_HEADER_SEARCH_METRICS, "store_bytes_read=" + bytesRead);
    }

    private List<String> executeAndCaptureSearchMetricsHeader(TransportMultiSearchAction action, MultiSearchRequest request)
        throws Exception {
        AtomicReference<List<String>> headerValues = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        ActionTestUtils.execute(action, request, future.delegateFailure((l, response) -> {
            headerValues.set(
                threadPool.getThreadContext().getResponseHeaders().get(AbstractSearchAsyncAction.RESPONSE_HEADER_SEARCH_METRICS)
            );
            l.onResponse(null);
        }));
        future.get();
        return headerValues.get();
    }

    private static MultiSearchRequest newMultiSearchRequest(int numSearchRequests) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (int i = 0; i < numSearchRequests; i++) {
            multiSearchRequest.add(new SearchRequest());
        }
        return multiSearchRequest;
    }

    private static void respondWithEmptySearchResponse(ActionListener<SearchResponse> listener) {
        var response = SearchResponseUtils.emptyWithTotalHits(
            null,
            0,
            0,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static long sumStoreBytesRead(List<String> headerValues) {
        return headerValues.stream().mapToLong(value -> Long.parseLong(value.substring(value.indexOf('=') + 1))).sum();
    }

    private static long expectedTotalBytesRead(int count) {
        long total = 0L;
        for (int seq = 1; seq <= count; seq++) {
            total += seq * 1000L;
        }
        return total;
    }
}
