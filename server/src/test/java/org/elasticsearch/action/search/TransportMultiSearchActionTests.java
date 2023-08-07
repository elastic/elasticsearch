/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.PlainActionFuture.newFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiSearchActionTests extends ESTestCase {

    public void testParentTaskId() throws Exception {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            );
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

            String localNodeId = randomAlphaOfLengthBetween(3, 10);
            int numSearchRequests = randomIntBetween(1, 100);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }
            AtomicInteger counter = new AtomicInteger(0);
            Task task = multiSearchRequest.createTask(randomLong(), "type", "action", null, Collections.emptyMap());
            NodeClient client = new NodeClient(settings, threadPool) {
                @Override
                public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
                    assertEquals(task.getId(), request.getParentTask().getId());
                    assertEquals(localNodeId, request.getParentTask().getNodeId());
                    counter.incrementAndGet();
                    listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
                }

                @Override
                public String getLocalNodeId() {
                    return localNodeId;
                }
            };
            TransportMultiSearchAction action = new TransportMultiSearchAction(
                threadPool,
                actionFilters,
                transportService,
                clusterService,
                10,
                System::nanoTime,
                client
            );

            PlainActionFuture<MultiSearchResponse> future = newFuture();
            action.execute(task, multiSearchRequest, future);
            future.get();
            assertEquals(numSearchRequests, counter.get());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    public void testBatchExecute() {
        // Initialize dependencies of TransportMultiSearchAction
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet()
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());

        // Keep track of the number of concurrent searches started by multi search api,
        // and if there are more searches than is allowed create an error and remember that.
        int maxAllowedConcurrentSearches = scaledRandomIntBetween(1, 16);
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<AssertionError> errorHolder = new AtomicReference<>();
        // randomize whether or not requests are executed asynchronously
        final List<String> threadPoolNames = Arrays.asList(ThreadPool.Names.GENERIC, ThreadPool.Names.SAME);
        Randomness.shuffle(threadPoolNames);
        final ExecutorService commonExecutor = threadPool.executor(threadPoolNames.get(0));
        final ExecutorService rarelyExecutor = threadPool.executor(threadPoolNames.get(1));
        final Set<SearchRequest> requests = Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));
        NodeClient client = new NodeClient(settings, threadPool) {
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
                final ExecutorService executorService = rarely() ? rarelyExecutor : commonExecutor;
                executorService.execute(() -> {
                    counter.decrementAndGet();
                    listener.onResponse(
                        new SearchResponse(
                            InternalSearchResponse.EMPTY_WITH_TOTAL_HITS,
                            null,
                            0,
                            0,
                            0,
                            0L,
                            ShardSearchFailure.EMPTY_ARRAY,
                            SearchResponse.Clusters.EMPTY
                        )
                    );
                });
            }

            @Override
            public String getLocalNodeId() {
                return "local_node_id";
            }
        };

        TransportMultiSearchAction action = new TransportMultiSearchAction(
            threadPool,
            actionFilters,
            transportService,
            clusterService,
            10,
            System::nanoTime,
            client
        );

        // Execute the multi search api and fail if we find an error after executing:
        try {
            /*
             * Allow for a large number of search requests in a single batch as previous implementations could stack overflow if the number
             * of requests in a single batch was large
             */
            int numSearchRequests = scaledRandomIntBetween(1, 8192);
            MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
            multiSearchRequest.maxConcurrentSearchRequests(maxAllowedConcurrentSearches);
            for (int i = 0; i < numSearchRequests; i++) {
                multiSearchRequest.add(new SearchRequest());
            }

            MultiSearchResponse response = ActionTestUtils.executeBlocking(action, multiSearchRequest);
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            assertThat(requests.size(), equalTo(numSearchRequests));
            assertThat(errorHolder.get(), nullValue());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
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

}
