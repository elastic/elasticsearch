/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativePrivilegeStoreRetryTests extends ESTestCase {

    private NativePrivilegeStore store;
    private List<ActionRequest> requests;
    private AtomicReference<ActionListener<ActionResponse>> listener;
    private Client client;
    private SecurityIndexManager securityIndex;
    private CacheInvalidatorRegistry cacheInvalidatorRegistry;
    private CountDownLatch retryLatch;
    private ThreadPool threadPool;
    private ClusterService clusterService;

    private void setupStore(int expectedRetryCount) {
        requests = new ArrayList<>();
        listener = new AtomicReference<>();
        retryLatch = new CountDownLatch(expectedRetryCount);
        threadPool = new MockThreadPool(getTestName(), retryLatch);
        client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                NativePrivilegeStoreRetryTests.this.requests.add(request);
                NativePrivilegeStoreRetryTests.this.listener.set((ActionListener<ActionResponse>) listener);
                retryLatch.countDown();
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                ActionListener.respondAndRelease(listener, SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            }
        };
        securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.defensiveCopy()).thenReturn(securityIndex);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        cacheInvalidatorRegistry = new CacheInvalidatorRegistry();

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        store = new NativePrivilegeStore(Settings.EMPTY, client, securityIndex, cacheInvalidatorRegistry, clusterService);
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    public void testGetPrivilegesRetriesOnUnavailableShards() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );
        int numRetryableFailures = 4;
        setupStore(numRetryableFailures + 1);
        var mockk = when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS));
        for (int i = 0; i < numRetryableFailures; i++) {
            mockk = mockk.thenReturn(false);
        }
        mockk.thenReturn(true);
        when(securityIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(
            new UnavailableShardsException("index", 1, "bad shard")
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(retryLatch.await(20, TimeUnit.SECONDS));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        assertResult(sourcePrivileges, future);
    }

    private static class MockThreadPool extends TestThreadPool {

        private final List<TimeValue> delays = new ArrayList<>();
        private final CountDownLatch countDownLatch;

        MockThreadPool(String name, CountDownLatch countDownLatch, ExecutorBuilder<?>... customBuilders) {
            super(name, Settings.EMPTY, customBuilders);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
            delays.add(delay);
            countDownLatch.countDown();
            return super.schedule(command, TimeValue.ZERO, executor);
        }

    }

    private <T extends ActionRequest> T getLastRequest(Class<T> requestClass) {
        final ActionRequest last = requests.get(requests.size() - 1);
        assertThat(last, instanceOf(requestClass));
        return requestClass.cast(last);
    }

    private SearchHit[] buildHits(List<ApplicationPrivilegeDescriptor> sourcePrivileges) {
        final SearchHit[] hits = new SearchHit[sourcePrivileges.size()];
        for (int i = 0; i < hits.length; i++) {
            final ApplicationPrivilegeDescriptor p = sourcePrivileges.get(i);
            hits[i] = new SearchHit(i, "application-privilege_" + p.getApplication() + ":" + p.getName());
            hits[i].sourceRef(new BytesArray(Strings.toString(p)));
        }
        return hits;
    }

    private static SearchResponse buildSearchResponse(SearchHit[] hits) {
        var searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f);
        try {
            return new SearchResponse(searchHits.asUnpooled(), null, null, false, false, null, 1, "_scrollId1", 1, 1, 0, 1, null, null);
        } finally {
            searchHits.decRef();
        }
    }

    private void assertResult(
        List<ApplicationPrivilegeDescriptor> sourcePrivileges,
        PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future
    ) throws Exception {
        final Collection<ApplicationPrivilegeDescriptor> getPrivileges = future.get(1, TimeUnit.SECONDS);
        assertThat(getPrivileges, iterableWithSize(sourcePrivileges.size()));
        assertThat(new HashSet<>(getPrivileges), equalTo(new HashSet<>(sourcePrivileges)));
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
