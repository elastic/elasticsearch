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
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
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

    private MockThreadPool threadPool;

    private static final int MAX_NUMBER_OF_RETRIES = 8;

    @After
    public void cleanup() {
        terminate(threadPool);
        threadPool = null;
    }

    private NativePrivilegeStore setupStore(MockThreadPool threadPool, NoOpClient client, SecurityIndexManager securityIndex) {
        return new NativePrivilegeStore(
            Settings.EMPTY,
            client,
            securityIndex,
            new CacheInvalidatorRegistry(),
            ClusterServiceUtils.createClusterService(
                threadPool,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ),
            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), MAX_NUMBER_OF_RETRIES)
        );
    }

    private static SecurityIndexManager defaultSecurityIndex() {
        var securityIndex = mock(SecurityIndexManager.class);
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
        return securityIndex;
    }

    public void testGetPrivilegesSucceedsWithRetriesOnUnavailableShardFailures() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );
        int numRetryableFailures = randomIntBetween(1, MAX_NUMBER_OF_RETRIES);
        int totalCallsToLatch = numRetryableFailures + 1;
        var resultLatch = new CountDownLatch(totalCallsToLatch);
        threadPool = new MockThreadPool(getTestName(), resultLatch);

        var securityIndex = defaultSecurityIndex();
        var mockk = when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS));
        for (int i = 0; i < numRetryableFailures; i++) {
            mockk = mockk.thenReturn(false);
        }
        mockk.thenReturn(true);
        when(securityIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(unavailableShardsException());
        var listenerRef = new AtomicReference<ActionListener<ActionResponse>>();
        var store = setupStore(threadPool, new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listenerRef.set((ActionListener<ActionResponse>) listener);
                resultLatch.countDown();
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                ActionListener.respondAndRelease(listener, SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            }
        }, securityIndex);

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        ActionListener.respondAndRelease(listenerRef.get(), buildSearchResponse(buildHits(sourcePrivileges)));

        assertResult(sourcePrivileges, future);
        assertThat(threadPool.delays.size(), equalTo(numRetryableFailures));
    }

    public void testGetPrivilegesSucceedsWithRetriesOnUnavailableShardFailuresThrownDuringSearch() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );
        int numRetryableFailures = randomIntBetween(1, MAX_NUMBER_OF_RETRIES);
        int totalCallsToLatch = numRetryableFailures + 1;
        var resultLatch = new CountDownLatch(totalCallsToLatch);
        threadPool = new MockThreadPool(getTestName(), resultLatch);

        var securityIndex = defaultSecurityIndex();
        var listenerRef = new AtomicReference<ActionListener<ActionResponse>>();
        var store = setupStore(threadPool, new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (threadPool.delays.size() < numRetryableFailures) {
                    listener.onFailure(unavailableShardsException());
                } else {
                    listenerRef.set((ActionListener<ActionResponse>) listener);
                    resultLatch.countDown();
                }
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                ActionListener.respondAndRelease(listener, SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            }
        }, securityIndex);

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(resultLatch.await(5, TimeUnit.SECONDS));
        ActionListener.respondAndRelease(listenerRef.get(), buildSearchResponse(buildHits(sourcePrivileges)));

        assertResult(sourcePrivileges, future);
        assertThat(threadPool.delays.size(), equalTo(numRetryableFailures));
    }

    public void testGetPrivilegesThrowsWhenOutOfRetriesOnUnavailableShardFailures() throws Exception {
        var resultLatch = new CountDownLatch(MAX_NUMBER_OF_RETRIES);
        threadPool = new MockThreadPool(getTestName(), resultLatch);
        var securityIndex = defaultSecurityIndex();
        // always fails
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        when(securityIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(unavailableShardsException());

        var store = setupStore(threadPool, new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                fail("should not get here");
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                fail("should not get here");
            }
        }, securityIndex);

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(resultLatch.await(5, TimeUnit.SECONDS));

        expectThrows(UnavailableShardsException.class, future::actionGet);
        assertThat(threadPool.delays.size(), equalTo(MAX_NUMBER_OF_RETRIES));
    }

    public void testGetPrivilegesThrowsWhenOutOfRetriesOnUnavailableShardFailuresThrownDuringSearch() throws Exception {
        int numRetryableFailures = MAX_NUMBER_OF_RETRIES + 1;
        var resultLatch = new CountDownLatch(MAX_NUMBER_OF_RETRIES);
        threadPool = new MockThreadPool(getTestName(), resultLatch);
        var securityIndex = defaultSecurityIndex();

        var store = setupStore(threadPool, new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(unavailableShardsException());
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                ActionListener.respondAndRelease(listener, SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            }
        }, securityIndex);

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(resultLatch.await(5, TimeUnit.SECONDS));

        expectThrows(UnavailableShardsException.class, future::actionGet);
        assertThat(threadPool.delays.size(), equalTo(numRetryableFailures - 1));
    }

    public void testGetPrivilegesDoesNotRetryOnUnretriableFailure() throws Exception {
        var resultLatch = new CountDownLatch(0);
        threadPool = new MockThreadPool(getTestName(), resultLatch);
        var securityIndex = defaultSecurityIndex();
        // always fails
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        when(securityIndex.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(
            // we do not retry on index closed
            new IndexClosedException(new Index("name", ClusterState.UNKNOWN_UUID))
        );

        var store = setupStore(threadPool, new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                fail("should not get here");
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                fail("should not get here");
            }
        }, securityIndex);

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);

        assertTrue(resultLatch.await(5, TimeUnit.SECONDS));

        expectThrows(IndexClosedException.class, future::actionGet);
        assertThat(threadPool.delays.size(), equalTo(0));
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

    private static UnavailableShardsException unavailableShardsException() {
        return new UnavailableShardsException("index", 1, "bad shard");
    }
}
