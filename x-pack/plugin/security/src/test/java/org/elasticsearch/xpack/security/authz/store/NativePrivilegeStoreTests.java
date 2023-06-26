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
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class NativePrivilegeStoreTests extends ESTestCase {

    private NativePrivilegeStore store;
    private List<ActionRequest> requests;
    private AtomicReference<ActionListener<ActionResponse>> listener;
    private Client client;
    private SecurityIndexManager securityIndex;
    private CacheInvalidatorRegistry cacheInvalidatorRegistry;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private boolean allowExpensiveQueries;

    @Before
    public void setup() {
        requests = new ArrayList<>();
        listener = new AtomicReference<>();
        client = new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                NativePrivilegeStoreTests.this.requests.add(request);
                NativePrivilegeStoreTests.this.listener.set((ActionListener<ActionResponse>) listener);
            }

            @Override
            public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                listener.onResponse(SearchResponse.empty(() -> 1L, SearchResponse.Clusters.EMPTY));
            }
        };
        securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isAvailable()).thenReturn(true);
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

        threadPool = new TestThreadPool(getTestName());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        allowExpensiveQueries = randomBoolean();
        store = new NativePrivilegeStore(
            setAllowExpensiveQueries(Settings.EMPTY),
            client,
            securityIndex,
            cacheInvalidatorRegistry,
            clusterService
        );
    }

    @After
    public void cleanup() {
        client.close();
        terminate(threadPool);
    }

    public void testGetSinglePrivilegeByName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = List.of(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("admin"), future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("""
            {"terms":{"application":["myapp"]"""));
        assertThat(query, containsString("""
            {"term":{"type":{"value":"application-privilege\""""));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertResult(sourcePrivileges, future);
    }

    public void testGetMissingPrivilege() throws InterruptedException, ExecutionException, TimeoutException {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("admin"), future);
        final SearchHit[] hits = new SearchHit[0];
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        final Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors = future.get(1, TimeUnit.SECONDS);
        assertThat(applicationPrivilegeDescriptors, empty());
    }

    public void testGetPrivilegesByApplicationName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, anyOf(containsString("""
            {"terms":{"application":["myapp","yourapp"]"""), containsString("""
            {"terms":{"application":["yourapp","myapp"]""")));
        assertThat(query, containsString("""
            {"term":{"type":{"value":"application-privilege\""""));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertResult(sourcePrivileges, future);
    }

    public void testGetPrivilegesByWildcardApplicationName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                "theapp",
                randomAlphaOfLength(5),
                newHashSet("action:" + randomAlphaOfLength(5) + "/*"),
                emptyMap()
            ),
            new ApplicationPrivilegeDescriptor(
                "myapp-1",
                randomAlphaOfLength(5),
                newHashSet("action:" + randomAlphaOfLength(5) + "/*"),
                emptyMap()
            ),
            new ApplicationPrivilegeDescriptor(
                "myapp-2",
                randomAlphaOfLength(5),
                newHashSet("action:" + randomAlphaOfLength(5) + "/*"),
                emptyMap()
            ),
            new ApplicationPrivilegeDescriptor(
                "yourapp",
                randomAlphaOfLength(5),
                newHashSet("action:" + randomAlphaOfLength(5) + "/*"),
                emptyMap()
            ),
            new ApplicationPrivilegeDescriptor(
                "theirapp",
                randomAlphaOfLength(5),
                newHashSet("action:" + randomAlphaOfLength(5) + "/*"),
                emptyMap()
            )
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp-*", "yourapp"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        if (allowExpensiveQueries) {
            assertThat(query, containsString("{\"bool\":{\"should\":[{\"terms\":{\"application\":[\"yourapp\"]"));
            assertThat(query, containsString("{\"prefix\":{\"application\":{\"value\":\"myapp-\""));
            assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));
        } else {
            assertThat(
                query,
                equalTo("{\"bool\":{\"filter\":[{\"term\":{\"type\":{\"value\":\"application-privilege\"}}}],\"boost\":1.0}}")
            );
        }

        final SearchHit[] hits = buildHits(allowExpensiveQueries ? sourcePrivileges.subList(1, 4) : sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );
        // The first and last privilege should not be retrieved
        assertResult(sourcePrivileges.subList(1, 4), future);
    }

    public void testGetPrivilegesByStarApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("*", "anything"), null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"exists\":{\"field\":\"application\""));
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));

        final SearchHit[] hits = new SearchHit[0];
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );
    }

    public void testGetAllPrivileges() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app1", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app2", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app3", "all", newHashSet("*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(null, null, future);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));
        SearchRequest request = (SearchRequest) requests.get(0);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));
        assertThat(query, not(containsString("{\"terms\"")));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertResult(sourcePrivileges, future);
    }

    public void testGetPrivilegesCacheByApplicationNames() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp", "yourapp"), null, future);

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertEquals(Set.of("myapp"), store.getApplicationNamesCache().get(Set.of("myapp", "yourapp")));
        assertEquals(Set.copyOf(sourcePrivileges), store.getDescriptorsCache().get("myapp"));
        assertResult(sourcePrivileges, future);

        // The 2nd call should use cache and success
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp", "yourapp"), null, future2);
        listener.get().onResponse(null);
        assertResult(sourcePrivileges, future2);

        // The 3rd call should use cache when the application name is part of the original query
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future3 = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), null, future3);
        listener.get().onResponse(null);
        // Does not cache the name expansion if descriptors of the literal name is already cached
        assertNull(store.getApplicationNamesCache().get(Set.of("myapp")));
        assertResult(sourcePrivileges, future3);
    }

    public void testGetPrivilegesCacheWithApplicationAndPrivilegeName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = List.of(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("myapp"), singletonList("user"), future);

        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        // Not caching names with no wildcard
        assertNull(store.getApplicationNamesCache().get(singleton("myapp")));
        // All privileges are cached
        assertEquals(Set.copyOf(sourcePrivileges), store.getDescriptorsCache().get("myapp"));
        assertResult(sourcePrivileges.subList(1, 2), future);

        // 2nd call with more privilege names can still use the cache
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("myapp"), List.of("user", "author"), future2);
        listener.get().onResponse(null);
        assertResult(sourcePrivileges.subList(1, 3), future2);
    }

    public void testGetPrivilegesCacheWithNonExistentApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("no-such-app"), null, future);
        final SearchHit[] hits = buildHits(emptyList());
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("no-such-app")));
        assertEquals(0, store.getDescriptorsCache().count());
        assertResult(emptyList(), future);

        // The 2nd call should use cache
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("no-such-app"), null, future2);
        listener.get().onResponse(null);
        assertResult(emptyList(), future2);
    }

    public void testGetPrivilegesCacheWithDifferentMatchAllApplicationNames() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(emptyList(), null, future);
        final SearchHit[] hits = buildHits(emptyList());
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );
        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("*")));
        assertEquals(1, store.getApplicationNamesCache().count());
        assertResult(emptyList(), future);

        // The 2nd call should use cache should translated to match all since it has a "*"
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(List.of("a", "b", "*", "c"), null, future2);
        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("*")));
        assertEquals(1, store.getApplicationNamesCache().count());
        assertResult(emptyList(), future2);

        // The 3rd call also translated to match all
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future3 = new PlainActionFuture<>();
        store.getPrivileges(null, null, future3);
        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("*")));
        assertEquals(1, store.getApplicationNamesCache().count());
        assertResult(emptyList(), future3);

        // The 4th call is also match all
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future4 = new PlainActionFuture<>();
        store.getPrivileges(List.of("*"), null, future4);
        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("*")));
        assertEquals(1, store.getApplicationNamesCache().count());
        assertResult(emptyList(), future4);
    }

    public void testStaleResultsWillNotBeCached() {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = singletonList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(null, null, future);

        // Before the results can be cached, invalidate the cache to simulate stale search results
        store.getDescriptorsAndApplicationNamesCache().invalidateAll();
        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        // Nothing should be cached since the results are stale
        assertEquals(0, store.getApplicationNamesCache().count());
        assertEquals(0, store.getDescriptorsCache().count());
    }

    public void testWhenStaleResultsAreCachedTheyWillBeCleared() throws InterruptedException {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = singletonList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap())
        );

        final CountDownLatch getPrivilegeCountDown = new CountDownLatch(1);
        final CountDownLatch invalidationCountDown = new CountDownLatch(1);
        // Use subclass so we can put the caching process on hold, which allows time to fire the cache invalidation call
        // When the process reaches the overridden method, it already acquires the read lock.
        // Hence the cache invalidation will be block at acquiring the write lock.
        // This simulates the scenario when stale results are cached just before the invalidation call arrives.
        // In this case, we guarantee the cache will be invalidate and the stale results won't stay for long.
        final NativePrivilegeStore store1 = new NativePrivilegeStore(
            setAllowExpensiveQueries(Settings.EMPTY),
            client,
            securityIndex,
            new CacheInvalidatorRegistry(),
            clusterService
        ) {
            @Override
            protected void cacheFetchedDescriptors(
                Set<String> applicationNamesCacheKey,
                Map<String, Set<ApplicationPrivilegeDescriptor>> mapOfFetchedDescriptors,
                long invalidationCount
            ) {
                getPrivilegeCountDown.countDown();
                try {
                    // wait till the invalidation call is at the door step
                    invalidationCountDown.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                super.cacheFetchedDescriptors(applicationNamesCacheKey, mapOfFetchedDescriptors, invalidationCount);
                // Assert that cache is successful
                assertEquals(1, getApplicationNamesCache().count());
                assertEquals(1, getDescriptorsCache().count());
            }
        };
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store1.getPrivileges(null, null, future);
        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        // Make sure the caching is about to happen
        getPrivilegeCountDown.await(5, TimeUnit.SECONDS);
        // Fire the invalidation call in another thread
        new Thread(() -> {
            // Let the caching proceed
            invalidationCountDown.countDown();
            store.getDescriptorsAndApplicationNamesCache().invalidateAll();
        }).start();
        // The cache should be cleared
        assertEquals(0, store.getApplicationNamesCache().count());
        assertEquals(0, store.getDescriptorsCache().count());
    }

    public void testPutPrivileges() throws Exception {
        final List<ApplicationPrivilegeDescriptor> putPrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app1", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app1", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app2", "all", newHashSet("*"), emptyMap())
        );

        final PlainActionFuture<Map<String, List<String>>> putPrivilegeFuture = new PlainActionFuture<>();
        store.putPrivileges(putPrivileges, WriteRequest.RefreshPolicy.IMMEDIATE, putPrivilegeFuture);
        assertThat(requests, iterableWithSize(putPrivileges.size()));
        assertThat(requests, everyItem(instanceOf(IndexRequest.class)));

        final List<IndexRequest> indexRequests = new ArrayList<>(requests.size());
        requests.stream().map(IndexRequest.class::cast).forEach(indexRequests::add);
        requests.clear();

        final ActionListener<ActionResponse> indexListener = listener.get();
        final String uuid = UUIDs.randomBase64UUID(random());
        for (int i = 0; i < putPrivileges.size(); i++) {
            ApplicationPrivilegeDescriptor privilege = putPrivileges.get(i);
            IndexRequest request = indexRequests.get(i);
            assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));
            assertThat(request.id(), equalTo("application-privilege_" + privilege.getApplication() + ":" + privilege.getName()));
            final XContentBuilder builder = privilege.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), true);
            assertThat(request.source(), equalTo(BytesReference.bytes(builder)));
            final boolean created = privilege.getName().equals("user") == false;
            indexListener.onResponse(
                new IndexResponse(new ShardId(SecuritySystemIndices.SECURITY_MAIN_ALIAS, uuid, i), request.id(), 1, 1, 1, created)
            );
        }

        assertBusy(() -> assertFalse(requests.isEmpty()), 1, TimeUnit.SECONDS);

        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(ClearPrivilegesCacheRequest.class));
        listener.get().onResponse(null);

        final Map<String, List<String>> map = putPrivilegeFuture.actionGet();
        assertThat(map.entrySet(), iterableWithSize(2));
        assertThat(map.get("app1"), iterableWithSize(1));
        assertThat(map.get("app2"), iterableWithSize(1));
        assertThat(map.get("app1"), contains("admin"));
        assertThat(map.get("app2"), contains("all"));
    }

    public void testRetrieveActionNamePatternsInsteadOfPrivileges() throws Exception {
        // test disabling caching
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        for (List<String> applications : List.<List<String>>of(
            List.of("myapp"),
            List.of("myapp*"),
            List.of("myapp", "myapp*"),
            List.of(),
            List.of("*"),
            List.of("myapp-2", "*")
        )) {
            Collection<String> actions = randomList(1, 4, () -> {
                String actionName = randomAlphaOfLengthBetween(0, 3) + randomFrom("*", "/", ":") + randomAlphaOfLengthBetween(0, 3)
                    + randomFrom("*", "/", ":", "");
                ApplicationPrivilege.validateActionName(actionName);
                return actionName;
            });
            Client mockClient = mock(Client.class);
            SecurityIndexManager mockSecurityIndexManager = mock(SecurityIndexManager.class);
            Settings settings = randomFrom(
                Settings.builder().put("xpack.security.authz.store.privileges.cache.ttl", 0).build(),
                Settings.EMPTY
            );
            NativePrivilegeStore store1 = new NativePrivilegeStore(
                setAllowExpensiveQueries(settings),
                mockClient,
                mockSecurityIndexManager,
                new CacheInvalidatorRegistry(),
                clusterService
            );
            store1.getPrivileges(applications, actions, future);
            assertResult(emptyList(), future);
            verifyNoInteractions(mockClient);
            verifyNoInteractions(mockSecurityIndexManager);
        }
    }

    public void testDeletePrivileges() throws Exception {
        final List<String> privilegeNames = Arrays.asList("p1", "p2", "p3");

        final PlainActionFuture<Map<String, List<String>>> future = new PlainActionFuture<>();
        store.deletePrivileges("app1", privilegeNames, WriteRequest.RefreshPolicy.IMMEDIATE, future);
        assertThat(requests, iterableWithSize(privilegeNames.size()));
        assertThat(requests, everyItem(instanceOf(DeleteRequest.class)));

        final List<DeleteRequest> deletes = new ArrayList<>(requests.size());
        requests.stream().map(DeleteRequest.class::cast).forEach(deletes::add);
        requests.clear();

        final ActionListener<ActionResponse> deleteListener = listener.get();
        final String uuid = UUIDs.randomBase64UUID(random());
        for (int i = 0; i < privilegeNames.size(); i++) {
            String name = privilegeNames.get(i);
            DeleteRequest request = deletes.get(i);
            assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));
            assertThat(request.id(), equalTo("application-privilege_app1:" + name));
            final boolean found = name.equals("p2") == false;
            deleteListener.onResponse(
                new DeleteResponse(new ShardId(SecuritySystemIndices.SECURITY_MAIN_ALIAS, uuid, i), request.id(), 1, 1, 1, found)
            );
        }

        assertBusy(() -> assertFalse(requests.isEmpty()), 1, TimeUnit.SECONDS);

        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(ClearPrivilegesCacheRequest.class));
        listener.get().onResponse(null);

        final Map<String, List<String>> map = future.actionGet();
        assertThat(map.entrySet(), iterableWithSize(1));
        assertThat(map.get("app1"), iterableWithSize(2));
        assertThat(map.get("app1"), containsInAnyOrder("p1", "p3"));
    }

    public void testInvalidate() {
        store.getApplicationNamesCache().put(singleton("*"), Set.of());
        store.getDescriptorsCache().put("app-1", singleton(new ApplicationPrivilegeDescriptor("app-1", "read", emptySet(), emptyMap())));
        store.getDescriptorsCache().put("app-2", singleton(new ApplicationPrivilegeDescriptor("app-2", "read", emptySet(), emptyMap())));
        store.getDescriptorsAndApplicationNamesCache().invalidate(singletonList("app-1"));
        assertEquals(0, store.getApplicationNamesCache().count());
        assertEquals(1, store.getDescriptorsCache().count());
    }

    public void testInvalidateAll() {
        store.getApplicationNamesCache().put(singleton("*"), Set.of());
        store.getDescriptorsCache().put("app-1", singleton(new ApplicationPrivilegeDescriptor("app-1", "read", emptySet(), emptyMap())));
        store.getDescriptorsCache().put("app-2", singleton(new ApplicationPrivilegeDescriptor("app-2", "read", emptySet(), emptyMap())));
        store.getDescriptorsAndApplicationNamesCache().invalidateAll();
        assertEquals(0, store.getApplicationNamesCache().count());
        assertEquals(0, store.getDescriptorsCache().count());
    }

    public void testCacheClearOnIndexHealthChange() {
        final String securityIndexName = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );

        long count = store.getNumInvalidation();

        // Cache should be cleared when security is back to green
        cacheInvalidatorRegistry.onSecurityIndexStateChange(
            dummyState(securityIndexName, true, randomFrom((ClusterHealthStatus) null, ClusterHealthStatus.RED)),
            dummyState(securityIndexName, true, randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW))
        );
        assertEquals(++count, store.getNumInvalidation());

        // Cache should be cleared when security is deleted
        cacheInvalidatorRegistry.onSecurityIndexStateChange(
            dummyState(securityIndexName, true, randomFrom(ClusterHealthStatus.values())),
            dummyState(securityIndexName, true, null)
        );
        assertEquals(++count, store.getNumInvalidation());

        // Cache should be cleared if indexUpToDate changed
        final boolean isIndexUpToDate = randomBoolean();
        final List<ClusterHealthStatus> allPossibleHealthStatus = CollectionUtils.appendToCopy(
            Arrays.asList(ClusterHealthStatus.values()),
            null
        );
        cacheInvalidatorRegistry.onSecurityIndexStateChange(
            dummyState(securityIndexName, isIndexUpToDate, randomFrom(allPossibleHealthStatus)),
            dummyState(securityIndexName, isIndexUpToDate == false, randomFrom(allPossibleHealthStatus))
        );
        assertEquals(++count, store.getNumInvalidation());
    }

    public void testCacheWillBeDisabledWhenTtlIsZero() {
        final Settings settings = Settings.builder().put("xpack.security.authz.store.privileges.cache.ttl", 0).build();
        final NativePrivilegeStore store1 = new NativePrivilegeStore(
            settings,
            client,
            securityIndex,
            new CacheInvalidatorRegistry(),
            clusterService
        );
        assertNull(store1.getApplicationNamesCache());
        assertNull(store1.getDescriptorsCache());
    }

    public void testGetPrivilegesWorkWithoutCache() throws Exception {
        final Settings settings = Settings.builder().put("xpack.security.authz.store.privileges.cache.ttl", 0).build();
        final NativePrivilegeStore store1 = new NativePrivilegeStore(
            settings,
            client,
            securityIndex,
            new CacheInvalidatorRegistry(),
            clusterService
        );
        assertNull(store1.getDescriptorsAndApplicationNamesCache());
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap())
        );
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store1.getPrivileges(singletonList("myapp"), null, future);
        final SearchHit[] hits = buildHits(sourcePrivileges);
        listener.get()
            .onResponse(
                new SearchResponse(
                    new SearchResponseSections(
                        new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 0f),
                        null,
                        null,
                        false,
                        false,
                        null,
                        1
                    ),
                    "_scrollId1",
                    1,
                    1,
                    0,
                    1,
                    null,
                    null
                )
            );

        assertResult(sourcePrivileges, future);
    }

    private SecurityIndexManager.State dummyState(
        String concreteSecurityIndexName,
        boolean isIndexUpToDate,
        ClusterHealthStatus healthStatus
    ) {
        return new SecurityIndexManager.State(
            Instant.now(),
            isIndexUpToDate,
            true,
            true,
            null,
            concreteSecurityIndexName,
            healthStatus,
            IndexMetadata.State.OPEN,
            null,
            "my_uuid"
        );
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

    private Settings setAllowExpensiveQueries(Settings settings) {
        return Settings.builder().put(settings).put(ALLOW_EXPENSIVE_QUERIES.getKey(), allowExpensiveQueries).build();
    }
}
