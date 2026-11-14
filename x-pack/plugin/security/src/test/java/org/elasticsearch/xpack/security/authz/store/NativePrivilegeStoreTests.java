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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore.DescriptorsAndApplicationNamesCache;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    private SecurityIndexManager.IndexState projectIndex;

    @Before
    public void setup() {
        requests = new ArrayList<>();
        listener = new AtomicReference<>();
        threadPool = createThreadPool();
        client = new NoOpClient(threadPool) {
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
                ActionListener.respondAndRelease(listener, SearchResponse.emptyResponseBuilder().tookInMillis(1L).build());
            }
        };
        securityIndex = mock(SecurityIndexManager.class);
        projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        Mockito.doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments().length, equalTo(2));
            assertThat(invocationOnMock.getArguments()[1], instanceOf(Runnable.class));
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(projectIndex).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        Mockito.doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final var l = (ActionListener<SecurityIndexManager.IndexState>) invocationOnMock.getArguments()[0];
            l.onResponse(projectIndex);
            return null;
        }).when(securityIndex).tryAwaitIndexAvailableForSearch(anyActionListener());
        cacheInvalidatorRegistry = new CacheInvalidatorRegistry();

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
        terminate(threadPool);
    }

    public void testGetSinglePrivilegeByName() throws Exception {
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = List.of(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("admin"), future);
        assertThat(requests, iterableWithSize(1));
        final SearchRequest request = getLastRequest(SearchRequest.class);
        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("""
            {"terms":{"application":["myapp"]"""));
        assertThat(query, containsString("""
            {"term":{"type":{"value":"application-privilege\""""));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        assertResult(sourcePrivileges, future);
    }

    public void testGetMissingPrivilege() throws InterruptedException, ExecutionException, TimeoutException {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("admin"), future);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(SearchHits.EMPTY));

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
        final SearchRequest request = getLastRequest(SearchRequest.class);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, anyOf(containsString("""
            {"terms":{"application":["myapp","yourapp"]"""), containsString("""
            {"terms":{"application":["yourapp","myapp"]""")));
        assertThat(query, containsString("""
            {"term":{"type":{"value":"application-privilege\""""));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

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
        final SearchRequest request = getLastRequest(SearchRequest.class);
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
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));
        // The first and last privilege should not be retrieved
        assertResult(sourcePrivileges.subList(1, 4), future);
    }

    public void testGetPrivilegesByStarApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("*", "anything"), null, future);
        assertThat(requests, iterableWithSize(1));
        final SearchRequest request = getLastRequest(SearchRequest.class);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"exists\":{\"field\":\"application\""));
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));

        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(SearchHits.EMPTY));
    }

    public void testGetPrivilegesSucceedsWhenHelperResolvesSnapshot() throws Exception {
        markShardsUnavailable();
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp", "author", newHashSet("action:login", "data:read/*", "data:write/*"), emptyMap())
        );

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, true, future);

        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(buildHits(sourcePrivileges)));

        assertResult(sourcePrivileges, future);
        verify(securityIndex, times(1)).tryAwaitIndexAvailableForSearch(anyActionListener());
    }

    public void testGetPrivilegesPropagatesHelperFailureWithNonShardsReason() {
        markShardsUnavailable();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var l = (ActionListener<SecurityIndexManager.IndexState>) invocation.getArguments()[0];
            l.onFailure(new IndexClosedException(new Index("potato", randomUUID())));
            return null;
        }).when(securityIndex).tryAwaitIndexAvailableForSearch(anyActionListener());

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, true, future);
        expectThrows(IndexClosedException.class, future::actionGet);
    }

    public void testGetPrivilegesPropagatesHelperFailureWithUnavailableShardsReason() {
        markShardsUnavailable();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var l = (ActionListener<SecurityIndexManager.IndexState>) invocation.getArguments()[0];
            l.onFailure(unavailableShardsException());
            return null;
        }).when(securityIndex).tryAwaitIndexAvailableForSearch(anyActionListener());

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, true, future);
        expectThrows(UnavailableShardsException.class, future::actionGet);
    }

    public void testGetPrivilegesReturnsEmptyWhenIndexNotFoundAfterHelperWait() throws Exception {
        markShardsUnavailable();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var l = (ActionListener<SecurityIndexManager.IndexState>) invocation.getArguments()[0];
            l.onFailure(new IndexNotFoundException("potato"));
            return null;
        }).when(securityIndex).tryAwaitIndexAvailableForSearch(anyActionListener());

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Arrays.asList("myapp", "yourapp"), null, true, future);
        assertThat(future.get(1, TimeUnit.SECONDS), empty());
    }

    private void markShardsUnavailable() {
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
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
        final SearchRequest request = getLastRequest(SearchRequest.class);
        assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));

        final String query = Strings.toString(request.source().query());
        assertThat(query, containsString("{\"term\":{\"type\":{\"value\":\"application-privilege\""));
        assertThat(query, not(containsString("{\"terms\"")));

        final SearchHit[] hits = buildHits(sourcePrivileges);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

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
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        assertEquals(Set.of("myapp"), store.getApplicationNamesCache().get(Set.of("myapp", "yourapp")));
        assertEquals(Set.copyOf(sourcePrivileges), store.getDescriptorsCache().get("myapp"));
        assertResult(sourcePrivileges, future);

        // The 2nd call should use cache and success
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp", "yourapp"), null, future2);
        assertThat(requests, hasSize(1));
        assertResult(sourcePrivileges, future2);

        // The 3rd call should use cache when the application name is part of the original query
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future3 = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), null, future3);
        assertThat(requests, hasSize(1));
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
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        // Not caching names with no wildcard
        assertNull(store.getApplicationNamesCache().get(singleton("myapp")));
        // All privileges are cached
        assertEquals(Set.copyOf(sourcePrivileges), store.getDescriptorsCache().get("myapp"));
        assertResult(sourcePrivileges.subList(1, 2), future);

        // 2nd call with more privilege names can still use the cache
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("myapp"), List.of("user", "author"), future2);
        assertThat(requests, hasSize(1));
        assertResult(sourcePrivileges.subList(1, 3), future2);
    }

    public void testGetPrivilegesCacheWithNonExistentApplicationName() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("no-such-app"), null, future);
        final SearchHit[] hits = buildHits(emptyList());
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        assertEquals(emptySet(), store.getApplicationNamesCache().get(singleton("no-such-app")));
        assertEquals(0, store.getDescriptorsCache().count());
        assertResult(emptyList(), future);

        // The 2nd call should use cache
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future2 = new PlainActionFuture<>();
        store.getPrivileges(Collections.singletonList("no-such-app"), null, future2);
        assertThat(requests, hasSize(1));
        assertResult(emptyList(), future2);
    }

    public void testGetPrivilegesCacheWithDifferentMatchAllApplicationNames() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        store.getPrivileges(emptyList(), null, future);
        final SearchHit[] hits = buildHits(emptyList());
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));
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

    public void testCacheIsClearedByApplicationNameWhenPrivilegesAreModified() throws Exception {
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> getFuture = new PlainActionFuture<>();
        store.getPrivileges(emptyList(), null, getFuture);
        final List<ApplicationPrivilegeDescriptor> sourcePrivileges = List.of(
            new ApplicationPrivilegeDescriptor("app1", "priv1a", Set.of("action:1a"), Map.of()),
            new ApplicationPrivilegeDescriptor("app1", "priv1b", Set.of("action:1b"), Map.of()),
            new ApplicationPrivilegeDescriptor("app2", "priv2a", Set.of("action:2a"), Map.of()),
            new ApplicationPrivilegeDescriptor("app2", "priv2b", Set.of("action:2b"), Map.of())
        );
        final SearchHit[] hits = buildHits(sourcePrivileges);
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));
        assertEquals(Set.of("app1", "app2"), store.getApplicationNamesCache().get(singleton("*")));
        assertResult(sourcePrivileges, getFuture);

        // add a new privilege to app1
        var priv1c = new ApplicationPrivilegeDescriptor("app1", "priv1c", Set.of("action:1c"), Map.of());
        PlainActionFuture<Map<String, Map<String, DocWriteResponse.Result>>> putFuture = new PlainActionFuture<>();
        store.putPrivileges(List.of(priv1c), WriteRequest.RefreshPolicy.IMMEDIATE, putFuture);

        handleBulkRequest(1, item -> true);

        assertCacheCleared("app1");

        Map<String, Map<String, DocWriteResponse.Result>> putResponse = putFuture.get();
        assertThat(putResponse, aMapWithSize(1));
        assertThat(putResponse, hasKey("app1"));
        assertThat(putResponse.get("app1"), aMapWithSize(1));
        assertThat(putResponse.get("app1"), hasEntry("priv1c", DocWriteResponse.Result.CREATED));

        // modify a privilege in app2
        var priv2a = new ApplicationPrivilegeDescriptor("app2", "priv2a", Set.of("action:2*"), Map.of());
        putFuture = new PlainActionFuture<>();
        store.putPrivileges(List.of(priv2a), WriteRequest.RefreshPolicy.IMMEDIATE, putFuture);

        handleBulkRequest(1, item -> false);
        assertCacheCleared("app2");

        putResponse = putFuture.get();
        assertThat(putResponse, aMapWithSize(1));
        assertThat(putResponse, hasKey("app2"));
        assertThat(putResponse.get("app2"), aMapWithSize(1));
        assertThat(putResponse.get("app2"), hasEntry("priv2a", DocWriteResponse.Result.UPDATED));

        // modify a privilege in app1, add a privilege in app2
        var priv1a = new ApplicationPrivilegeDescriptor("app1", "priv1a", Set.of("action:1*"), Map.of());
        var priv2c = new ApplicationPrivilegeDescriptor("app2", "priv2c", Set.of("action:2c"), Map.of());
        putFuture = new PlainActionFuture<>();
        store.putPrivileges(List.of(priv1a, priv2c), WriteRequest.RefreshPolicy.IMMEDIATE, putFuture);

        handleBulkRequest(2, item -> item.id().contains("app2"));
        assertCacheCleared("app1", "app2");

        putResponse = putFuture.get();
        assertThat(putResponse, aMapWithSize(2));
        assertThat(putResponse, hasKey("app1"));
        assertThat(putResponse.get("app1"), aMapWithSize(1));
        assertThat(putResponse.get("app1"), hasEntry("priv1a", DocWriteResponse.Result.UPDATED));
        assertThat(putResponse, hasKey("app2"));
        assertThat(putResponse.get("app2"), aMapWithSize(1));
        assertThat(putResponse.get("app2"), hasEntry("priv2c", DocWriteResponse.Result.CREATED));
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
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        // Nothing should be cached since the results are stale
        assertEquals(0, store.getApplicationNamesCache().count());
        assertEquals(0, store.getDescriptorsCache().count());
    }

    /**
     * A caller that arrives after an invalidation must not join an in-flight load that started before the invalidation, since that
     * load may return results that predate the invalidating write. It starts its own load, and only that load's result is cached.
     */
    public void testCallerAfterInvalidationDoesNotJoinOlderLoad() throws Exception {
        final DescriptorsAndApplicationNamesCache cache = newPrivilegeCache();
        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> callerBeforeInvalidation = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, loads::add, callerBeforeInvalidation);
        assertThat(loads, hasSize(1));

        cache.invalidateAll();

        // The first load is still in flight, but it started before the invalidation so this caller must not join it
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> callerAfterInvalidation = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, loads::add, callerAfterInvalidation);
        assertThat("a caller after an invalidation must start a new load", loads, hasSize(2));

        final ApplicationPrivilegeDescriptor staleDescriptor = myappPrivilege("stale");
        final ApplicationPrivilegeDescriptor freshDescriptor = myappPrivilege("fresh");
        loads.get(0).onResponse(List.of(staleDescriptor));
        loads.get(1).onResponse(List.of(freshDescriptor));
        assertThat(callerBeforeInvalidation.get(1, TimeUnit.SECONDS), containsInAnyOrder(staleDescriptor));
        assertThat(callerAfterInvalidation.get(1, TimeUnit.SECONDS), containsInAnyOrder(freshDescriptor));

        // Only the post-invalidation load's result was cached
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> cachedFuture = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, l -> fail("expected a cache hit"), cachedFuture);
        assertThat(cachedFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(freshDescriptor));
    }

    /**
     * Concurrent cache misses for the same application names share one load, so a burst of requests after a cache clear runs one
     * .security search rather than one per caller. The shared result is cached for later callers.
     */
    public void testConcurrentCacheMissesShareOneLoad() throws Exception {
        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        final DescriptorsAndApplicationNamesCache cache = newPrivilegeCache();

        // Issue every caller before the load completes, as a burst of requests after a cache clear would
        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();
        final List<PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>>> futures = new ArrayList<>();
        final int concurrentCallers = randomIntBetween(2, 20);
        for (int i = 0; i < concurrentCallers; i++) {
            final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
            cache.get(Set.of("myapp"), true, loads::add, future);
            futures.add(future);
        }
        assertThat("concurrent cache misses should share one load", loads, hasSize(1));

        loads.get(0).onResponse(List.of(descriptor));
        for (PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future : futures) {
            assertThat(future.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
        }

        // The shared result was cached, so a later caller is answered without a load
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> cachedFuture = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, l -> fail("expected a cache hit"), cachedFuture);
        assertThat(cachedFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
    }

    /**
     * Sharing applies across callers of the store, not only to identical requests: two concurrent getPrivileges calls for the same
     * application run a single search even when they ask for different privilege names, because the store always fetches all
     * privileges of an application and filters per caller. Each caller must only receive the privileges it asked for.
     */
    public void testConcurrentGetPrivilegesForSameApplicationShareOneSearch() throws Exception {
        final ApplicationPrivilegeDescriptor adminPrivilege = myappPrivilege("admin");
        final ApplicationPrivilegeDescriptor userPrivilege = myappPrivilege("user");

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> adminFuture = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("admin"), adminFuture);
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> userFuture = new PlainActionFuture<>();
        store.getPrivileges(List.of("myapp"), List.of("user"), userFuture);

        assertThat("both callers should share one search", requests, hasSize(1));
        assertThat(requests.get(0), instanceOf(SearchRequest.class));

        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(buildHits(List.of(adminPrivilege, userPrivilege))));

        assertThat(adminFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(adminPrivilege));
        assertThat(userFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(userPrivilege));
    }

    /**
     * Concurrency smoke test: several threads read through the cache while another thread updates the backing data and invalidates,
     * as a stream of privilege writes would. Every listener must complete exactly once, and once all threads are done the cache must
     * not serve anything loaded before the last invalidation.
     */
    public void testConcurrentGetsAndInvalidationsNeverLeaveStaleResults() {
        final DescriptorsAndApplicationNamesCache cache = newPrivilegeCache();
        // Stands in for the .security index: the invalidating thread updates it before invalidating, as a real write would
        final AtomicReference<ApplicationPrivilegeDescriptor> backingStore = new AtomicReference<>(myappPrivilege("v0"));
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loader = l -> l.onResponse(List.of(backingStore.get()));

        final int getterThreads = between(2, 4);
        final int getsPerThread = between(10, 30);
        final int invalidations = between(5, 15);
        startInParallel(getterThreads + 1, threadIndex -> {
            if (threadIndex == 0) {
                for (int i = 1; i <= invalidations; i++) {
                    backingStore.set(myappPrivilege("v" + i));
                    cache.invalidateAll();
                }
            } else {
                for (int i = 0; i < getsPerThread; i++) {
                    final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
                    cache.get(Set.of("myapp"), true, loader, ActionListener.assertOnce(future));
                    assertThat(safeGet(future), hasSize(1));
                }
            }
        });

        // Whatever the cache holds now must reflect the backing data as of the last invalidation
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, loader, future);
        assertThat(safeGet(future), containsInAnyOrder(backingStore.get()));
    }

    /**
     * A loader that throws instead of completing its listener must fail the shared load. If it did not, the in-flight entry would
     * never complete or be removed, and every later caller for the same key would wait on it forever. The failure must not be cached:
     * the next caller starts a fresh load.
     */
    public void testThrowingLoaderFailsCallerAndIsNotCached() throws Exception {
        final DescriptorsAndApplicationNamesCache cache = newPrivilegeCache();

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, l -> { throw new IllegalStateException("loader is broken"); }, future);
        final ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));

        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> retryFuture = new PlainActionFuture<>();
        cache.get(Set.of("myapp"), true, loads::add, retryFuture);
        assertThat("the failed load should not be joined or cached", loads, hasSize(1));
        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        loads.get(0).onResponse(List.of(descriptor));
        assertThat(retryFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
    }

    /**
     * Retrying from the creator's failure callback must start a fresh load rather than join the failed load whose listeners are
     * still being notified.
     */
    public void testFailureCallbackCanRetryWithNewLoad() throws Exception {
        final DescriptorsAndApplicationNamesCache cache = newPrivilegeCache();
        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> firstFuture = new PlainActionFuture<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> retryFuture = new PlainActionFuture<>();

        cache.get(Set.of("myapp"), true, loads::add, firstFuture.delegateResponse((l, e) -> {
            cache.get(Set.of("myapp"), true, loads::add, retryFuture);
            l.onFailure(e);
        }));
        assertThat(loads, hasSize(1));

        final IllegalStateException failure = new IllegalStateException("load failed");
        loads.get(0).onFailure(failure);
        final ExecutionException e = expectThrows(ExecutionException.class, () -> firstFuture.get(1, TimeUnit.SECONDS));
        assertSame(failure, e.getCause());
        assertThat("retrying from the failure callback should start a new load", loads, hasSize(2));
        assertFalse(retryFuture.isDone());

        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        loads.get(1).onResponse(List.of(descriptor));
        assertThat(retryFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
    }

    /**
     * A failure during the cache recheck must fail every subscriber and remove the in-flight entry so a later caller can retry.
     */
    public void testCacheRecheckFailureFailsSharedLoadAndAllowsRetry() throws Exception {
        final Set<String> applicationNames = Set.of("myapp");
        final IllegalStateException failure = new IllegalStateException("cache recheck failed");
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> creatorFuture = new PlainActionFuture<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> joinerFuture = new PlainActionFuture<>();

        // Inject a failure on the second iteration of cached names, after the shared load is registered. The first lookup
        // misses the descriptors cache.
        store.getApplicationNamesCache().put(applicationNames, new AbstractSet<>() {
            private boolean firstLookup = true;

            @Override
            public Iterator<String> iterator() {
                if (firstLookup) {
                    firstLookup = false;
                    return applicationNames.iterator();
                }
                // Remove the failing value before joining so the joining caller can reach the in-flight load.
                store.getApplicationNamesCache().invalidate(applicationNames);
                store.getPrivileges(applicationNames, null, joinerFuture);
                throw failure;
            }

            @Override
            public int size() {
                return applicationNames.size();
            }
        });

        store.getPrivileges(applicationNames, null, creatorFuture);
        for (PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future : List.of(creatorFuture, joinerFuture)) {
            final ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(1, TimeUnit.SECONDS));
            assertSame(failure, e.getCause());
        }
        assertThat("the cache recheck should fail before searching", requests, empty());

        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> retryFuture = new PlainActionFuture<>();
        store.getPrivileges(applicationNames, null, retryFuture);
        assertThat("the retry should start a new search", requests, hasSize(1));
        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(buildHits(List.of(descriptor))));
        assertThat(retryFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
    }

    /**
     * Shared loads must not inherit caller authentication or response headers, but must retain the project needed to resolve the
     * security index. Starting a load must also restore the calling thread's context.
     */
    public void testSharedLoadUsesCleanContextAndPreservesProject() throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final DescriptorsAndApplicationNamesCache cache = new DescriptorsAndApplicationNamesCache(
            TimeValue.timeValueMinutes(10),
            100,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            threadContext
        );
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final String projectId = randomUniqueProjectId().id();
        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();

        try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
            authentication.writeToContext(threadContext);
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
            threadContext.addResponseHeader("X-Test-Response", "creator");
            final Map<String, String> callerHeaders = threadContext.getHeaders();
            cache.get(Set.of("myapp"), true, l -> {
                assertNull(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                assertNull(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
                assertThat(threadContext.getResponseHeaders(), equalTo(Map.of()));
                assertThat(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(projectId));
                l.onResponse(List.of(descriptor));
            }, future);

            assertThat(threadContext.getHeaders(), equalTo(callerHeaders));
            assertSame(authentication, threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
            assertThat(threadContext.getResponseHeaders(), equalTo(Map.of("X-Test-Response", List.of("creator"))));
        }
        assertThat(future.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
    }

    /**
     * The caller that starts a shared load must see its original context on both success and failure, even if another request's
     * context is active when the load completes.
     */
    public void testCreatingCallerIsCompletedUnderItsOwnThreadContext() throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        for (boolean failLoad : new boolean[] { false, true }) {
            final DescriptorsAndApplicationNamesCache cache = new DescriptorsAndApplicationNamesCache(
                TimeValue.timeValueMinutes(10),
                100,
                threadPool.generic(),
                threadContext
            );
            final Authentication creatorAuthentication = AuthenticationTestHelper.builder().build();
            final Authentication completingAuthentication = AuthenticationTestHelper.builder().build();
            final AtomicReference<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loadListener = new AtomicReference<>();
            final AtomicReference<Map<String, String>> headersSeenByCreator = new AtomicReference<>();
            final AtomicReference<Authentication> authenticationSeenByCreator = new AtomicReference<>();
            final AtomicReference<Map<String, List<String>>> responseHeadersSeenByCreator = new AtomicReference<>();
            final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future = new PlainActionFuture<>();
            final Map<String, String> creatorHeaders;

            try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
                creatorAuthentication.writeToContext(threadContext);
                threadContext.putHeader("caller", "creator");
                threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, randomUniqueProjectId().id());
                threadContext.addResponseHeader("X-Test-Response", "creator");
                creatorHeaders = threadContext.getHeaders();
                cache.get(Set.of("myapp"), true, loadListener::set, ActionListener.runBefore(future, () -> {
                    headersSeenByCreator.set(threadContext.getHeaders());
                    authenticationSeenByCreator.set(threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
                    responseHeadersSeenByCreator.set(threadContext.getResponseHeaders());
                }));
            }

            final IllegalStateException failure = new IllegalStateException("load failed");
            try (ThreadContext.StoredContext ignored = threadContext.newEmptyContext()) {
                completingAuthentication.writeToContext(threadContext);
                threadContext.putHeader("caller", "completing");
                threadContext.addResponseHeader("X-Test-Response", "completing");
                if (failLoad) {
                    loadListener.get().onFailure(failure);
                } else {
                    loadListener.get().onResponse(List.of(descriptor));
                }
                assertThat(threadContext.getHeader("caller"), equalTo("completing"));
                assertSame(completingAuthentication, threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY));
                assertThat(threadContext.getResponseHeaders(), equalTo(Map.of("X-Test-Response", List.of("completing"))));
            }

            if (failLoad) {
                final ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
                assertSame(failure, e.getCause());
            } else {
                assertThat(future.get(10, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
            }
            assertThat(headersSeenByCreator.get(), equalTo(creatorHeaders));
            assertSame(creatorAuthentication, authenticationSeenByCreator.get());
            assertThat(responseHeadersSeenByCreator.get(), equalTo(Map.of("X-Test-Response", List.of("creator"))));
        }
    }

    /**
     * A caller that joins a shared load must observe its own thread context when its listener runs, even if the load completes under
     * the creating caller's context.
     */
    public void testJoiningCallerIsCompletedUnderItsOwnThreadContext() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final DescriptorsAndApplicationNamesCache cache = new DescriptorsAndApplicationNamesCache(
            TimeValue.timeValueMinutes(10),
            100,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            threadContext
        );

        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> creatorFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller", "creator");
            cache.get(Set.of("myapp"), true, loads::add, creatorFuture);
        }
        assertThat(loads, hasSize(1));

        final AtomicReference<String> headerSeenByJoiner = new AtomicReference<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> joinerFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller", "joiner");
            cache.get(
                Set.of("myapp"),
                true,
                l -> fail("should have joined the in-flight load"),
                ActionListener.runBefore(joinerFuture, () -> headerSeenByJoiner.set(threadContext.getHeader("caller")))
            );
        }

        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller", "creator");
            loads.get(0).onResponse(List.of(descriptor));
        }
        assertThat(joinerFuture.get(1, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
        assertThat(headerSeenByJoiner.get(), equalTo("joiner"));
    }

    /**
     * Sharing a load must not copy the creating caller's response headers into the joining caller's response.
     */
    public void testJoinedLoadDoesNotInheritCreatorResponseHeaders() throws Exception {
        final ThreadContext threadContext = threadPool.getThreadContext();
        final DescriptorsAndApplicationNamesCache cache = new DescriptorsAndApplicationNamesCache(
            TimeValue.timeValueMinutes(10),
            100,
            threadPool.generic(),
            threadContext
        );

        final List<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loads = new ArrayList<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> creatorFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.addResponseHeader("X-Test-Response", "creator");
            cache.get(Set.of("myapp"), true, loads::add, creatorFuture);
        }
        assertThat(loads, hasSize(1));

        final AtomicReference<Map<String, List<String>>> headersSeenByJoiner = new AtomicReference<>();
        final PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> joinerFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.addResponseHeader("X-Test-Response", "joiner");
            cache.get(
                Set.of("myapp"),
                true,
                l -> fail("should have joined the in-flight load"),
                ActionListener.runBefore(joinerFuture, () -> headersSeenByJoiner.set(threadContext.getResponseHeaders()))
            );
        }

        final ApplicationPrivilegeDescriptor descriptor = myappAdminPrivilege();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.addResponseHeader("X-Test-Response", "creator");
            loads.get(0).onResponse(List.of(descriptor));
        }
        assertThat(creatorFuture.get(10, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
        assertThat(joinerFuture.get(10, TimeUnit.SECONDS), containsInAnyOrder(descriptor));
        assertThat(headersSeenByJoiner.get(), equalTo(Map.of("X-Test-Response", List.of("joiner"))));
    }

    private static DescriptorsAndApplicationNamesCache newPrivilegeCache() {
        return new DescriptorsAndApplicationNamesCache(
            TimeValue.timeValueMinutes(10),
            100,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new ThreadContext(Settings.EMPTY)
        );
    }

    private static ApplicationPrivilegeDescriptor myappAdminPrivilege() {
        return new ApplicationPrivilegeDescriptor(
            "myapp",
            "admin",
            newHashSet("action:admin/*", "action:login", "data:read/*"),
            emptyMap()
        );
    }

    private static ApplicationPrivilegeDescriptor myappPrivilege(String privilegeName) {
        return new ApplicationPrivilegeDescriptor("myapp", privilegeName, newHashSet("action:" + privilegeName), emptyMap());
    }

    public void testPutPrivileges() throws Exception {
        final List<ApplicationPrivilegeDescriptor> putPrivileges = Arrays.asList(
            new ApplicationPrivilegeDescriptor("app1", "admin", newHashSet("action:admin/*", "action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app1", "user", newHashSet("action:login", "data:read/*"), emptyMap()),
            new ApplicationPrivilegeDescriptor("app2", "all", newHashSet("*"), emptyMap())
        );

        final PlainActionFuture<Map<String, Map<String, DocWriteResponse.Result>>> putPrivilegeFuture = new PlainActionFuture<>();
        store.putPrivileges(putPrivileges, WriteRequest.RefreshPolicy.IMMEDIATE, putPrivilegeFuture);
        assertThat(requests, iterableWithSize(1));
        assertThat(requests, everyItem(instanceOf(BulkRequest.class)));

        final BulkRequest bulkRequest = (BulkRequest) requests.get(0);
        requests.clear();

        assertThat(bulkRequest.requests(), iterableWithSize(putPrivileges.size()));
        assertThat(bulkRequest.requests(), everyItem(instanceOf(IndexRequest.class)));

        final List<IndexRequest> indexRequests = new ArrayList<>(putPrivileges.size());
        bulkRequest.requests().stream().map(IndexRequest.class::cast).forEach(indexRequests::add);

        final String uuid = UUIDs.randomBase64UUID(random());
        final BulkItemResponse[] responses = new BulkItemResponse[putPrivileges.size()];
        for (int i = 0; i < putPrivileges.size(); i++) {
            ApplicationPrivilegeDescriptor privilege = putPrivileges.get(i);
            IndexRequest request = indexRequests.get(i);
            assertThat(request.indices(), arrayContaining(SecuritySystemIndices.SECURITY_MAIN_ALIAS));
            assertThat(request.id(), equalTo("application-privilege_" + privilege.getApplication() + ":" + privilege.getName()));
            final XContentBuilder builder = privilege.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), true);
            assertThat(request.source(), equalBytes(BytesReference.bytes(builder)));
            final boolean created = privilege.getName().equals("user") == false;
            responses[i] = BulkItemResponse.success(
                i,
                DocWriteRequest.OpType.INDEX,
                new IndexResponse(new ShardId(SecuritySystemIndices.SECURITY_MAIN_ALIAS, uuid, i), request.id(), 1, 1, 1, created)
            );
        }

        listener.get().onResponse(new BulkResponse(responses, randomLongBetween(1, 1_000)));

        assertBusy(() -> assertFalse(requests.isEmpty()), 1, TimeUnit.SECONDS);

        assertThat(requests, iterableWithSize(1));
        assertThat(requests.get(0), instanceOf(ClearPrivilegesCacheRequest.class));
        listener.get().onResponse(null);

        final Map<String, Map<String, DocWriteResponse.Result>> map = putPrivilegeFuture.actionGet();
        assertThat(map.entrySet(), iterableWithSize(2));
        assertThat(map.get("app1"), aMapWithSize(2));
        assertThat(map.get("app2"), aMapWithSize(1));
        assertThat(map.get("app1"), hasEntry("admin", DocWriteResponse.Result.CREATED));
        assertThat(map.get("app1"), hasEntry("user", DocWriteResponse.Result.UPDATED));
        assertThat(map.get("app2"), hasEntry("all", DocWriteResponse.Result.CREATED));
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
            // The constructor reads the thread pool from the client when the cache is enabled; this is the only
            // interaction the client may see, which the verification below checks
            when(mockClient.threadPool()).thenReturn(threadPool);
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
            verify(mockClient, atMost(1)).threadPool();
            verifyNoMoreInteractions(mockClient);
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
            Metadata.DEFAULT_PROJECT_ID,
            dummyState(securityIndexName, true, randomFrom((ClusterHealthStatus) null, ClusterHealthStatus.RED)),
            dummyState(securityIndexName, true, randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW))
        );
        assertEquals(++count, store.getNumInvalidation());

        // Cache should be cleared when security is deleted
        cacheInvalidatorRegistry.onSecurityIndexStateChange(
            Metadata.DEFAULT_PROJECT_ID,
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
            Metadata.DEFAULT_PROJECT_ID,
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
        ActionListener.respondAndRelease(listener.get(), buildSearchResponse(hits));

        assertResult(sourcePrivileges, future);
    }

    private SecurityIndexManager.IndexState dummyState(
        String concreteSecurityIndexName,
        boolean isIndexUpToDate,
        ClusterHealthStatus healthStatus
    ) {

        return securityIndex.new IndexState(
            Metadata.DEFAULT_PROJECT_ID, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), isIndexUpToDate, true, true,
            true, true, null, false, null, null, null, concreteSecurityIndexName, healthStatus, IndexMetadata.State.OPEN, "my_uuid", Set
                .of()
        );
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
            return SearchResponseUtils.successfulResponse(searchHits);
        } finally {
            searchHits.decRef();
        }
    }

    private void handleBulkRequest(int expectedCount, Predicate<DocWriteRequest<?>> isCreated) {
        final BulkRequest bulkReq = getLastRequest(BulkRequest.class);
        assertThat(bulkReq.requests(), hasSize(expectedCount));

        final var uuid = UUIDs.randomBase64UUID(random());
        final var items = new BulkItemResponse[expectedCount];
        for (int i = 0; i < expectedCount; i++) {
            final DocWriteRequest<?> itemReq = bulkReq.requests().get(i);
            items[i] = BulkItemResponse.success(
                i,
                itemReq.opType(),
                new IndexResponse(
                    new ShardId(SecuritySystemIndices.SECURITY_MAIN_ALIAS, uuid, 0),
                    itemReq.id(),
                    1,
                    1,
                    1,
                    isCreated.test(itemReq)
                )
            );
        }
        listener.get().onResponse(new BulkResponse(items, randomIntBetween(1, 999)));
    }

    private void assertResult(
        List<ApplicationPrivilegeDescriptor> sourcePrivileges,
        PlainActionFuture<Collection<ApplicationPrivilegeDescriptor>> future
    ) throws Exception {
        final Collection<ApplicationPrivilegeDescriptor> getPrivileges = future.get(1, TimeUnit.SECONDS);
        assertThat(getPrivileges, iterableWithSize(sourcePrivileges.size()));
        assertThat(new HashSet<>(getPrivileges), equalTo(new HashSet<>(sourcePrivileges)));
    }

    private void assertCacheCleared(String... applicationNames) {
        final ClearPrivilegesCacheRequest clearCacheReq = getLastRequest(ClearPrivilegesCacheRequest.class);
        assertThat(clearCacheReq.applicationNames(), arrayContainingInAnyOrder(applicationNames));
        assertThat(clearCacheReq.clearRolesCache(), is(true));
        listener.get().onResponse(new ClearPrivilegesCacheResponse(clusterService.getClusterName(), List.of(), List.of()));
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }

    private Settings setAllowExpensiveQueries(Settings settings) {
        return Settings.builder().put(settings).put(ALLOW_EXPENSIVE_QUERIES.getKey(), allowExpensiveQueries).build();
    }

    private static UnavailableShardsException unavailableShardsException() {
        return new UnavailableShardsException("index", 1, "bad shard");
    }
}
