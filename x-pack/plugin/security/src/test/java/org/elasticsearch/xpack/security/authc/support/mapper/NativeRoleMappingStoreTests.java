/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeRoleMappingStoreTests extends ESTestCase {
    private final String concreteSecurityIndexName = randomFrom(
        TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
        TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
    );

    private ScriptService scriptService;
    private SecurityIndexManager securityIndex;

    @Before
    public void setup() {
        scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine(Settings.EMPTY)),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        securityIndex = mockHealthySecurityIndex();
    }

    public void testResolveRoles() throws Exception {
        // Does match DN
        final ExpressionRoleMapping mapping1 = new ExpressionRoleMapping(
            "dept_h",
            new FieldExpression("dn", Collections.singletonList(new FieldValue("*,ou=dept_h,o=forces,dc=gc,dc=ca"))),
            Arrays.asList("dept_h", "defence"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        // Does not match - user is not in this group
        final ExpressionRoleMapping mapping2 = new ExpressionRoleMapping(
            "admin",
            new FieldExpression(
                "groups",
                Collections.singletonList(new FieldValue(randomiseDn("cn=esadmin,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))
            ),
            Arrays.asList("admin"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        // Does match - user is one of these groups
        final ExpressionRoleMapping mapping3 = new ExpressionRoleMapping(
            "flight",
            new FieldExpression(
                "groups",
                Arrays.asList(
                    new FieldValue(randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                    new FieldValue(randomiseDn("cn=betaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                    new FieldValue(randomiseDn("cn=gammaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"))
                )
            ),
            Collections.emptyList(),
            Arrays.asList(
                new TemplateRoleName(new BytesArray("{ \"source\":\"{{metadata.extra_group}}\" }"), TemplateRoleName.Format.STRING)
            ),
            Collections.emptyMap(),
            true
        );
        // Does not match - mapping is not enabled
        final ExpressionRoleMapping mapping4 = new ExpressionRoleMapping(
            "mutants",
            new FieldExpression(
                "groups",
                Collections.singletonList(new FieldValue(randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))
            ),
            Arrays.asList("mutants"),
            Collections.emptyList(),
            Collections.emptyMap(),
            false
        );

        final Client client = mock(Client.class);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, client, securityIndex, scriptService) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                final List<ExpressionRoleMapping> mappings = Arrays.asList(mapping1, mapping2, mapping3, mapping4);
                logger.info("Role mappings are: [{}]", mappings);
                listener.onResponse(mappings);
            }
        };

        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ldap1");
        final Settings settings = Settings.builder()
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final RealmConfig realm = new RealmConfig(realmIdentifier, settings, mock(Environment.class), new ThreadContext(settings));

        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        final UserRoleMapper.UserData user = new UserRoleMapper.UserData(
            "sasquatch",
            randomiseDn("cn=walter.langowski,ou=people,ou=dept_h,o=forces,dc=gc,dc=ca"),
            List.of(
                randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"),
                randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")
            ),
            Map.of("extra_group", "flight"),
            realm
        );

        logger.info("UserData is [{}]", user);
        store.resolveRoles(user, future);
        final Set<String> roles = future.get();
        assertThat(roles, Matchers.containsInAnyOrder("dept_h", "defence", "flight"));
        assertThat(store.getLastLoad(), is(nullValue()));
    }

    public void testResolveRolesDoesNotUseLastLoadCacheWhenSecurityIndexAvailable() throws Exception {
        final Client client = mock(Client.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(Mockito.spy(new SearchRequestBuilder(client)));
        final ExpressionRoleMapping mapping = new ExpressionRoleMapping(
            "mapping",
            new FieldExpression("dn", Collections.singletonList(new FieldValue("*"))),
            List.of("role"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        doAnswerWithSearchResult(client, mapping);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(
            Settings.builder().put("xpack.security.authz.store.role_mappings.last_load_cache.enabled", "true").build(),
            client,
            securityIndex,
            scriptService
        );

        final UserRoleMapper.UserData user = new UserRoleMapper.UserData(
            "user",
            randomiseDn("cn=user,ou=people,dc=org"),
            List.of(),
            Map.of(),
            mock(RealmConfig.class)
        );
        assertThat(store.getLastLoad(), is(nullValue()));

        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder("role"));
        assertThat(store.getLastLoad(), contains(mapping));
        verify(client, times(1)).search(any(SearchRequest.class), anyActionListener());

        // when security index is available, we still run a search
        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder("role"));
        assertThat(store.getLastLoad(), contains(mapping));
        verify(client, times(2)).search(any(SearchRequest.class), anyActionListener());
    }

    public void testResolveRolesUsesLastLoadCacheWhenSecurityIndexUnavailable() throws Exception {
        final Client client = mock(Client.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(Mockito.spy(new SearchRequestBuilder(client)));
        final ExpressionRoleMapping mapping = new ExpressionRoleMapping(
            "mapping",
            new FieldExpression("dn", Collections.singletonList(new FieldValue("*"))),
            List.of("role"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        doAnswerWithSearchResult(client, mapping);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(
            Settings.builder().put("xpack.security.authz.store.role_mappings.last_load_cache.enabled", "true").build(),
            client,
            securityIndex,
            scriptService
        );

        final UserRoleMapper.UserData user = new UserRoleMapper.UserData(
            "user",
            randomiseDn("cn=user,ou=people,dc=org"),
            List.of(),
            Map.of(),
            mock(RealmConfig.class)
        );
        assertThat(store.getLastLoad(), is(nullValue()));

        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder("role"));
        assertThat(store.getLastLoad(), contains(mapping));
        verify(client, times(1)).search(any(SearchRequest.class), anyActionListener());

        SecurityIndexManager.IndexState projectIndex = securityIndex.forCurrentProject();
        final boolean indexAvailable = randomBoolean();
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(indexAvailable);
        final boolean indexClosed = indexAvailable || randomBoolean();
        when(projectIndex.indexIsClosed()).thenReturn(indexClosed);
        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder(mapping.getRoles().toArray()));
        assertThat(store.getLastLoad(), contains(mapping));
        // index was unavailable, so we returned result from cache; no new search
        verify(client, times(1)).search(any(SearchRequest.class), anyActionListener());

        // new search result from index overwrites previous
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexIsClosed()).thenReturn(false);
        final ExpressionRoleMapping mapping2 = new ExpressionRoleMapping(
            "mapping2",
            new FieldExpression("dn", Collections.singletonList(new FieldValue("*"))),
            List.of("role2"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        doAnswerWithSearchResult(client, mapping2);
        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder(mapping2.getRoles().toArray()));
        assertThat(store.getLastLoad(), contains(mapping2));
    }

    public void testResolveRolesDoesNotUseLastLoadCacheWhenSecurityIndexDoesNotExist() throws Exception {
        final Client client = mock(Client.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(client.prepareSearch(eq(SECURITY_MAIN_ALIAS))).thenReturn(Mockito.spy(new SearchRequestBuilder(client)));
        final ExpressionRoleMapping mapping = new ExpressionRoleMapping(
            "mapping",
            new FieldExpression("dn", Collections.singletonList(new FieldValue("*"))),
            List.of("role"),
            Collections.emptyList(),
            Collections.emptyMap(),
            true
        );
        doAnswerWithSearchResult(client, mapping);

        final NativeRoleMappingStore store = new NativeRoleMappingStore(
            Settings.builder().put("xpack.security.authz.store.role_mappings.last_load_cache.enabled", "true").build(),
            client,
            securityIndex,
            scriptService
        );

        final UserRoleMapper.UserData user = new UserRoleMapper.UserData(
            "user",
            randomiseDn("cn=user,ou=people,dc=org"),
            List.of(),
            Map.of(),
            mock(RealmConfig.class)
        );
        assertThat(store.getLastLoad(), is(nullValue()));

        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder("role"));
        assertThat(store.getLastLoad(), contains(mapping));
        verify(client, times(1)).search(any(SearchRequest.class), anyActionListener());

        SecurityIndexManager.IndexState projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.indexExists()).thenReturn(false);
        assertThat(resolveRoles(store, user), is(empty()));
        assertThat(store.getLastLoad(), contains(mapping));
    }

    private SecurityIndexManager mockHealthySecurityIndex() {
        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        SecurityIndexManager.IndexState projectIndex = Mockito.mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.isIndexUpToDate()).thenReturn(true);
        return securityIndex;
    }

    private void doAnswerWithSearchResult(Client client, ExpressionRoleMapping mapping) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            final var searchHit = SearchHit.unpooled(
                randomIntBetween(0, Integer.MAX_VALUE),
                NativeRoleMappingStore.getIdForName(mapping.getName())
            );
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                mapping.toXContent(builder, ToXContent.EMPTY_PARAMS);
                searchHit.sourceRef(BytesReference.bytes(builder));
            }
            ActionListener.respondAndRelease(
                listener,
                SearchResponseUtils.successfulResponse(
                    SearchHits.unpooled(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), randomFloat())
                )
            );
            return null;
        }).when(client).search(any(SearchRequest.class), anyActionListener());
    }

    private Set<String> resolveRoles(NativeRoleMappingStore store, UserRoleMapper.UserData user) throws InterruptedException,
        ExecutionException {
        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        store.resolveRoles(user, future);
        return future.get();
    }

    private String randomiseDn(String dn) {
        // Randomly transform the dn into another valid form that is logically identical,
        // but (potentially) textually different
        return switch (randomIntBetween(0, 3)) {
            case 0 ->
                // do nothing
                dn;
            case 1 -> dn.toUpperCase(Locale.ROOT);
            case 2 ->
                // Upper case just the attribute name for each RDN
                Arrays.stream(dn.split(",")).map(s -> {
                    final String[] arr = s.split("=");
                    arr[0] = arr[0].toUpperCase(Locale.ROOT);
                    return String.join("=", arr);
                }).collect(Collectors.joining(","));
            case 3 -> dn.replaceAll(",", ", ");
            default -> dn;
        };
    }

    private SecurityIndexManager.IndexState dummyState(ClusterHealthStatus indexStatus) {
        return indexState(true, indexStatus);
    }

    private SecurityIndexManager.IndexState indexState(boolean isUpToDate, ClusterHealthStatus healthStatus) {
        return this.securityIndex.new IndexState(
            Metadata.DEFAULT_PROJECT_ID, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), isUpToDate, true, true, true,
            true, null, null, null, null, concreteSecurityIndexName, healthStatus, IndexMetadata.State.OPEN, "my_uuid", Set.of()
        );
    }

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numGlobalInvalidation = new AtomicInteger(0);
        final AtomicInteger numLocalInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(numGlobalInvalidation, numLocalInvalidation, true);

        int expectedInvalidation = 0;
        // existing to no longer present
        SecurityIndexManager.IndexState previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.IndexState currentState = dummyState(null);
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numLocalInvalidation.get());
        assertEquals(0, numGlobalInvalidation.get());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numLocalInvalidation.get());
        assertEquals(0, numGlobalInvalidation.get());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, numLocalInvalidation.get());
        assertEquals(0, numGlobalInvalidation.get());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numLocalInvalidation.get());
        assertEquals(0, numGlobalInvalidation.get());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(
            previousState.indexHealth == ClusterHealthStatus.GREEN ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN
        );
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, numLocalInvalidation.get());
        assertEquals(0, numGlobalInvalidation.get());
    }

    public void testCacheClearOnIndexOutOfDateChange() {
        final AtomicInteger numGlobalInvalidation = new AtomicInteger(0);
        final AtomicInteger numLocalInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(numGlobalInvalidation, numLocalInvalidation, true);

        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, indexState(false, null), indexState(true, null));
        assertEquals(0, numGlobalInvalidation.get());
        assertEquals(1, numLocalInvalidation.get());

        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, indexState(true, null), indexState(false, null));
        assertEquals(0, numGlobalInvalidation.get());
        assertEquals(2, numLocalInvalidation.get());
    }

    public void testCacheIsNotClearedIfNoRealmsAreAttached() {
        final AtomicInteger numGlobalInvalidation = new AtomicInteger(0);
        final AtomicInteger numLocalInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(
            numGlobalInvalidation,
            numLocalInvalidation,
            false
        );

        final SecurityIndexManager.IndexState noIndexState = dummyState(null);
        final SecurityIndexManager.IndexState greenIndexState = dummyState(ClusterHealthStatus.GREEN);
        store.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, noIndexState, greenIndexState);
        assertEquals(0, numGlobalInvalidation.get());
        assertEquals(0, numLocalInvalidation.get());
    }

    public void testPutRoleMappingWillValidateTemplateRoleNamesBeforeSave() {
        final PutRoleMappingRequest putRoleMappingRequest = mock(PutRoleMappingRequest.class);
        final TemplateRoleName templateRoleName = mock(TemplateRoleName.class);
        final ScriptService scriptService = mock(ScriptService.class);
        when(putRoleMappingRequest.getRoleTemplates()).thenReturn(Collections.singletonList(templateRoleName));
        doAnswer(invocationOnMock -> { throw new IllegalArgumentException(); }).when(templateRoleName).validate(scriptService);

        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(
            Settings.EMPTY,
            mock(Client.class),
            mock(SecurityIndexManager.class),
            scriptService
        );
        expectThrows(IllegalArgumentException.class, () -> nativeRoleMappingStore.putRoleMapping(putRoleMappingRequest, null));
    }

    private NativeRoleMappingStore buildRoleMappingStoreForInvalidationTesting(
        AtomicInteger globalInvalidationCounter,
        AtomicInteger localInvalidationCounter,
        boolean attachRealm
    ) {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();

        final ThreadPool threadPool = mock(ThreadPool.class);
        final ThreadContext threadContext = new ThreadContext(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        final String realmName = randomAlphaOfLengthBetween(4, 8);

        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        doAnswer(invocationOnMock -> {
            assertThat(invocationOnMock.getArguments(), Matchers.arrayWithSize(3));
            final ClearRealmCacheRequest request = (ClearRealmCacheRequest) invocationOnMock.getArguments()[1];
            assertThat(request.realms(), Matchers.arrayContaining(realmName));

            @SuppressWarnings("unchecked")
            ActionListener<ClearRealmCacheResponse> listener = (ActionListener<ClearRealmCacheResponse>) invocationOnMock.getArguments()[2];
            globalInvalidationCounter.incrementAndGet();
            listener.onResponse(new ClearRealmCacheResponse(new ClusterName("cluster"), Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(ClearRealmCacheAction.INSTANCE), any(ClearRealmCacheRequest.class), anyActionListener());

        final NativeRoleMappingStore store = new NativeRoleMappingStore(
            Settings.EMPTY,
            client,
            mock(SecurityIndexManager.class),
            mock(ScriptService.class)
        );

        if (attachRealm) {
            CachingRealm mockRealm = mock(CachingRealm.class);
            when(mockRealm.name()).thenReturn("mockRealm");
            doAnswer(inv -> {
                localInvalidationCounter.incrementAndGet();
                return null;
            }).when(mockRealm).expireAll();
            store.clearRealmCacheOnChange(mockRealm);
        }
        return store;
    }
}
