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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
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
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
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

        final boolean indexAvailable = randomBoolean();
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(indexAvailable);
        final boolean indexClosed = indexAvailable || randomBoolean();
        when(securityIndex.indexIsClosed()).thenReturn(indexClosed);
        assertThat(resolveRoles(store, user), Matchers.containsInAnyOrder(mapping.getRoles().toArray()));
        assertThat(store.getLastLoad(), contains(mapping));
        // index was unavailable, so we returned result from cache; no new search
        verify(client, times(1)).search(any(SearchRequest.class), anyActionListener());

        // new search result from index overwrites previous
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(securityIndex.indexIsClosed()).thenReturn(false);
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

        when(securityIndex.indexExists()).thenReturn(false);
        assertThat(resolveRoles(store, user), is(empty()));
        assertThat(store.getLastLoad(), contains(mapping));
    }

    private SecurityIndexManager mockHealthySecurityIndex() {
        final SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(securityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isIndexUpToDate()).thenReturn(true);
        when(securityIndex.defensiveCopy()).thenReturn(securityIndex);
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
                new SearchResponse(
                    SearchHits.unpooled(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), randomFloat()),
                    null,
                    null,
                    false,
                    null,
                    null,
                    0,
                    randomAlphaOfLengthBetween(3, 8),
                    1,
                    1,
                    0,
                    10,
                    null,
                    null
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

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return indexState(true, indexStatus);
    }

    private SecurityIndexManager.State indexState(boolean isUpToDate, ClusterHealthStatus healthStatus) {
        return new SecurityIndexManager.State(
            Instant.now(),
            isUpToDate,
            true,
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

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(numInvalidation, true);

        int expectedInvalidation = 0;
        // existing to no longer present
        SecurityIndexManager.State previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.State currentState = dummyState(null);
        store.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        store.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        store.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        store.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(
            previousState.indexHealth == ClusterHealthStatus.GREEN ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN
        );
        store.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());
    }

    public void testCacheClearOnIndexOutOfDateChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(numInvalidation, true);

        store.onSecurityIndexStateChange(indexState(false, null), indexState(true, null));
        assertEquals(1, numInvalidation.get());

        store.onSecurityIndexStateChange(indexState(true, null), indexState(false, null));
        assertEquals(2, numInvalidation.get());
    }

    public void testCacheIsNotClearedIfNoRealmsAreAttached() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);
        final NativeRoleMappingStore store = buildRoleMappingStoreForInvalidationTesting(numInvalidation, false);

        final SecurityIndexManager.State noIndexState = dummyState(null);
        final SecurityIndexManager.State greenIndexState = dummyState(ClusterHealthStatus.GREEN);
        store.onSecurityIndexStateChange(noIndexState, greenIndexState);
        assertEquals(0, numInvalidation.get());
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

    private NativeRoleMappingStore buildRoleMappingStoreForInvalidationTesting(AtomicInteger invalidationCounter, boolean attachRealm) {
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
            invalidationCounter.incrementAndGet();
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
            final Environment env = TestEnvironment.newEnvironment(settings);
            final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("ldap", realmName);
            final RealmConfig realmConfig = new RealmConfig(
                identifier,
                Settings.builder().put(settings).put(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0).build(),
                env,
                threadContext
            );
            final CachingUsernamePasswordRealm mockRealm = new CachingUsernamePasswordRealm(realmConfig, threadPool) {
                @Override
                protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
                    listener.onResponse(AuthenticationResult.notHandled());
                }

                @Override
                protected void doLookupUser(String username, ActionListener<User> listener) {
                    listener.onResponse(null);
                }
            };
            store.refreshRealmOnChange(mockRealm);
        }
        return store;
    }
}
