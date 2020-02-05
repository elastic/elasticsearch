/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeRoleMappingStoreTests extends ESTestCase {
    private final String concreteSecurityIndexName = randomFrom(
        RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6, RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);

    public void testResolveRoles() throws Exception {
        // Does match DN
        final ExpressionRoleMapping mapping1 = new ExpressionRoleMapping("dept_h",
                new FieldExpression("dn", Collections.singletonList(new FieldValue("*,ou=dept_h,o=forces,dc=gc,dc=ca"))),
                Arrays.asList("dept_h", "defence"), Collections.emptyList(), Collections.emptyMap(), true);
        // Does not match - user is not in this group
        final ExpressionRoleMapping mapping2 = new ExpressionRoleMapping("admin",
            new FieldExpression("groups", Collections.singletonList(
                new FieldValue(randomiseDn("cn=esadmin,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))),
            Arrays.asList("admin"), Collections.emptyList(), Collections.emptyMap(), true);
        // Does match - user is one of these groups
        final ExpressionRoleMapping mapping3 = new ExpressionRoleMapping("flight",
                new FieldExpression("groups", Arrays.asList(
                        new FieldValue(randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                        new FieldValue(randomiseDn("cn=betaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")),
                        new FieldValue(randomiseDn("cn=gammaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"))
                )),
            Collections.emptyList(),
            Arrays.asList(new TemplateRoleName(new BytesArray("{ \"source\":\"{{metadata.extra_group}}\" }"),
                TemplateRoleName.Format.STRING)),
            Collections.emptyMap(), true);
        // Does not match - mapping is not enabled
        final ExpressionRoleMapping mapping4 = new ExpressionRoleMapping("mutants",
                new FieldExpression("groups", Collections.singletonList(
                        new FieldValue(randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")))),
            Arrays.asList("mutants"), Collections.emptyList(), Collections.emptyMap(), false);

        final Client client = mock(Client.class);
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        ScriptService scriptService  = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        when(securityIndex.isAvailable()).thenReturn(true);

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
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build();
        final RealmConfig realm = new RealmConfig(realmIdentifier, settings,
                mock(Environment.class), new ThreadContext(settings));

        final PlainActionFuture<Set<String>> future = new PlainActionFuture<>();
        final UserRoleMapper.UserData user = new UserRoleMapper.UserData("sasquatch",
                randomiseDn("cn=walter.langowski,ou=people,ou=dept_h,o=forces,dc=gc,dc=ca"),
                List.of(
                        randomiseDn("cn=alphaflight,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca"),
                        randomiseDn("cn=mutants,ou=groups,ou=dept_h,o=forces,dc=gc,dc=ca")
                ), Map.of("extra_group", "flight"), realm);

        logger.info("UserData is [{}]", user);
        store.resolveRoles(user, future);
        final Set<String> roles = future.get();
        assertThat(roles, Matchers.containsInAnyOrder("dept_h", "defence", "flight"));
    }

    private String randomiseDn(String dn) {
        // Randomly transform the dn into another valid form that is logically identical,
        // but (potentially) textually different
        switch (randomIntBetween(0, 3)) {
            case 0:
                // do nothing
                return dn;
            case 1:
                return dn.toUpperCase(Locale.ROOT);
            case 2:
                // Upper case just the attribute name for each RDN
                return Arrays.stream(dn.split(",")).map(s -> {
                    final String[] arr = s.split("=");
                    arr[0] = arr[0].toUpperCase(Locale.ROOT);
                    return String.join("=", arr);
                }).collect(Collectors.joining(","));
            case 3:
                return dn.replaceAll(",", ", ");
        }
        return dn;
    }

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return indexState(true, indexStatus);
    }

    private SecurityIndexManager.State indexState(boolean isUpToDate, ClusterHealthStatus healthStatus) {
        return new SecurityIndexManager.State(
            Instant.now(), isUpToDate, true, true, null, concreteSecurityIndexName, healthStatus, IndexMetaData.State.OPEN);
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
        currentState = dummyState(previousState.indexHealth == ClusterHealthStatus.GREEN ?
            ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN);
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

            ActionListener<ClearRealmCacheResponse> listener = (ActionListener<ClearRealmCacheResponse>) invocationOnMock.getArguments()[2];
            invalidationCounter.incrementAndGet();
            listener.onResponse(new ClearRealmCacheResponse(new ClusterName("cluster"), Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(ClearRealmCacheAction.INSTANCE), any(ClearRealmCacheRequest.class), any(ActionListener.class));

        final NativeRoleMappingStore store = new NativeRoleMappingStore(Settings.EMPTY, client, mock(SecurityIndexManager.class),
            mock(ScriptService.class));

        if (attachRealm) {
            final Environment env = TestEnvironment.newEnvironment(settings);
            final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier("ldap", realmName);
            final RealmConfig realmConfig = new RealmConfig(identifier,
                Settings.builder().put(settings)
                    .put(RealmSettings.getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0).build(),
                env, threadContext);
            final CachingUsernamePasswordRealm mockRealm = new CachingUsernamePasswordRealm(realmConfig, threadPool) {
                @Override
                protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener) {
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
