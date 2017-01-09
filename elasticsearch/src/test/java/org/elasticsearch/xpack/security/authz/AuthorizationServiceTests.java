/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.UserRequest;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.ldap.LdapRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAs;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class AuthorizationServiceTests extends ESTestCase {
    private AuditTrailService auditTrail;
    private ClusterService clusterService;
    private AuthorizationService authorizationService;
    private ThreadContext threadContext;
    private ThreadPool threadPool;
    private Map<String, RoleDescriptor> roleMap = new HashMap<>();
    private CompositeRolesStore rolesStore;

    @Before
    public void setup() {
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        auditTrail = mock(AuditTrailService.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        doAnswer((i) -> {
            ActionListener<Role> callback =
                    (ActionListener<Role>) i.getArguments()[2];
            Set<String> names = (Set<String>) i.getArguments()[0];
            assertNotNull(names);
            Set<RoleDescriptor> roleDescriptors = new HashSet<>();
            for (String name : names) {
                RoleDescriptor descriptor = roleMap.get(name);
                if (descriptor != null) {
                    roleDescriptors.add(descriptor);
                }
            }

            if (roleDescriptors.isEmpty()) {
                callback.onResponse(Role.EMPTY);
            } else {
                callback.onResponse(
                        CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache));
            }
            return Void.TYPE;
        }).when(rolesStore).roles(any(Set.class), any(FieldPermissionsCache.class), any(ActionListener.class));
        authorizationService = new AuthorizationService(Settings.EMPTY, rolesStore, clusterService,
                auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(Settings.EMPTY));
    }

    private void authorize(Authentication authentication, String action, TransportRequest request) {
        PlainActionFuture future = new PlainActionFuture();
        AuthorizationUtils.AsyncAuthorizer authorizer = new AuthorizationUtils.AsyncAuthorizer(authentication, future,
                (userRoles, runAsRoles) -> {authorizationService.authorize(authentication, action, request, userRoles, runAsRoles);
            future.onResponse(null);
        });
        authorizer.authorize(authorizationService);
        future.actionGet();
    }

    public void testActionsSystemUserIsAuthorized() {
        TransportRequest request = mock(TransportRequest.class);

        // A failure would throw an exception
        authorize(createAuthentication(SystemUser.INSTANCE), "indices:monitor/whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "indices:monitor/whatever", request);

        authorize(createAuthentication(SystemUser.INSTANCE), "internal:whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "internal:whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(SystemUser.INSTANCE), "indices:", request),
                "indices:", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "indices:", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/whatever", request),
                "cluster:admin/whatever", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/snapshot/status", request),
                "cluster:admin/snapshot/status", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/snapshot/status", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNoRolesCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user", "non-existent-role");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), "whatever", request),
                "whatever", "test user");
        verify(auditTrail).accessDenied(user, "whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatRoleWithNoIndicesIsDenied() {
        TransportRequest request = new IndicesExistsRequest("a");
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("a_role",null,null,null));
        mockEmptyMetaData();

        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testSearchAgainstEmptyCluster() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();

        {
            //ignore_unavailable set to false, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                    .indicesOptions(IndicesOptions.fromOptions(false, true, true, false));

            assertThrowsAuthorizationException(
                    () -> authorize(createAuthentication(user), SearchAction.NAME, searchRequest),
                    SearchAction.NAME, "test user");
            verify(auditTrail).accessDenied(user, SearchAction.NAME, searchRequest);
            verifyNoMoreInteractions(auditTrail);
        }

        {
            //ignore_unavailable and allow_no_indices both set to true, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                    .indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
            verify(auditTrail).accessGranted(user, SearchAction.NAME, searchRequest);
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_KEY);
            IndicesAccessControl.IndexAccessControl indexAccessControl =
                    indicesAccessControl.getIndexPermissions(IndicesAndAliasesResolver.NO_INDEX_PLACEHOLDER);
            assertFalse(indexAccessControl.getFieldPermissions().hasFieldLevelSecurity());
            assertNull(indexAccessControl.getQueries());
        }
    }

    public void testScrollRelatedRequestsAllowed() {
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        authorize(createAuthentication(user), ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(user, ClearScrollAction.NAME, clearScrollRequest);

        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        authorize(createAuthentication(user), SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(user, SearchScrollAction.NAME, searchScrollRequest);

        // We have to use a mock request for other Scroll actions as the actual requests are package private to SearchTransportService
        TransportRequest request = mock(TransportRequest.class);
        authorize(createAuthentication(user), SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);

        authorize(createAuthentication(user), SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);

        authorize(createAuthentication(user), SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);

        authorize(createAuthentication(user), SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);

        authorize(createAuthentication(user), SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAliasWithoutPermissions() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_role",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), CreateIndexAction.NAME, request),
                IndicesAliasesAction.NAME, "test user");
        verify(auditTrail).accessDenied(user, IndicesAliasesAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAlias() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_all",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a", "a2").privileges("all").build() },null));

        authorize(createAuthentication(user), CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(user, CreateIndexAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testDenialForAnonymousUser() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "a_all").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);

        roleMap.put("a_all", new RoleDescriptor("a_all",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(anonymousUser), "indices:a", request),
                "indices:a", anonymousUser.principal());
        verify(auditTrail).accessDenied(anonymousUser, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        Settings settings = Settings.builder()
                .put(AnonymousUser.ROLES_SETTING.getKey(), "a_all")
                .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false)
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(settings));

        roleMap.put("a_all", new RoleDescriptor("a_all",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> authorize(createAuthentication(anonymousUser), "indices:a", request));
        assertAuthenticationException(securityException, containsString("action [indices:a] requires authentication"));
        verify(auditTrail).accessDenied(anonymousUser, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsError() {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, true);
        TransportRequest request = new GetIndexRequest().indices("not-an-index-*").indicesOptions(options);
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        roleMap.put("a_all", new RoleDescriptor("a_all",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },null));

        final IndexNotFoundException nfe = expectThrows(
                IndexNotFoundException.class,
                () -> authorize(createAuthentication(user), GetIndexAction.NAME, request));
        assertThat(nfe.getIndex(), is(notNullValue()));
        assertThat(nfe.getIndex().getName(), is("not-an-index-*"));
        verify(auditTrail).accessDenied(user, GetIndexAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testRunAsRequestWithNoRolesUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", null, new User("run as me", new String[] { "admin" }));
        assertThat(user.runAs(), is(notNullValue()));
        assertThrowsAuthorizationExceptionRunAs(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me"); // run as [run as me]
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithoutLookedUpBy() {
        AuthenticateRequest request = new AuthenticateRequest("run as me");
        roleMap.put("can run as", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", Strings.EMPTY_ARRAY));
        Authentication authentication = new Authentication(user, new RealmRef("foo", "bar", "baz"), null);
        assertThat(user.runAs(), is(notNullValue()));
        assertThrowsAuthorizationExceptionRunAs(
                () -> authorize(authentication, AuthenticateAction.NAME, request),
                AuthenticateAction.NAME, "test user", "run as me"); // run as [run as me]
        verify(auditTrail).runAsDenied(user, AuthenticateAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "doesn't exist"));
        assertThat(user.runAs(), is(notNullValue()));
        roleMap.put("can run as", new RoleDescriptor("can run as",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
                new String[] { "not the right user" }));

        assertThrowsAuthorizationExceptionRunAs(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me");
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new GetIndexRequest().indices("a");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "b"));
        assertThat(user.runAs(), is(notNullValue()));
        roleMap.put("can run as", new RoleDescriptor("can run as",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
                new String[] { "run as me" }));

        if (randomBoolean()) {
            ClusterState state = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(state);
            when(state.metaData()).thenReturn(MetaData.builder()
                    .put(new IndexMetaData.Builder("a")
                            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                            .numberOfShards(1).numberOfReplicas(0).build(), true)
                    .build());
            roleMap.put("b", new RoleDescriptor("b",null,
                    new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() }, null));
        } else {
            mockEmptyMetaData();
        }

        assertThrowsAuthorizationExceptionRunAs(
                () -> authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me");
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new GetIndexRequest().indices("b");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "b"));
        assertThat(user.runAs(), is(notNullValue()));
        roleMap.put("can run as", new RoleDescriptor("can run as",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
                new String[] { "run as me" }));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder("b")
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());
        roleMap.put("b", new RoleDescriptor("b",null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() }, null));

        authorize(createAuthentication(user), "indices:a", request);
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessGranted(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNonXPackUserCannotExecuteOperationAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all access",new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(BulkAction.NAME + "[s]", new DeleteRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(BulkAction.NAME + "[s]", new IndexRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
                new TermVectorsRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
                new TermVectorsRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest()
                .addAliasAction(AliasActions.add().alias("security_alias").index(SecurityTemplateService.SECURITY_INDEX_NAME))));
        requests.add(
                new Tuple<>(UpdateSettingsAction.NAME, new UpdateSettingsRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));

        for (Tuple<String, TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            assertThrowsAuthorizationException(
                    () -> authorize(createAuthentication(user), action, request),
                    action, "all_access_user");
            verify(auditTrail).accessDenied(user, action, request);
            verifyNoMoreInteractions(auditTrail);
        }

        // we should allow waiting for the health of the index or any index if the user has this permission
        ClusterHealthRequest request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME);
        authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);

        // multiple indices
        request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "foo", "bar");
        authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);

        SearchRequest searchRequest = new SearchRequest("_all");
        authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_LIST, Arrays.asList(searchRequest.indices()));
    }

    public void testGrantedNonXPackUserCanExecuteMonitoringOperationsAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        roleMap.put("all_access", new RoleDescriptor("all access",new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() }, null));
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        List<Tuple<String, ? extends TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(IndicesStatsAction.NAME, new IndicesStatsRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(RecoveryAction.NAME, new RecoveryRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesSegmentsAction.NAME,
                new IndicesSegmentsRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(GetSettingsAction.NAME, new GetSettingsRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesShardStoresAction.NAME,
                new IndicesShardStoresRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(UpgradeStatusAction.NAME,
                new UpgradeStatusRequest().indices(SecurityTemplateService.SECURITY_INDEX_NAME)));

        for (Tuple<String, ? extends TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            authorize(createAuthentication(user), action, request);
            verify(auditTrail).accessGranted(user, action, request);
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        for (User user : Arrays.asList(XPackUser.INSTANCE, superuser)) {
            List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
            requests.add(new Tuple<>(DeleteAction.NAME, new DeleteRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(BulkAction.NAME + "[s]",
                    new DeleteRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(IndexAction.NAME, new IndexRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(BulkAction.NAME + "[s]", new IndexRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SecurityTemplateService.SECURITY_INDEX_NAME)));
            requests.add(new Tuple<>(TermVectorsAction.NAME,
                    new TermVectorsRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(TermVectorsAction.NAME,
                    new TermVectorsRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest()
                    .addAliasAction(AliasActions.add().alias("security_alias").index(SecurityTemplateService.SECURITY_INDEX_NAME))));
            requests.add(new Tuple<>(ClusterHealthAction.NAME, new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME)));
            requests.add(new Tuple<>(ClusterHealthAction.NAME,
                    new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "foo", "bar")));

            for (Tuple<String, TransportRequest> requestTuple : requests) {
                String action = requestTuple.v1();
                TransportRequest request = requestTuple.v2();
                authorize(createAuthentication(user), action, request);
                verify(auditTrail).accessGranted(user, action, request);
            }
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndexWithWildcard() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        String action = SearchAction.NAME;
        SearchRequest request = new SearchRequest("_all");
        authorize(createAuthentication(XPackUser.INSTANCE), action, request);
        verify(auditTrail).accessGranted(XPackUser.INSTANCE, action, request);
        assertThat(request.indices(), arrayContaining(".security"));

        request = new SearchRequest("_all");
        authorize(createAuthentication(superuser), action, request);
        verify(auditTrail).accessGranted(superuser, action, request);
        assertThat(request.indices(), arrayContaining(".security"));
    }

    public void testAnonymousRolesAreAppliedToOtherUsers() {
        TransportRequest request = new ClusterHealthRequest();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role",new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();

        // sanity check the anonymous user
        authorize(createAuthentication(anonymousUser), ClusterHealthAction.NAME, request);
        authorize(createAuthentication(anonymousUser), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));

        // test the no role user
        final User userWithNoRoles = new User("no role user");
        authorize(createAuthentication(userWithNoRoles), ClusterHealthAction.NAME, request);
        authorize(createAuthentication(userWithNoRoles), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));
    }

    public void testDefaultRoleUserWithoutRoles() {
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(new User("no role user"), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertEquals(Role.EMPTY, roles);
    }

    public void testAnonymousUserEnabledRoleAdded() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role",new String[] { "all" },
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(new User("no role user"), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(roles.name(), containsString("anonymous_user_role"));
    }

    public void testCompositeActionsAreImmediatelyRejected() {
        //if the user has no permission for composite actions against any index, the request fails straight-away in the main action
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "no_indices");
        roleMap.put("no_indices", new RoleDescriptor("no_indices", null, null, null));
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(user), action, request), action, "test user");
        verify(auditTrail).accessDenied(user, action, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsIndicesAreNotChecked() {
        //if the user has permission for some index, the request goes through without looking at the indices, they will be checked later
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "role");
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() },
                null));
        authorize(createAuthentication(user), action, request);
        verify(auditTrail).accessGranted(user, action, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsMustImplementCompositeIndicesRequest() {
        String action = randomCompositeRequest().v1();
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "role");
        roleMap.put("role", new RoleDescriptor("role", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() },
                null));
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> authorize(createAuthentication(user), action, request));
        assertThat(illegalStateException.getMessage(), containsString("Composite actions must implement CompositeIndicesRequest"));
    }

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() {
        String action;
        switch(randomIntBetween(0, 4)) {
            case 0:
                action = MultiGetAction.NAME + "[shard]";
                break;
            case 1:
                //reindex, msearch, search template, and multi search template delegate to search
                action = SearchAction.NAME;
                break;
            case 2:
                action = MultiTermVectorsAction.NAME + "[shard]";
                break;
            case 3:
                action = BulkAction.NAME + "[s]";
                break;
            case 4:
                action = "indices:data/read/mpercolate[s]";
                break;
            default:
                throw new UnsupportedOperationException();
        }
        logger.info("--> action: {}", action);

        TransportRequest request = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        User userAllowed = new User("userAllowed", "roleAllowed");
        roleMap.put("roleAllowed", new RoleDescriptor("roleAllowed", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index").privileges("all").build() }, null));
        User userDenied = new User("userDenied", "roleDenied");
        roleMap.put("roleDenied", new RoleDescriptor("roleDenied", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null));
        mockEmptyMetaData();
        authorize(createAuthentication(userAllowed), action, request);
        assertThrowsAuthorizationException(
                () -> authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    public void testSameUserPermission() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
                new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request() :
                new AuthenticateRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
                .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAsciiOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ?
                new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
                new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
                .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAsciiOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        final User user2 = new User("admin", new String[] { "bar" }, user);
        when(authentication.getUser()).thenReturn(user2);
        when(authentication.getRunAsUser()).thenReturn(user);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
                .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAsciiOfLengthBetween(4, 12));
        // this should still fail since the username is still different
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest)request).username("joe");
        } else {
            ((AuthenticateRequest)request).username("joe");
        }
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowOtherActions() {
        final User user = mock(User.class);
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(PutUserAction.NAME, DeleteUserAction.NAME, ClusterHealthAction.NAME, ClusterStateAction.NAME,
                ClusterStatsAction.NAME, GetLicenseAction.NAME);
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(randomBoolean() ? user : new User("runAs"));
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
                .thenReturn(randomAsciiOfLengthBetween(4, 12));

        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verifyZeroInteractions(user, request, authentication);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final String username = "joe";
        final User runAs = new User(username);
        final User user = new User("admin", new String[] { "bar" }, runAs);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
                new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
                new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(runAs);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(authentication.isRunAs()).thenReturn(true);
        when(lookedUpBy.getType())
                .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealm.TYPE) : randomAsciiOfLengthBetween(4, 12));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        when(authentication.getRunAsUser()).thenReturn(user);
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(user);
        when(authentication.isRunAs()).thenReturn(false);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomFrom(LdapRealm.LDAP_TYPE, FileRealm.TYPE, LdapRealm.AD_TYPE, PkiRealm.TYPE,
                randomAsciiOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getRunAsUser();
        verify(authentication).getAuthenticatedBy();
        verify(authentication).isRunAs();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final User runAs = new User("joe");
        final User user = new User("admin", new String[] { "bar" }, runAs);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(runAs.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getRunAsUser()).thenReturn(runAs);
        when(authentication.isRunAs()).thenReturn(true);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(randomFrom(LdapRealm.LDAP_TYPE, FileRealm.TYPE, LdapRealm.AD_TYPE, PkiRealm.TYPE,
                randomAsciiOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authentication).getLookedUpBy();
        verify(authentication).getRunAsUser();
        verify(authentication).isRunAs();
        verify(lookedUpBy).getType();
        verifyNoMoreInteractions(authentication, lookedUpBy, authenticatedBy);
    }

    private static Tuple<String, TransportRequest> randomCompositeRequest() {
        switch(randomIntBetween(0, 7)) {
            case 0:
                return Tuple.tuple(MultiGetAction.NAME, new MultiGetRequest().add("index", "type", "id"));
            case 1:
                return Tuple.tuple(MultiSearchAction.NAME, new MultiSearchRequest().add(new SearchRequest()));
            case 2:
                return Tuple.tuple(MultiTermVectorsAction.NAME, new MultiTermVectorsRequest().add("index", "type", "id"));
            case 3:
                return Tuple.tuple(BulkAction.NAME, new BulkRequest().add(new DeleteRequest("index", "type", "id")));
            case 4:
                return Tuple.tuple("indices:data/read/mpercolate", new MockCompositeIndicesRequest());
            case 5:
                return Tuple.tuple("indices:data/read/msearch/template", new MockCompositeIndicesRequest());
            case 6:
                return Tuple.tuple("indices:data/read/search/template", new MockCompositeIndicesRequest());
            case 7:
                return Tuple.tuple("indices:data/write/reindex", new MockCompositeIndicesRequest());
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static class MockCompositeIndicesRequest extends TransportRequest implements CompositeIndicesRequest {
    }

    public void testDoesNotUseRolesStoreForXPackUser() {
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(XPackUser.INSTANCE, rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(roles, equalTo(ReservedRolesStore.SUPERUSER_ROLE));
        verifyZeroInteractions(rolesStore);
    }

    public void testGetRolesForSystemUserThrowsException() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> authorizationService.roles(SystemUser.INSTANCE,
                null));
        assertEquals("the user [_system] is the system user and we should never try to get its roles", iae.getMessage());
    }

    private static Authentication createAuthentication(User user) {
        RealmRef lookedUpBy = user.runAs() == null ? null : new RealmRef("looked", "up", "by");
        return new Authentication(user, new RealmRef("test", "test", "foo"), lookedUpBy);
    }

    private ClusterState mockEmptyMetaData() {
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);
        return state;
    }
}
