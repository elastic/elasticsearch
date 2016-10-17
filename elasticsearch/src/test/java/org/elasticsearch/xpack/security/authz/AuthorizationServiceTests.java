/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
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
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.permission.DefaultRole;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.permission.SuperuserRole;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.GeneralPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAs;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuthorizationServiceTests extends ESTestCase {
    private AuditTrailService auditTrail;
    private CompositeRolesStore rolesStore;
    private ClusterService clusterService;
    private AuthorizationService authorizationService;
    private ThreadContext threadContext;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        auditTrail = mock(AuditTrailService.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        authorizationService = new AuthorizationService(Settings.EMPTY, rolesStore, clusterService,
                auditTrail, new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(Settings.EMPTY));
    }

    public void testActionsSystemUserIsAuthorized() {
        TransportRequest request = mock(TransportRequest.class);

        // A failure would throw an exception
        authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "indices:monitor/whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "indices:monitor/whatever", request);

        authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "internal:whatever", request);
        verify(auditTrail).accessGranted(SystemUser.INSTANCE, "internal:whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "indices:", request),
                "indices:", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "indices:", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/whatever", request),
                "cluster:admin/whatever", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/snapshot/status", request),
                "cluster:admin/snapshot/status", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/snapshot/status", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNoRolesCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user", "non-existent-role");
        mockEmptyMetaData();
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_role").add(IndexPrivilege.ALL, "a").build());

        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), "whatever", request),
                "whatever", "test user");
        verify(auditTrail).accessDenied(user, "whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatRoleWithNoIndicesIsDenied() {
        TransportRequest request = new IndicesExistsRequest("a");
        User user = new User("test user", "no_indices");
        when(rolesStore.role("no_indices")).thenReturn(Role.builder("no_indices").cluster(ClusterPrivilege.action("")).build());
        mockEmptyMetaData();

        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user");
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testSearchAgainstEmptyCluster() {
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_role").add(IndexPrivilege.ALL, "a").build());
        mockEmptyMetaData();

        {
            //ignore_unavailable set to false, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                    .indicesOptions(IndicesOptions.fromOptions(false, true, true, false));

            assertThrowsAuthorizationException(
                    () -> authorizationService.authorize(createAuthentication(user), SearchAction.NAME, searchRequest),
                    SearchAction.NAME, "test user");
            verify(auditTrail).accessDenied(user, SearchAction.NAME, searchRequest);
            verifyNoMoreInteractions(auditTrail);
        }

        {
            //ignore_unavailable and allow_no_indices both set to true, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                    .indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            authorizationService.authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
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
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_role").add(IndexPrivilege.ALL, "a").build());
        mockEmptyMetaData();

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        authorizationService.authorize(createAuthentication(user), ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(user, ClearScrollAction.NAME, clearScrollRequest);

        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        authorizationService.authorize(createAuthentication(user), SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(user, SearchScrollAction.NAME, searchScrollRequest);

        // We have to use a mock request for other Scroll actions as the actual requests are package private to SearchTransportService
        TransportRequest request = mock(TransportRequest.class);
        authorizationService
                .authorize(createAuthentication(user), SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);

        authorizationService.authorize(createAuthentication(user), SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);

        authorizationService.authorize(createAuthentication(user), SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);

        authorizationService.authorize(createAuthentication(user), SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);

        authorizationService.authorize(createAuthentication(user), SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());

        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
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
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());

        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), CreateIndexAction.NAME, request),
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
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a", "a2").build());

        authorizationService.authorize(createAuthentication(user), CreateIndexAction.NAME, request);

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

        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());

        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(anonymousUser), "indices:a", request),
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

        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());

        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> authorizationService.authorize(createAuthentication(anonymousUser), "indices:a", request));
        assertAuthenticationException(securityException, containsString("action [indices:a] requires authentication"));
        verify(auditTrail).accessDenied(anonymousUser, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testRunAsRequestWithNoRolesUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", null, new User("run as me", new String[] { "admin" }));
        assertThat(user.runAs(), is(notNullValue()));
        assertThrowsAuthorizationExceptionRunAs(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me"); // run as [run as me]
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "doesn't exist"));
        assertThat(user.runAs(), is(notNullValue()));
        when(rolesStore.role("can run as")).thenReturn(Role
                .builder("can run as")
                .runAs(new GeneralPrivilege("", "not the right user"))
                .add(IndexPrivilege.ALL, "a")
                .build());

        assertThrowsAuthorizationExceptionRunAs(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me");
        verify(auditTrail).runAsDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new GetIndexRequest().indices("a");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "b"));
        assertThat(user.runAs(), is(notNullValue()));
        when(rolesStore.role("can run as")).thenReturn(Role
                .builder("can run as")
                .runAs(new GeneralPrivilege("", "run as me"))
                .add(IndexPrivilege.ALL, "a")
                .build());

        if (randomBoolean()) {
            ClusterState state = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(state);
            when(state.metaData()).thenReturn(MetaData.builder()
                    .put(new IndexMetaData.Builder("a")
                            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                            .numberOfShards(1).numberOfReplicas(0).build(), true)
                    .build());
            when(rolesStore.role("b")).thenReturn(Role
                    .builder("b")
                    .add(IndexPrivilege.ALL, "b")
                    .build());
        } else {
            mockEmptyMetaData();
        }

        assertThrowsAuthorizationExceptionRunAs(
                () -> authorizationService.authorize(createAuthentication(user), "indices:a", request),
                "indices:a", "test user", "run as me");
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessDenied(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new GetIndexRequest().indices("b");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", "b"));
        assertThat(user.runAs(), is(notNullValue()));
        when(rolesStore.role("can run as")).thenReturn(Role
                .builder("can run as")
                .runAs(new GeneralPrivilege("", "run as me"))
                .add(IndexPrivilege.ALL, "a")
                .build());
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder("b")
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());
        when(rolesStore.role("b")).thenReturn(Role
                .builder("b")
                .add(IndexPrivilege.ALL, "b")
                .build());

        authorizationService.authorize(createAuthentication(user), "indices:a", request);
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessGranted(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNonXPackUserCannotExecuteOperationAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        when(rolesStore.role("all_access")).thenReturn(Role.builder("all_access")
                .add(IndexPrivilege.ALL, "*")
                .cluster(ClusterPrivilege.ALL)
                .build());
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(DeleteAction.NAME, new DeleteRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndexAction.NAME, new IndexRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
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
                    () -> authorizationService.authorize(createAuthentication(user), action, request),
                    action, "all_access_user");
            verify(auditTrail).accessDenied(user, action, request);
            verifyNoMoreInteractions(auditTrail);
        }

        // we should allow waiting for the health of the index or any index if the user has this permission
        ClusterHealthRequest request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME);
        authorizationService.authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);

        // multiple indices
        request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "foo", "bar");
        authorizationService.authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);

        SearchRequest searchRequest = new SearchRequest("_all");
        authorizationService.authorize(createAuthentication(user), SearchAction.NAME, searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_LIST, Arrays.asList(searchRequest.indices()));
    }

    public void testGrantedNonXPackUserCanExecuteMonitoringOperationsAgainstSecurityIndex() {
        User user = new User("all_access_user", "all_access");
        when(rolesStore.role("all_access")).thenReturn(Role.builder("all_access")
                .add(IndexPrivilege.ALL, "*")
                .cluster(ClusterPrivilege.ALL)
                .build());
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
            authorizationService.authorize(createAuthentication(user), action, request);
            verify(auditTrail).accessGranted(user, action, request);
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", SuperuserRole.NAME);
        when(rolesStore.role(SuperuserRole.NAME)).thenReturn(Role.builder(SuperuserRole.DESCRIPTOR).build());
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
            requests.add(new Tuple<>(UpdateAction.NAME, new UpdateRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
            requests.add(new Tuple<>(IndexAction.NAME, new IndexRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "type", "id")));
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
                authorizationService.authorize(createAuthentication(user), action, request);
                verify(auditTrail).accessGranted(user, action, request);
            }
        }
    }

    public void testXPackUserAndSuperusersCanExecuteOperationAgainstSecurityIndexWithWildcard() {
        final User superuser = new User("custom_admin", SuperuserRole.NAME);
        when(rolesStore.role(SuperuserRole.NAME)).thenReturn(Role.builder(SuperuserRole.DESCRIPTOR).build());
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder(SecurityTemplateService.SECURITY_INDEX_NAME)
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());

        String action = SearchAction.NAME;
        SearchRequest request = new SearchRequest("_all");
        authorizationService.authorize(createAuthentication(XPackUser.INSTANCE), action, request);
        verify(auditTrail).accessGranted(XPackUser.INSTANCE, action, request);
        assertThat(request.indices(), arrayContaining(".security"));

        request = new SearchRequest("_all");
        authorizationService.authorize(createAuthentication(superuser), action, request);
        verify(auditTrail).accessGranted(superuser, action, request);
        assertThat(request.indices(), arrayContaining(".security"));
    }

    public void testAnonymousRolesAreAppliedToOtherUsers() {
        TransportRequest request = new ClusterHealthRequest();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        when(rolesStore.role("anonymous_user_role"))
                .thenReturn(Role.builder("anonymous_user_role")
                        .cluster(ClusterPrivilege.ALL)
                        .add(IndexPrivilege.ALL, "a")
                        .build());
        mockEmptyMetaData();

        // sanity check the anonymous user
        authorizationService.authorize(createAuthentication(anonymousUser), ClusterHealthAction.NAME, request);
        authorizationService.authorize(createAuthentication(anonymousUser), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));

        // test the no role user
        final User userWithNoRoles = new User("no role user");
        authorizationService.authorize(createAuthentication(userWithNoRoles), ClusterHealthAction.NAME, request);
        authorizationService.authorize(createAuthentication(userWithNoRoles), IndicesExistsAction.NAME, new IndicesExistsRequest("a"));
    }

    public void testDefaultRoleUserWithoutRoles() {
        Collection<Role> roles = authorizationService.roles(new User("no role user"));
        assertEquals(1, roles.size());
        assertEquals(DefaultRole.NAME, roles.iterator().next().name());
    }

    public void testDefaultRoleUserWithoutRolesAnonymousUserEnabled() {
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        when(rolesStore.role("anonymous_user_role"))
                .thenReturn(Role.builder("anonymous_user_role")
                        .cluster(ClusterPrivilege.ALL)
                        .add(IndexPrivilege.ALL, "a")
                        .build());
        mockEmptyMetaData();
        Collection<Role> roles = authorizationService.roles(new User("no role user"));
        assertEquals(2, roles.size());
        for (Role role : roles) {
            assertThat(role.name(), either(equalTo(DefaultRole.NAME)).or(equalTo("anonymous_user_role")));
        }
    }

    public void testDefaultRoleUserWithSomeRole() {
        when(rolesStore.role("role"))
                .thenReturn(Role.builder("role")
                        .cluster(ClusterPrivilege.ALL)
                        .add(IndexPrivilege.ALL, "a")
                        .build());
        Collection<Role> roles = authorizationService.roles(new User("user with role", "role"));
        assertEquals(2, roles.size());
        for (Role role : roles) {
            assertThat(role.name(), either(equalTo(DefaultRole.NAME)).or(equalTo("role")));
        }
    }

    public void testCompositeActionsAreImmediatelyRejected() {
        //if the user has no permission for composite actions against any index, the request fails straight-away in the main action
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "no_indices");
        when(rolesStore.role("no_indices")).thenReturn(Role.builder("no_indices").cluster(ClusterPrivilege.action("")).build());
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(user), action, request), action, "test user");
        verify(auditTrail).accessDenied(user, action, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsIndicesAreNotChecked() {
        //if the user has permission for some index, the request goes through without looking at the indices, they will be checked later
        Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        String action = compositeRequest.v1();
        TransportRequest request = compositeRequest.v2();
        User user = new User("test user", "role");
        when(rolesStore.role("role")).thenReturn(Role.builder("role").add(IndexPrivilege.ALL, randomBoolean() ? "a" : "index").build());
        authorizationService.authorize(createAuthentication(user), action, request);
        verify(auditTrail).accessGranted(user, action, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsMustImplementCompositeIndicesRequest() {
        String action = randomCompositeRequest().v1();
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "role");
        when(rolesStore.role("role")).thenReturn(Role.builder("role").add(IndexPrivilege.ALL, randomBoolean() ? "a" : "index").build());
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
                () -> authorizationService.authorize(createAuthentication(user), action, request));
        assertThat(illegalStateException.getMessage(), containsString("Composite actions must implement CompositeIndicesRequest"));
    }

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() {
        String action;
        switch(randomIntBetween(0, 6)) {
            case 0:
                action = MultiGetAction.NAME + "[shard]";
                break;
            case 1:
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
            case 5:
                action = "indices:data/read/search/template";
                break;
            case 6:
                //reindex delegates to search and index
                action = randomBoolean() ? SearchAction.NAME : IndexAction.NAME;
                break;
            default:
                throw new UnsupportedOperationException();
        }

        TransportRequest request = new MockIndicesRequest();
        User userAllowed = new User("userAllowed", "roleAllowed");
        when(rolesStore.role("roleAllowed")).thenReturn(Role.builder("roleAllowed").add(IndexPrivilege.ALL, "index").build());
        User userDenied = new User("userDenied", "roleDenied");
        when(rolesStore.role("roleDenied")).thenReturn(Role.builder("roleDenied").add(IndexPrivilege.ALL, "a").build());
        mockEmptyMetaData();
        authorizationService.authorize(createAuthentication(userAllowed), action, request);
        assertThrowsAuthorizationException(
                () -> authorizationService.authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    private static Tuple<String, TransportRequest> randomCompositeRequest() {
        switch(randomIntBetween(0, 6)) {
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
                return Tuple.tuple("indices:data/write/reindex", new MockCompositeIndicesRequest());
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static class MockIndicesRequest extends TransportRequest implements IndicesRequest {
        @Override
        public String[] indices() {
            return new String[]{"index"};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpandOpen();
        }
    }

    private static class MockCompositeIndicesRequest extends TransportRequest implements CompositeIndicesRequest {
        @Override
        public List<? extends IndicesRequest> subRequests() {
            return Collections.singletonList(new MockIndicesRequest());
        }
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
