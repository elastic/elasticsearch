/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
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
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
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
import java.util.List;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthorizationException;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
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
        try {
            authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "indices:", request);
            fail("action beginning with indices should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:] is unauthorized for user [" + SystemUser.INSTANCE.principal() + "]"));
            verify(auditTrail).accessDenied(SystemUser.INSTANCE, "indices:", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/whatever", request);
            fail("action beginning with cluster:admin/whatever should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [cluster:admin/whatever] is unauthorized for user [" + SystemUser.INSTANCE.principal() + "]"));
            verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/whatever", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            authorizationService.authorize(createAuthentication(SystemUser.INSTANCE), "cluster:admin/snapshot/status", request);
            fail("action beginning with cluster:admin/snapshot/status should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [cluster:admin/snapshot/status] is unauthorized for user [" +
                    SystemUser.INSTANCE.principal() + "]"));
            verify(auditTrail).accessDenied(SystemUser.INSTANCE, "cluster:admin/snapshot/status", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testNoRolesCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user");
        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("user without roles should be denied");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testUnknownRoleCausesDenial() {
        TransportRequest request = new SearchRequest();
        User user = new User("test user", "non-existent-role");
        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("user with unknown role only should have been denied");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_role").add(IndexPrivilege.ALL, "a").build());

        try {
            authorizationService.authorize(createAuthentication(user), "whatever", request);
            fail("non indices and non cluster requests should be denied");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [whatever] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "whatever", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testThatRoleWithNoIndicesIsDenied() {
        TransportRequest request = new IndicesExistsRequest("a");
        User user = new User("test user", "no_indices");
        when(rolesStore.role("no_indices")).thenReturn(Role.builder("no_indices").cluster(ClusterPrivilege.action("")).build());

        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("user only has cluster roles so indices requests should fail");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testScrollRelatedRequestsAllowed() {
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_role").add(IndexPrivilege.ALL, "a").build());

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
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
            verify(clusterService, times(2)).state();
            verify(state, times(3)).metaData();
        }
    }

    public void testCreateIndexWithAliasWithoutPermissions() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mock(ClusterState.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            authorizationService.authorize(createAuthentication(user), CreateIndexAction.NAME, request);
            fail("indices creation request with alias should be denied since user does not have permission to alias");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [" + IndicesAliasesAction.NAME + "] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, IndicesAliasesAction.NAME, request);
            verifyNoMoreInteractions(auditTrail);
            verify(clusterService).state();
            verify(state, times(2)).metaData();
        }
    }

    public void testCreateIndexWithAlias() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mock(ClusterState.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a", "a2").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        authorizationService.authorize(createAuthentication(user), CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(user, CreateIndexAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(2)).metaData();
    }

    public void testIndicesAliasesWithNoRolesUser() {
        User user = new User("test user");

        List<String> list = authorizationService.authorizedIndicesAndAliases(user, "");
        assertThat(list.isEmpty(), is(true));
    }

    public void testIndicesAliasesWithUserHavingRoles() {
        User user = new User("test user", "a_star", "b");
        ClusterState state = mock(ClusterState.class);
        when(rolesStore.role("a_star")).thenReturn(Role.builder("a_star").add(IndexPrivilege.ALL, "a*").build());
        when(rolesStore.role("b")).thenReturn(Role.builder("a_star").add(IndexPrivilege.READ, "b").build());
        when(clusterService.state()).thenReturn(state);
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("b")
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder("ab").build())
                        .putAlias(new AliasMetaData.Builder("ba").build())
                        .build(), true)
                .build());

        List<String> list = authorizationService.authorizedIndicesAndAliases(user, SearchAction.NAME);
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertThat(list.contains("bbbbb"), is(false));
        assertThat(list.contains("ba"), is(false));
    }

    public void testDenialForAnonymousUser() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "a_all").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);

        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            authorizationService.authorize(createAuthentication(anonymousUser), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e,
                    containsString("action [indices:a] is unauthorized for user [" + anonymousUser.principal() + "]"));
            verify(auditTrail).accessDenied(anonymousUser, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
            verify(clusterService, times(2)).state();
            verify(state, times(3)).metaData();
        }
    }

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        Settings settings = Settings.builder()
                .put(AnonymousUser.ROLES_SETTING.getKey(), "a_all")
                .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false)
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(settings));

        when(rolesStore.role("a_all")).thenReturn(Role.builder("a_all").add(IndexPrivilege.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            authorizationService.authorize(createAuthentication(anonymousUser), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("action [indices:a] requires authentication"));
            verify(auditTrail).accessDenied(anonymousUser, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
            verify(clusterService, times(2)).state();
            verify(state, times(3)).metaData();
        }
    }

    public void testRunAsRequestWithNoRolesUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", null, new User("run as me", new String[] { "admin" }));
        assertThat(user.runAs(), is(notNullValue()));
        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("user without roles should be denied for run as");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user] run as [run as me]"));
            verify(auditTrail).runAsDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", new String[] { "doesn't exist" }));
        assertThat(user.runAs(), is(notNullValue()));
        when(rolesStore.role("can run as")).thenReturn(Role
                .builder("can run as")
                .runAs(new GeneralPrivilege("", "not the right user"))
                .add(IndexPrivilege.ALL, "a")
                .build());

        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("user without roles should be denied for run as");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user] run as [run as me]"));
            verify(auditTrail).runAsDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new IndicesExistsRequest("a");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", new String[] { "b" }));
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
        }

        try {
            authorizationService.authorize(createAuthentication(user), "indices:a", request);
            fail("the run as user's role doesn't exist so they should not get authorized");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user] run as [run as me]"));
            verify(auditTrail).runAsGranted(user, "indices:a", request);
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new IndicesExistsRequest("b");
        User user = new User("test user", new String[] { "can run as" }, new User("run as me", new String[] { "b" }));
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
            try {
                authorizationService.authorize(createAuthentication(user), action, request);
                fail("only the xpack user can execute operation [" + action + "] against the internal index");
            } catch (ElasticsearchSecurityException e) {
                assertAuthorizationException(e, containsString("action [" + action + "] is unauthorized for user [all_access_user]"));
                verify(auditTrail).accessDenied(user, action, request);
                verifyNoMoreInteractions(auditTrail);
            }
        }

        // we should allow waiting for the health of the index or any index if the user has this permission
        ClusterHealthRequest request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME);
        authorizationService.authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);

        // multiple indices
        request = new ClusterHealthRequest(SecurityTemplateService.SECURITY_INDEX_NAME, "foo", "bar");
        authorizationService.authorize(createAuthentication(user), ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(user, ClusterHealthAction.NAME, request);
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
            authorizationService.authorize(createAuthentication(XPackUser.INSTANCE), action, request);
            verify(auditTrail).accessGranted(XPackUser.INSTANCE, action, request);
            authorizationService.authorize(createAuthentication(superuser), action, request);
            verify(auditTrail).accessGranted(superuser, action, request);
        }
    }

    private Authentication createAuthentication(User user) {
        RealmRef lookedUpBy = user.runAs() == null ? null : new RealmRef("looked", "up", "by");
        return new Authentication(user, new RealmRef("test", "test", "foo"), lookedUpBy);
    }
}
