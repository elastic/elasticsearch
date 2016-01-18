/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.AnonymousService;
import org.elasticsearch.shield.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthorizationException;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class InternalAuthorizationServiceTests extends ESTestCase {
    private AuditTrail auditTrail;
    private RolesStore rolesStore;
    private ClusterService clusterService;
    private InternalAuthorizationService internalAuthorizationService;

    @Before
    public void setup() {
        rolesStore = mock(RolesStore.class);
        clusterService = mock(ClusterService.class);
        auditTrail = mock(AuditTrail.class);
        AnonymousService anonymousService = new AnonymousService(Settings.EMPTY);
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService, new DefaultAuthenticationFailureHandler());
    }

    public void testActionsSystemUserIsAuthorized() {
        TransportRequest request = mock(TransportRequest.class);

        // A failure would throw an exception
        internalAuthorizationService.authorize(User.SYSTEM, "indices:monitor/whatever", request);
        verify(auditTrail).accessGranted(User.SYSTEM, "indices:monitor/whatever", request);

        internalAuthorizationService.authorize(User.SYSTEM, "internal:whatever", request);
        verify(auditTrail).accessGranted(User.SYSTEM, "internal:whatever", request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "indices:", request);
            fail("action beginning with indices should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "indices:", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "cluster:admin/whatever", request);
            fail("action beginning with cluster:admin/whatever should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [cluster:admin/whatever] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "cluster:admin/whatever", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "cluster:admin/snapshot/status", request);
            fail("action beginning with cluster:admin/snapshot/status should have failed");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [cluster:admin/snapshot/status] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "cluster:admin/snapshot/status", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testNoRolesCausesDenial() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user");
        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("user without roles should be denied");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testUnknownRoleCausesDenial() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("test user", "non-existent-role");
        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
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
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_role").add(Privilege.Index.ALL, "a").build());

        try {
            internalAuthorizationService.authorize(user, "whatever", request);
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
        when(rolesStore.role("no_indices")).thenReturn(Permission.Global.Role.builder("no_indices").cluster(Privilege.Cluster.action("")).build());

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("user only has cluster roles so indices requests should fail");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testScrollRelatedRequestsAllowed() {
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_role").add(Privilege.Index.ALL, "a").build());

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        internalAuthorizationService.authorize(user, ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(user, ClearScrollAction.NAME, clearScrollRequest);

        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        internalAuthorizationService.authorize(user, SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(user, SearchScrollAction.NAME, searchScrollRequest);

        // We have to use a mock request for other Scroll actions as the actual requests are package private to SearchServiceTransportAction
        TransportRequest request = mock(TransportRequest.class);
        internalAuthorizationService.authorize(user, SearchServiceTransportAction.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        User user = new User("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
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
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(user, CreateIndexAction.NAME, request);
            fail("indices creation request with alias should be denied since user does not have permission to alias");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [" + IndicesAliasesAction.NAME + "] is unauthorized for user [test user]"));
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
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a", "a2").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        internalAuthorizationService.authorize(user, CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(user, CreateIndexAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(2)).metaData();
    }

    public void testIndicesAliasesWithNoRolesUser() {
        User user = new User("test user");

        List<String> list = internalAuthorizationService.authorizedIndicesAndAliases(user, "");
        assertThat(list.isEmpty(), is(true));
    }

    public void testIndicesAliasesWithUserHavingRoles() {
        User user = new User("test user", "a_star", "b");
        ClusterState state = mock(ClusterState.class);
        when(rolesStore.role("a_star")).thenReturn(Permission.Global.Role.builder("a_star").add(Privilege.Index.ALL, "a*").build());
        when(rolesStore.role("b")).thenReturn(Permission.Global.Role.builder("a_star").add(Privilege.Index.SEARCH, "b").build());
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

        List<String> list = internalAuthorizationService.authorizedIndicesAndAliases(user, SearchAction.NAME);
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertThat(list.contains("bbbbb"), is(false));
        assertThat(list.contains("ba"), is(false));
    }

    public void testDenialForAnonymousUser() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        AnonymousService anonymousService = new AnonymousService(Settings.builder().put("shield.authc.anonymous.roles", "a_all").build());
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService, new DefaultAuthenticationFailureHandler());

        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(anonymousService.anonymousUser(), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:a] is unauthorized for user [" + anonymousService.anonymousUser().principal() + "]"));
            verify(auditTrail).accessDenied(anonymousService.anonymousUser(), "indices:a", request);
            verifyNoMoreInteractions(auditTrail);
            verify(clusterService, times(2)).state();
            verify(state, times(3)).metaData();
        }
    }

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        AnonymousService anonymousService = new AnonymousService(Settings.builder()
                .put("shield.authc.anonymous.roles", "a_all")
                .put(AnonymousService.SETTING_AUTHORIZATION_EXCEPTION_ENABLED, false)
                .build());
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService, new DefaultAuthenticationFailureHandler());

        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(anonymousService.anonymousUser(), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("action [indices:a] requires authentication"));
            verify(auditTrail).accessDenied(anonymousService.anonymousUser(), "indices:a", request);
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
            internalAuthorizationService.authorize(user, "indices:a", request);
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
        when(rolesStore.role("can run as")).thenReturn(Permission.Global.Role
                .builder("can run as")
                .runAs(new Privilege.General("", "not the right user"))
                .add(Privilege.Index.ALL, "a")
                .build());

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
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
        when(rolesStore.role("can run as")).thenReturn(Permission.Global.Role
                .builder("can run as")
                .runAs(new Privilege.General("", "run as me"))
                .add(Privilege.Index.ALL, "a")
                .build());

        if (randomBoolean()) {
            ClusterState state = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(state);
            when(state.metaData()).thenReturn(MetaData.builder()
                    .put(new IndexMetaData.Builder("a")
                            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                            .numberOfShards(1).numberOfReplicas(0).build(), true)
                    .build());
            when(rolesStore.role("b")).thenReturn(Permission.Global.Role
                    .builder("b")
                    .add(Privilege.Index.ALL, "b")
                    .build());
        }

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
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
        when(rolesStore.role("can run as")).thenReturn(Permission.Global.Role
                .builder("can run as")
                .runAs(new Privilege.General("", "run as me"))
                .add(Privilege.Index.ALL, "a")
                .build());
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder("b")
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());
        when(rolesStore.role("b")).thenReturn(Permission.Global.Role
                .builder("b")
                .add(Privilege.Index.ALL, "b")
                .build());

        internalAuthorizationService.authorize(user, "indices:a", request);
        verify(auditTrail).runAsGranted(user, "indices:a", request);
        verify(auditTrail).accessGranted(user, "indices:a", request);
        verifyNoMoreInteractions(auditTrail);
    }
}
