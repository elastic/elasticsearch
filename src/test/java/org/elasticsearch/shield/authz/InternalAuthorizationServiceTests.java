/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.search.*;
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
import org.elasticsearch.shield.authc.AuthenticationException;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class InternalAuthorizationServiceTests extends ElasticsearchTestCase {

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
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService);
    }

    @Test
    public void testActionsSystemUserIsAuthorized() {
        TransportRequest request = mock(TransportRequest.class);

        // A failure would throw an exception
        internalAuthorizationService.authorize(User.SYSTEM, "indices:monitor/whatever", request);
        verify(auditTrail).accessGranted(User.SYSTEM, "indices:monitor/whatever", request);

        internalAuthorizationService.authorize(User.SYSTEM, "internal:whatever", request);
        verify(auditTrail).accessGranted(User.SYSTEM, "internal:whatever", request);
    }

    @Test
    public void testIndicesActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "indices:", request);
            fail("action beginning with indices should have failed");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "indices:", request);
        }
    }

    @Test
    public void testClusterAdminActionsAreNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "cluster:admin/whatever", request);
            fail("action beginning with cluster:admin/whatever should have failed");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [cluster:admin/whatever] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "cluster:admin/whatever", request);
        }
    }

    @Test
    public void testClusterAdminSnapshotStatusActionIsNotAuthorized() {
        TransportRequest request = mock(TransportRequest.class);
        try {
            internalAuthorizationService.authorize(User.SYSTEM, "cluster:admin/snapshot/status", request);
            fail("action beginning with cluster:admin/snapshot/status should have failed");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [cluster:admin/snapshot/status] is unauthorized for user [" + User.SYSTEM.principal() + "]"));
            verify(auditTrail).accessDenied(User.SYSTEM, "cluster:admin/snapshot/status", request);
        }
    }

    @Test
    public void testNoRolesCausesDenial() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User.Simple("test user");
        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("user without roles should be denied");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
        }
    }

    @Test
    public void testUnknownRoleCausesDenial() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User.Simple("test user", "non-existent-role");
        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("user with unknown role only should have been denied");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
        }
    }

    @Test
    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User.Simple("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_role").add(Privilege.Index.ALL, "a").build());

        try {
            internalAuthorizationService.authorize(user, "whatever", request);
            fail("non indices and non cluster requests should be denied");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [whatever] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "whatever", request);
        }
    }

    @Test
    public void testThatRoleWithNoIndicesIsDenied() {
        TransportRequest request = new IndicesExistsRequest("a");
        User user = new User.Simple("test user", "no_indices");
        when(rolesStore.role("no_indices")).thenReturn(Permission.Global.Role.builder("no_indices").set(Privilege.Cluster.action("")).build());

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("user only has cluster roles so indices requests should fail");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
        }
    }

    @Test
    public void testScrollRelatedRequestsAllowed() {
        User user = new User.Simple("test user", "a_all");
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

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.SCAN_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.SCAN_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME, request);

        internalAuthorizationService.authorize(user, SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(user, SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
    }

    @Test
    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        User user = new User.Simple("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(user, "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, "indices:a", request);
            verify(clusterService, times(2)).state();
            verify(state, times(2)).metaData();
        }
    }

    @Test
    public void testCreateIndexWithAliasWithoutPermissions() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mock(ClusterState.class);
        User user = new User.Simple("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(user, CreateIndexAction.NAME, request);
            fail("indices creation request with alias should be denied since user does not have permission to alias");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [" + IndicesAliasesAction.NAME + "] is unauthorized for user [test user]"));
            verify(auditTrail).accessDenied(user, IndicesAliasesAction.NAME, request);
            verify(clusterService).state();
            verify(state).metaData();
        }
    }

    @Test
    public void testCreateIndexWithAlias() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mock(ClusterState.class);
        User user = new User.Simple("test user", "a_all");
        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a", "a2").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        internalAuthorizationService.authorize(user, CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(user, CreateIndexAction.NAME, request);
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state).metaData();
    }

    @Test
    public void testIndicesAliasesWithNoRolesUser() {
        User user = new User.Simple("test user");

        ImmutableList<String> list = internalAuthorizationService.authorizedIndicesAndAliases(user, "");
        assertThat(list.isEmpty(), is(true));
    }

    @Test
    public void testIndicesAliasesWithUserHavingRoles() {
        User user = new User.Simple("test user", "a_star", "b");
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

        ImmutableList<String> list = internalAuthorizationService.authorizedIndicesAndAliases(user, SearchAction.NAME);
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertThat(list, not(contains("bbbbb")));
        assertThat(list, not(contains("ba")));
    }

    @Test
    public void testDenialForAnonymousUser() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        AnonymousService anonymousService = new AnonymousService(Settings.builder().put("shield.authc.anonymous.roles", "a_all").build());
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService);

        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(anonymousService.anonymousUser(), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (AuthorizationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] is unauthorized for user [" + anonymousService.anonymousUser().principal() + "]"));
            verify(auditTrail).accessDenied(anonymousService.anonymousUser(), "indices:a", request);
            verify(clusterService, times(2)).state();
            verify(state, times(2)).metaData();
        }
    }

    @Test
    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new IndicesExistsRequest("b");
        ClusterState state = mock(ClusterState.class);
        AnonymousService anonymousService = new AnonymousService(Settings.builder()
                .put("shield.authc.anonymous.roles", "a_all")
                .put(AnonymousService.SETTING_AUTHORIZATION_EXCEPTION_ENABLED, false)
                .build());
        internalAuthorizationService = new InternalAuthorizationService(Settings.EMPTY, rolesStore, clusterService, auditTrail, anonymousService);

        when(rolesStore.role("a_all")).thenReturn(Permission.Global.Role.builder("a_all").add(Privilege.Index.ALL, "a").build());
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);

        try {
            internalAuthorizationService.authorize(anonymousService.anonymousUser(), "indices:a", request);
            fail("indices request for b should be denied since there is no such index");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("action [indices:a] requires authentication"));
            verify(auditTrail).accessDenied(anonymousService.anonymousUser(), "indices:a", request);
            verify(clusterService, times(2)).state();
            verify(state, times(2)).metaData();
        }
    }
}
