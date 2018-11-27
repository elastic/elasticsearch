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
import org.elasticsearch.action.DocWriteRequest;
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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
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
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.UserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAs;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_INDEX_NAME;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
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

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        final Settings settings = Settings.builder()
            .put("cluster.remote.other_cluster.seeds", "localhost:9999")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        auditTrail = mock(AuditTrailService.class);
        threadContext = new ThreadContext(settings);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(settings);

        final NativePrivilegeStore privilegesStore = mock(NativePrivilegeStore.class);
        doAnswer(i -> {
                assertThat(i.getArguments().length, equalTo(3));
                final Object arg2 = i.getArguments()[2];
                assertThat(arg2, instanceOf(ActionListener.class));
                ActionListener<Collection<ApplicationPrivilege>> listener = (ActionListener<Collection<ApplicationPrivilege>>) arg2;
                listener.onResponse(Collections.emptyList());
                return null;
            }
        ).when(privilegesStore).getPrivileges(any(Collection.class), any(Collection.class), any(ActionListener.class));

        doAnswer((i) -> {
            ActionListener<Role> callback = (ActionListener<Role>) i.getArguments()[2];
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
                CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, privilegesStore,
                    ActionListener.wrap(r -> callback.onResponse(r), callback::onFailure)
                );
            }
            return Void.TYPE;
        }).when(rolesStore).roles(any(Set.class), any(FieldPermissionsCache.class), any(ActionListener.class));
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService,
            auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, new AnonymousUser(settings));
    }

    private void authorize(Authentication authentication, String action, TransportRequest request) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        AuthorizationUtils.AsyncAuthorizer authorizer = new AuthorizationUtils.AsyncAuthorizer(authentication, future,
            (userRoles, runAsRoles) -> {
                authorizationService.authorize(authentication, action, request, userRoles, runAsRoles);
                future.onResponse(null);
            });
        authorizer.authorize(authorizationService);
        future.actionGet();
    }

    public void testActionsForSystemUserIsAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        // A failure would throw an exception
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String[] actions = { "indices:monitor/whatever", "internal:whatever", "cluster:monitor/whatever", "cluster:admin/reroute",
                "indices:admin/mapping/put", "indices:admin/template/put", "indices:admin/seq_no/global_checkpoint_sync",
                "indices:admin/settings/update" };
        for (String action : actions) {
            authorize(authentication, action, request);
            verify(auditTrail).accessGranted(requestId, authentication, action, request, new String[] { SystemUser.ROLE_NAME });
        }

        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsForSystemUserWhichAreNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:", request),
            "indices:", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(requestId, authentication, "indices:", request, new String[]{SystemUser.ROLE_NAME});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminActionsForSystemUserWhichAreNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/whatever", request),
            "cluster:admin/whatever", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(requestId, authentication, "cluster:admin/whatever", request,
            new String[] { SystemUser.ROLE_NAME });
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminSnapshotStatusActionForSystemUserWhichIsNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/snapshot/status", request),
            "cluster:admin/snapshot/status", SystemUser.INSTANCE.principal());
        verify(auditTrail).accessDenied(requestId, authentication, "cluster:admin/snapshot/status", request,
            new String[] { SystemUser.ROLE_NAME });
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeUsingConditionalPrivileges() {
        final DeletePrivilegesRequest request = new DeletePrivilegesRequest();
        final Authentication authentication = createAuthentication(new User("user1", "role1"));

        final ConditionalClusterPrivilege conditionalClusterPrivilege = Mockito.mock(ConditionalClusterPrivilege.class);
        final Predicate<TransportRequest> requestPredicate = r -> r == request;
        Mockito.when(conditionalClusterPrivilege.getRequestPredicate()).thenReturn(requestPredicate);
        Mockito.when(conditionalClusterPrivilege.getPrivilege()).thenReturn(ClusterPrivilege.MANAGE_SECURITY);
        final ConditionalClusterPrivilege[] conditionalClusterPrivileges = new ConditionalClusterPrivilege[] {
            conditionalClusterPrivilege
        };
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        RoleDescriptor role = new RoleDescriptor("role1", null, null, null, conditionalClusterPrivileges, null, null ,null);
        roleMap.put("role1", role);

        authorize(authentication, DeletePrivilegesAction.NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, DeletePrivilegesAction.NAME, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizationDeniedWhenConditionalPrivilegesDoNotMatch() {
        final DeletePrivilegesRequest request = new DeletePrivilegesRequest();
        final Authentication authentication = createAuthentication(new User("user1", "role1"));

        final ConditionalClusterPrivilege conditionalClusterPrivilege = Mockito.mock(ConditionalClusterPrivilege.class);
        final Predicate<TransportRequest> requestPredicate = r -> false;
        Mockito.when(conditionalClusterPrivilege.getRequestPredicate()).thenReturn(requestPredicate);
        Mockito.when(conditionalClusterPrivilege.getPrivilege()).thenReturn(ClusterPrivilege.MANAGE_SECURITY);
        final ConditionalClusterPrivilege[] conditionalClusterPrivileges = new ConditionalClusterPrivilege[] {
            conditionalClusterPrivilege
        };
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        RoleDescriptor role = new RoleDescriptor("role1", null, null, null, conditionalClusterPrivileges, null, null ,null);
        roleMap.put("role1", role);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, DeletePrivilegesAction.NAME, request),
            DeletePrivilegesAction.NAME, "user1");
        verify(auditTrail).accessDenied(requestId, authentication, DeletePrivilegesAction.NAME, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNoRolesCausesDenial() {
        final TransportRequest request = new SearchRequest();
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", "test user");
        verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCanPerformRemoteSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("other_cluster:index1", "*_cluster:index2", "other_cluster:other_*");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, SearchAction.NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchAction.NAME, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    /**
     * This test mimics {@link #testUserWithNoRolesCanPerformRemoteSearch()} except that
     * while the referenced index _looks_ like a remote index, the remote cluster name has not
     * been defined, so it is actually a local index and access should be denied
     */
    public void testUserWithNoRolesCannotPerformLocalSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("no_such_cluster:index");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, SearchAction.NAME, request),
            SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, SearchAction.NAME, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    /**
     * This test mimics {@link #testUserWithNoRolesCannotPerformLocalSearch()} but includes
     * both local and remote indices, including wildcards
     */
    public void testUserWithNoRolesCanPerformMultiClusterSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("local_index", "wildcard_*", "other_cluster:remote_index", "*:foo?");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, SearchAction.NAME, request),
            SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, SearchAction.NAME, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCannotSql() {
        TransportRequest request = new SqlQueryRequest();
        Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, SqlQueryAction.NAME, request),
            SqlQueryAction.NAME, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, SqlQueryAction.NAME, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }
    /**
     * Verifies that the behaviour tested in {@link #testUserWithNoRolesCanPerformRemoteSearch}
     * does not work for requests that are not remote-index-capable.
     */
    public void testRemoteIndicesOnlyWorkWithApplicableRequestTypes() {
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices("other_cluster:index1", "other_cluster:index2");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, DeleteIndexAction.NAME, request),
            DeleteIndexAction.NAME, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, DeleteIndexAction.NAME, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() {
        Tuple<String, TransportRequest> tuple = randomFrom(asList(
            new Tuple<>(SearchAction.NAME, new SearchRequest()),
            new Tuple<>(IndicesExistsAction.NAME, new IndicesExistsRequest()),
            new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())));
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        final Authentication authentication = createAuthentication(new User("test user", "non-existent-role"));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetaData();
        assertThrowsAuthorizationException(
            () -> authorize(authentication, action, request),
            action, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, action, request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        roleMap.put("a_all", role);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, "whatever", request),
            "whatever", "test user");
        verify(auditTrail).accessDenied(requestId, authentication, "whatever", request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatRoleWithNoIndicesIsDenied() {
        @SuppressWarnings("unchecked")
        Tuple<String, TransportRequest> tuple = randomFrom(
            new Tuple<>(SearchAction.NAME, new SearchRequest()),
            new Tuple<>(IndicesExistsAction.NAME, new IndicesExistsRequest()),
            new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest()));
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        RoleDescriptor role = new RoleDescriptor("a_role", null, null, null);
        roleMap.put("no_indices", role);
        mockEmptyMetaData();

        assertThrowsAuthorizationException(
            () -> authorize(authentication, action, request),
            action, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, action, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testElasticUserAuthorizedForNonChangePasswordRequestsWhenNotInSetupMode() {
        final Authentication authentication = createAuthentication(new ElasticUser(true));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Tuple<String, TransportRequest> request = randomCompositeRequest();
        authorize(authentication, request.v1(), request.v2());

        verify(auditTrail).accessGranted(requestId, authentication, request.v1(), request.v2(), new String[]{ElasticUser.ROLE_NAME});
    }

    public void testSearchAgainstEmptyCluster() {
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        roleMap.put("a_all", role);
        mockEmptyMetaData();

        {
            //ignore_unavailable set to false, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                .indicesOptions(IndicesOptions.fromOptions(false, true,
                    true, false));

            assertThrowsAuthorizationException(
                () -> authorize(authentication, SearchAction.NAME, searchRequest),
                SearchAction.NAME, "test user");
            verify(auditTrail).accessDenied(requestId, authentication, SearchAction.NAME, searchRequest, new String[]{role.getName()});
            verifyNoMoreInteractions(auditTrail);
        }

        {
            //ignore_unavailable and allow_no_indices both set to true, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist")
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            authorize(authentication, SearchAction.NAME, searchRequest);
            verify(auditTrail).accessGranted(requestId, authentication, SearchAction.NAME, searchRequest, new String[]{role.getName()});
            final IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            final IndicesAccessControl.IndexAccessControl indexAccessControl =
                indicesAccessControl.getIndexPermissions(IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER);
            assertFalse(indexAccessControl.getFieldPermissions().hasFieldLevelSecurity());
            assertNull(indexAccessControl.getQueries());
        }
    }

    public void testScrollRelatedRequestsAllowed() {
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        authorize(authentication, ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(requestId, authentication, ClearScrollAction.NAME, clearScrollRequest,
            new String[]{role.getName()});

        final SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        authorize(authentication, SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(requestId, authentication, SearchScrollAction.NAME, searchScrollRequest,
            new String[]{role.getName()});

        // We have to use a mock request for other Scroll actions as the actual requests are package private to SearchTransportService
        final TransportRequest request = mock(TransportRequest.class);
        authorize(authentication, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request,
            new String[]{role.getName()});

        authorize(authentication, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request,
            new String[]{role.getName()});

        authorize(authentication, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request,
            new String[]{role.getName()});

        authorize(authentication, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request,
            new String[]{role.getName()});

        authorize(authentication, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request,
            new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetaData();
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", "test user");
        verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAliasWithoutPermissions() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, CreateIndexAction.NAME, request),
            IndicesAliasesAction.NAME, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, IndicesAliasesAction.NAME, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testCreateIndexWithAlias() {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetaData();
        RoleDescriptor role = new RoleDescriptor("a_all", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a", "a2").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(requestId, authentication, CreateIndexAction.NAME, request, new String[]{role.getName()});
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
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser);

        RoleDescriptor role = new RoleDescriptor("a_all", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() }, null);
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final Authentication authentication = createAuthentication(anonymousUser);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", anonymousUser.principal());
        verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, new String[]{role.getName()});
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
        final Authentication authentication = createAuthentication(new AnonymousUser(settings));
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, new AnonymousUser(settings));

        RoleDescriptor role = new RoleDescriptor("a_all", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
            () -> authorize(authentication, "indices:a", request));
        assertAuthenticationException(securityException, containsString("action [indices:a] requires authentication"));
        verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metaData();
    }

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsError() {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, true);
        TransportRequest request = new GetIndexRequest().indices("not-an-index-*").indicesOptions(options);
        ClusterState state = mockEmptyMetaData();
        RoleDescriptor role = new RoleDescriptor("a_all", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final IndexNotFoundException nfe = expectThrows(
            IndexNotFoundException.class,
            () -> authorize(authentication, GetIndexAction.NAME, request));
        assertThat(nfe.getIndex(), is(notNullValue()));
        assertThat(nfe.getIndex().getName(), is("not-an-index-*"));
        verify(auditTrail).accessDenied(requestId, authentication, GetIndexAction.NAME, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metaData();
    }

    public void testRunAsRequestWithNoRolesUser() {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("run as me", null, new User("test user", "admin")));
        assertNotEquals(authentication.getUser().authenticatedUser(), authentication);
        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", "test user", "run as me"); // run as [run as me]
        verify(auditTrail).runAsDenied(requestId, authentication, "indices:a", request, Role.EMPTY.names());
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithoutLookedUpBy() {
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        AuthenticateRequest request = new AuthenticateRequest("run as me");
        roleMap.put("can run as", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        User user = new User("run as me", Strings.EMPTY_ARRAY, new User("test user", new String[]{"can run as"}));
        Authentication authentication = new Authentication(user, new RealmRef("foo", "bar", "baz"), null);
        assertNotEquals(user.authenticatedUser(), user);
        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, AuthenticateAction.NAME, request),
            AuthenticateAction.NAME, "test user", "run as me"); // run as [run as me]
        verify(auditTrail).runAsDenied(requestId, authentication, AuthenticateAction.NAME, request,
            new String[] { ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName() });
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("run as me", new String[]{"doesn't exist"}, new User("test user", "can run as"));
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor role = new RoleDescriptor("can run as", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()},
            new String[]{"not the right user"});
        roleMap.put("can run as", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", "test user", "run as me");
        verify(auditTrail).runAsDenied(requestId, authentication, "indices:a", request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new GetIndexRequest().indices("a");
        User authenticatedUser = new User("test user", "can run as");
        User user = new User("run as me", new String[]{"b"}, authenticatedUser);
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor runAsRole = new RoleDescriptor("can run as", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()},
            new String[]{"run as me"});
        roleMap.put("can run as", runAsRole);

        RoleDescriptor bRole = new RoleDescriptor("b", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("b").privileges("all").build()}, null);
        boolean indexExists = randomBoolean();
        if (indexExists) {
            ClusterState state = mock(ClusterState.class);
            when(clusterService.state()).thenReturn(state);
            when(state.metaData()).thenReturn(MetaData.builder()
                .put(new IndexMetaData.Builder("a")
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                    .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build());
            roleMap.put("b", bRole);
        } else {
            mockEmptyMetaData();
        }
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a", "test user", "run as me");
        verify(auditTrail).runAsGranted(requestId, authentication, "indices:a", request, new String[]{runAsRole.getName()});
        if (indexExists) {
            verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, new String[]{bRole.getName()});
        } else {
            verify(auditTrail).accessDenied(requestId, authentication, "indices:a", request, Role.EMPTY.names());
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new GetIndexRequest().indices("b");
        User authenticatedUser = new User("test user", new String[]{"can run as"});
        User user = new User("run as me", new String[]{"b"}, authenticatedUser);
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor runAsRole = new RoleDescriptor("can run as", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()},
            new String[]{"run as me"});
        roleMap.put("can run as", runAsRole);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
            .put(new IndexMetaData.Builder("b")
                .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                .numberOfShards(1).numberOfReplicas(0).build(), true)
            .build());
        RoleDescriptor bRole = new RoleDescriptor("b", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("b").privileges("all").build()}, null);
        roleMap.put("b", bRole);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, "indices:a", request);
        verify(auditTrail).runAsGranted(requestId, authentication, "indices:a", request, new String[]{runAsRole.getName()});
        verify(auditTrail).accessGranted(requestId, authentication, "indices:a", request, new String[]{bRole.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNonXPackUserCannotExecuteOperationAgainstSecurityIndex() {
        RoleDescriptor role = new RoleDescriptor("all access", new String[]{"all"},
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("*").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("all_access_user", "all_access"));
        roleMap.put("all_access", role);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
            .put(new IndexMetaData.Builder(SECURITY_INDEX_NAME)
                .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                .numberOfShards(1).numberOfReplicas(0).build(), true)
            .build());
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(BulkAction.NAME + "[s]",
            new DeleteRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(UpdateAction.NAME,
            new UpdateRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(BulkAction.NAME + "[s]",
            new IndexRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
            new TermVectorsRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
            new TermVectorsRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest()
            .addAliasAction(AliasActions.add().alias("security_alias").index(SECURITY_INDEX_NAME))));
        requests.add(
            new Tuple<>(UpdateSettingsAction.NAME, new UpdateSettingsRequest().indices(SECURITY_INDEX_NAME)));

        for (Tuple<String, TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            assertThrowsAuthorizationException(
                () -> authorize(authentication, action, request),
                action, "all_access_user");
            verify(auditTrail).accessDenied(requestId, authentication, action, request, new String[]{role.getName()});
            verifyNoMoreInteractions(auditTrail);
        }

        // we should allow waiting for the health of the index or any index if the user has this permission
        ClusterHealthRequest request = new ClusterHealthRequest(SECURITY_INDEX_NAME);
        authorize(authentication, ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, ClusterHealthAction.NAME, request, new String[]{role.getName()});

        // multiple indices
        request = new ClusterHealthRequest(SECURITY_INDEX_NAME, "foo", "bar");
        authorize(authentication, ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(requestId, authentication, ClusterHealthAction.NAME, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);

        final SearchRequest searchRequest = new SearchRequest("_all");
        authorize(authentication, SearchAction.NAME, searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_LIST, Arrays.asList(searchRequest.indices()));
    }

    public void testGrantedNonXPackUserCanExecuteMonitoringOperationsAgainstSecurityIndex() {
        RoleDescriptor role = new RoleDescriptor("all access", new String[]{"all"},
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("*").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("all_access_user", "all_access"));
        roleMap.put("all_access", role);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
            .put(new IndexMetaData.Builder(SECURITY_INDEX_NAME)
                .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                .numberOfShards(1).numberOfReplicas(0).build(), true)
            .build());
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        List<Tuple<String, ? extends TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(IndicesStatsAction.NAME, new IndicesStatsRequest().indices(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(RecoveryAction.NAME, new RecoveryRequest().indices(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesSegmentsAction.NAME, new IndicesSegmentsRequest().indices(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(GetSettingsAction.NAME, new GetSettingsRequest().indices(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(IndicesShardStoresAction.NAME,
            new IndicesShardStoresRequest().indices(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(UpgradeStatusAction.NAME,
            new UpgradeStatusRequest().indices(SECURITY_INDEX_NAME)));

        for (final Tuple<String, ? extends TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            authorize(authentication, action, request);
            verify(auditTrail).accessGranted(requestId, authentication, action, request, new String[]{role.getName()});
        }
    }

    public void testSuperusersCanExecuteOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
            .put(new IndexMetaData.Builder(SECURITY_INDEX_NAME)
                .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                .numberOfShards(1).numberOfReplicas(0).build(), true)
            .build());
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(DeleteAction.NAME,
            new DeleteRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(BulkAction.NAME + "[s]",
            createBulkShardRequest(SECURITY_INDEX_NAME, DeleteRequest::new)));
        requests.add(new Tuple<>(UpdateAction.NAME,
            new UpdateRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndexAction.NAME,
            new IndexRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(BulkAction.NAME + "[s]",
            createBulkShardRequest(SECURITY_INDEX_NAME, IndexRequest::new)));
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
            new TermVectorsRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(GetAction.NAME, new GetRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(TermVectorsAction.NAME,
            new TermVectorsRequest(SECURITY_INDEX_NAME, "type", "id")));
        requests.add(new Tuple<>(IndicesAliasesAction.NAME, new IndicesAliasesRequest()
            .addAliasAction(AliasActions.add().alias("security_alias").index(SECURITY_INDEX_NAME))));
        requests.add(new Tuple<>(ClusterHealthAction.NAME, new ClusterHealthRequest(SECURITY_INDEX_NAME)));
        requests.add(new Tuple<>(ClusterHealthAction.NAME,
            new ClusterHealthRequest(SECURITY_INDEX_NAME, "foo", "bar")));

        for (final Tuple<String, TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            final Authentication authentication = createAuthentication(superuser);
            authorize(authentication, action, request);
            verify(auditTrail).accessGranted(requestId, authentication, action, request, superuser.roles());
        }
    }

    public void testSuperusersCanExecuteOperationAgainstSecurityIndexWithWildcard() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        final Authentication authentication = createAuthentication(superuser);
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.builder()
            .put(new IndexMetaData.Builder(SECURITY_INDEX_NAME)
                .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                .numberOfShards(1).numberOfReplicas(0).build(), true)
            .build());
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        String action = SearchAction.NAME;
        SearchRequest request = new SearchRequest("_all");
        authorize(createAuthentication(superuser), action, request);
        verify(auditTrail).accessGranted(requestId, authentication, action, request, superuser.roles());
        assertThat(request.indices(), arrayContaining(".security"));
    }

    public void testAnonymousRolesAreAppliedToOtherUsers() {
        TransportRequest request = new ClusterHealthRequest();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(settings, rolesStore, clusterService, auditTrail,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role", new String[]{"all"},
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null));
        mockEmptyMetaData();
        AuditUtil.getOrGenerateRequestId(threadContext);

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
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, anonymousUser);
        roleMap.put("anonymous_user_role", new RoleDescriptor("anonymous_user_role", new String[]{"all"},
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null));
        mockEmptyMetaData();
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        authorizationService.roles(new User("no role user"), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(Arrays.asList(roles.names()), hasItem("anonymous_user_role"));
    }

    public void testCompositeActionsAreImmediatelyRejected() {
        //if the user has no permission for composite actions against any index, the request fails straight-away in the main action
        final Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        final String action = compositeRequest.v1();
        final TransportRequest request = compositeRequest.v2();
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        final RoleDescriptor role = new RoleDescriptor("no_indices", null, null, null);
        roleMap.put("no_indices", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, action, request), action, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, action, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsIndicesAreNotChecked() {
        //if the user has permission for some index, the request goes through without looking at the indices, they will be checked later
        final Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        final String action = compositeRequest.v1();
        final TransportRequest request = compositeRequest.v2();
        final Authentication authentication = createAuthentication(new User("test user", "role"));
        final RoleDescriptor role = new RoleDescriptor("role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build()},
            null);
        roleMap.put("role", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, action, request);
        verify(auditTrail).accessGranted(requestId, authentication, action, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsMustImplementCompositeIndicesRequest() {
        String action = randomCompositeRequest().v1();
        TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("test user", "role");
        roleMap.put("role", new RoleDescriptor("role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build()},
            null));
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> authorize(createAuthentication(user), action, request));
        assertThat(illegalStateException.getMessage(), containsString("Composite actions must implement CompositeIndicesRequest"));
    }

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() {
        final MockIndicesRequest mockRequest = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        final TransportRequest request;
        final String action;
        switch (randomIntBetween(0, 4)) {
            case 0:
                action = MultiGetAction.NAME + "[shard]";
                request = mockRequest;
                break;
            case 1:
                //reindex, msearch, search template, and multi search template delegate to search
                action = SearchAction.NAME;
                request = mockRequest;
                break;
            case 2:
                action = MultiTermVectorsAction.NAME + "[shard]";
                request = mockRequest;
                break;
            case 3:
                action = BulkAction.NAME + "[s]";
                request = createBulkShardRequest("index", IndexRequest::new);
                break;
            case 4:
                action = "indices:data/read/mpercolate[s]";
                request = mockRequest;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        logger.info("--> action: {}", action);

        User userAllowed = new User("userAllowed", "roleAllowed");
        roleMap.put("roleAllowed", new RoleDescriptor("roleAllowed", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("index").privileges("all").build()}, null));
        User userDenied = new User("userDenied", "roleDenied");
        roleMap.put("roleDenied", new RoleDescriptor("roleDenied", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null));
        AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetaData();
        authorize(createAuthentication(userAllowed), action, request);
        assertThrowsAuthorizationException(
            () -> authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    public void testAuthorizationOfIndividualBulkItems() {
        final String action = BulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new DeleteRequest("concrete-index", "doc", "c1")),
            new BulkItemRequest(2, new IndexRequest("concrete-index", "doc", "c2")),
            new BulkItemRequest(3, new DeleteRequest("alias-1", "doc", "a1a")),
            new BulkItemRequest(4, new IndexRequest("alias-1", "doc", "a1b")),
            new BulkItemRequest(5, new DeleteRequest("alias-2", "doc", "a2a")),
            new BulkItemRequest(6, new IndexRequest("alias-2", "doc", "a2b"))
        };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final TransportRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

        final Authentication authentication = createAuthentication(new User("user", "my-role"));
        RoleDescriptor role = new RoleDescriptor("my-role", null, new IndicesPrivileges[]{
            IndicesPrivileges.builder().indices("concrete-index").privileges("all").build(),
            IndicesPrivileges.builder().indices("alias-1").privileges("index").build(),
            IndicesPrivileges.builder().indices("alias-2").privileges("delete").build()
        }, null);
        roleMap.put("my-role", role);

        mockEmptyMetaData();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, action, request);

        verify(auditTrail).accessDenied(requestId, authentication, DeleteAction.NAME, request,
            new String[] { role.getName() }); // alias-1 delete
        verify(auditTrail).accessDenied(requestId, authentication, IndexAction.NAME, request,
            new String[] { role.getName() }); // alias-2 index
        verify(auditTrail).accessGranted(requestId, authentication, action, request,
            new String[] { role.getName() }); // bulk request is allowed
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizationOfIndividualBulkItemsWithDateMath() {
        final String action = BulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new IndexRequest("<datemath-{now/M{YYYY}}>", "doc", "dy1")),
            new BulkItemRequest(2,
                new DeleteRequest("<datemath-{now/d{YYYY}}>", "doc", "dy2")), // resolves to same as above
            new BulkItemRequest(3, new IndexRequest("<datemath-{now/M{YYYY.MM}}>", "doc", "dm1")),
            new BulkItemRequest(4,
                new DeleteRequest("<datemath-{now/d{YYYY.MM}}>", "doc", "dm2")), // resolves to same as above
        };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final TransportRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

        final Authentication authentication = createAuthentication(new User("user", "my-role"));
        final RoleDescriptor role = new RoleDescriptor("my-role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("datemath-*").privileges("index").build()}, null);
        roleMap.put("my-role", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        mockEmptyMetaData();
        authorize(authentication, action, request);

        // both deletes should fail
        verify(auditTrail, Mockito.times(2)).accessDenied(requestId, authentication, DeleteAction.NAME, request,
            new String[]{role.getName()});
        // bulk request is allowed
        verify(auditTrail).accessGranted(requestId, authentication, action, request, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    private BulkShardRequest createBulkShardRequest(String indexName, TriFunction<String, String, String, DocWriteRequest<?>> req) {
        final BulkItemRequest[] items = {new BulkItemRequest(1, req.apply(indexName, "type", "id"))};
        return new BulkShardRequest(new ShardId(indexName, UUID.randomUUID().toString(), 1),
            WriteRequest.RefreshPolicy.IMMEDIATE, items);
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
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowNonMatchingUsername() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(user);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        // this should still fail since the username is still different
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest) request).username("joe");
        } else {
            ((AuthenticateRequest) request).username("joe");
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
        final boolean runAs = randomBoolean();
        when(authentication.getUser()).thenReturn(user);
        when(user.authenticatedUser()).thenReturn(runAs ? new User("authUser") : user);
        when(user.isRunAs()).thenReturn(runAs);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType())
            .thenReturn(randomAlphaOfLengthBetween(4, 12));

        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verifyZeroInteractions(user, request, authentication);
    }

    public void testSameUserPermissionRunAsChecksAuthenticatedBy() {
        final User authUser = new User("admin", new String[]{"bar"});
        final String username = "joe";
        final User user = new User(username, null, authUser);
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
            new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
            new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType())
            .thenReturn(changePasswordRequest ? randomFrom(ReservedRealm.TYPE, NativeRealmSettings.TYPE) :
                randomAlphaOfLengthBetween(4, 12));
        assertTrue(AuthorizationService.checkSameUserPermissions(action, request, authentication));

        when(authentication.getUser()).thenReturn(authUser);
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForOtherRealms() {
        final User user = new User("joe");
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authenticatedBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authenticatedBy).getType();
        verify(authentication).getAuthenticatedBy();
        verify(authentication, times(2)).getUser();
        verifyNoMoreInteractions(authenticatedBy, authentication);
    }

    public void testSameUserPermissionDoesNotAllowChangePasswordForLookedUpByOtherRealms() {
        final User authUser = new User("admin", new String[]{"bar"});
        final User user = new User("joe", null, authUser);
        final ChangePasswordRequest request = new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = ChangePasswordAction.NAME;
        final Authentication authentication = mock(Authentication.class);
        final RealmRef authenticatedBy = mock(RealmRef.class);
        final RealmRef lookedUpBy = mock(RealmRef.class);
        when(authentication.getUser()).thenReturn(user);
        when(authentication.getAuthenticatedBy()).thenReturn(authenticatedBy);
        when(authentication.getLookedUpBy()).thenReturn(lookedUpBy);
        when(lookedUpBy.getType()).thenReturn(randomFrom(LdapRealmSettings.LDAP_TYPE, FileRealmSettings.TYPE,
            LdapRealmSettings.AD_TYPE, PkiRealmSettings.TYPE,
            randomAlphaOfLengthBetween(4, 12)));

        assertThat(request, instanceOf(UserRequest.class));
        assertFalse(AuthorizationService.checkSameUserPermissions(action, request, authentication));
        verify(authentication).getLookedUpBy();
        verify(authentication, times(2)).getUser();
        verify(lookedUpBy).getType();
        verifyNoMoreInteractions(authentication, lookedUpBy, authenticatedBy);
    }

    private static Tuple<String, TransportRequest> randomCompositeRequest() {
        switch (randomIntBetween(0, 7)) {
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
        assertThat(roles, equalTo(XPackUser.ROLE));
        verifyZeroInteractions(rolesStore);
    }

    public void testGetRolesForSystemUserThrowsException() {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> authorizationService.roles(SystemUser.INSTANCE,
            null));
        assertEquals("the user [_system] is the system user and we should never try to get its roles", iae.getMessage());
    }

    private static Authentication createAuthentication(User user) {
        RealmRef lookedUpBy = user.authenticatedUser() == user ? null : new RealmRef("looked", "up", "by");
        return new Authentication(user, new RealmRef("test", "test", "foo"), lookedUpBy);
    }

    private ClusterState mockEmptyMetaData() {
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metaData()).thenReturn(MetaData.EMPTY_META_DATA);
        return state;
    }

    public void testProxyRequestFailsOnNonProxyAction() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, request);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("test user", "role");
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> authorize(createAuthentication(user), "indices:some/action", transportRequest));
        assertThat(illegalStateException.getMessage(),
            startsWith("originalRequest is a proxy request for: [org.elasticsearch.transport.TransportRequest$"));
        assertThat(illegalStateException.getMessage(), endsWith("] but action: [indices:some/action] isn't"));
    }

    public void testProxyRequestFailsOnNonProxyRequest() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        User user = new User("test user", "role");
        AuditUtil.getOrGenerateRequestId(threadContext);
        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> authorize(createAuthentication(user), TransportActionProxy.getProxyAction("indices:some/action"), request));
        assertThat(illegalStateException.getMessage(),
            startsWith("originalRequest is not a proxy request: [org.elasticsearch.transport.TransportRequest$"));
        assertThat(illegalStateException.getMessage(),
            endsWith("] but action: [internal:transport/proxy/indices:some/action] is a proxy action"));
    }

    public void testProxyRequestAuthenticationDenied() {
        final TransportRequest proxiedRequest = new SearchRequest();
        final DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, proxiedRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.QUERY_ACTION_NAME);
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        final RoleDescriptor role = new RoleDescriptor("no_indices", null, null, null);
        roleMap.put("no_indices", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, action, proxiedRequest, new String[]{role.getName()});
        verifyNoMoreInteractions(auditTrail);
    }

    public void testProxyRequestAuthenticationGrantedWithAllPrivileges() {
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("all").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(authentication, action, transportRequest);
        verify(auditTrail).accessGranted(requestId, authentication, action, clearScrollRequest, new String[]{role.getName()});
    }

    public void testProxyRequestAuthenticationGranted() {
        RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("read_cross_cluster").build()}, null);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(authentication, action, transportRequest);
        verify(auditTrail).accessGranted(requestId, authentication, action, clearScrollRequest, new String[]{role.getName()});
    }

    public void testProxyRequestAuthenticationDeniedWithReadPrivileges() {
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final RoleDescriptor role = new RoleDescriptor("a_role", null,
            new IndicesPrivileges[]{IndicesPrivileges.builder().indices("a").privileges("read").build()}, null);
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetaData();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(requestId, authentication, action, clearScrollRequest, new String[]{role.getName()});
    }
}
