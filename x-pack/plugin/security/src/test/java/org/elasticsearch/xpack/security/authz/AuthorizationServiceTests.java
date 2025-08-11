/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.MappingUpdatePerformer;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
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
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.ParsedScrollId;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ActionClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAs;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuthorizationServiceTests extends ESTestCase {
    private AuditTrail auditTrail;
    private AuditTrailService auditTrailService;
    private ClusterService clusterService;
    private AuthorizationService authorizationService;
    private ThreadContext threadContext;
    private ThreadPool threadPool;
    private Map<String, RoleDescriptor> roleMap = new HashMap<>();
    private CompositeRolesStore rolesStore;
    private FieldPermissionsCache fieldPermissionsCache;
    private OperatorPrivileges.OperatorPrivilegesService operatorPrivilegesService;
    private boolean shouldFailOperatorPrivilegesCheck = false;
    private boolean setFakeOriginatingAction = true;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        final Settings settings = Settings.builder().put("cluster.remote.other_cluster.seeds", "localhost:9999").build();
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, LoadAuthorizedIndicesTimeChecker.Factory.getSettings())
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        auditTrail = mock(AuditTrail.class);
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        auditTrailService = new AuditTrailService(Collections.singletonList(auditTrail), licenseState);
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
        }).when(privilegesStore).getPrivileges(any(Collection.class), any(Collection.class), anyActionListener());

        final Map<Set<String>, Role> roleCache = new HashMap<>();
        doAnswer((i) -> {
            ActionListener<Role> callback = (ActionListener<Role>) i.getArguments()[2];
            User user = (User) i.getArguments()[0];
            buildRole(user, privilegesStore, fieldPermissionsCache, roleCache, callback);
            return null;
        }).when(rolesStore).getRoles(any(User.class), any(Authentication.class), anyActionListener());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        operatorPrivilegesService = mock(OperatorPrivileges.OperatorPrivilegesService.class);
        authorizationService = new AuthorizationService(
            settings,
            rolesStore,
            fieldPermissionsCache,
            clusterService,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            new AnonymousUser(settings),
            null,
            Collections.emptySet(),
            licenseState,
            TestIndexNameExpressionResolver.newInstance(),
            operatorPrivilegesService
        );
    }

    private void buildRole(
        User user,
        NativePrivilegeStore privilegesStore,
        FieldPermissionsCache fieldPermissionsCache,
        Map<Set<String>, Role> roleCache,
        ActionListener<Role> listener
    ) {
        final Set<String> names = org.elasticsearch.core.Set.of(user.roles());
        assertNotNull(names);

        // We need a cache here because CompositeRoleStore has one, and our tests rely on it
        // (to check that nested calls use the same object)
        final Role cachedRole = roleCache.get(names);
        if (cachedRole != null) {
            listener.onResponse(cachedRole);
            return;
        }

        Set<RoleDescriptor> roleDescriptors = new HashSet<>();
        for (String name : names) {
            RoleDescriptor descriptor = roleMap.get(name);
            if (descriptor != null) {
                roleDescriptors.add(descriptor);
            }
        }

        if (roleDescriptors.isEmpty()) {
            listener.onResponse(Role.EMPTY);
        } else {
            CompositeRolesStore.buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, privilegesStore, ActionListener.wrap(r -> {
                roleCache.put(names, r);
                listener.onResponse(r);
            }, listener::onFailure));
        }
    }

    private void authorize(Authentication authentication, String action, TransportRequest request) {
        authorize(authentication, action, request, true, null);
    }

    private void authorize(
        Authentication authentication,
        String action,
        TransportRequest request,
        boolean expectCleanThreadContext,
        Runnable listenerBody
    ) {
        PlainActionFuture<Object> done = new PlainActionFuture<>();
        PlainActionFuture<IndicesAccessControl> indicesPermissions = new PlainActionFuture<>();
        PlainActionFuture<Object> originatingAction = new PlainActionFuture<>();
        PlainActionFuture<Object> authorizationInfo = new PlainActionFuture<>();
        String someRandomHeader = "test_" + UUIDs.randomBase64UUID();
        Object someRandomHeaderValue = mock(Object.class);
        threadContext.putTransient(someRandomHeader, someRandomHeaderValue);
        // the thread context before authorization could contain any of the transient headers
        IndicesAccessControl mockAccessControlHeader = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
        if (mockAccessControlHeader == null && randomBoolean()) {
            mockAccessControlHeader = mock(IndicesAccessControl.class);
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, mockAccessControlHeader);
        }
        String originatingActionHeader = threadContext.getTransient(ORIGINATING_ACTION_KEY);
        if (this.setFakeOriginatingAction) {
            if (originatingActionHeader == null && randomBoolean()) {
                originatingActionHeader = randomAlphaOfLength(8);
                threadContext.putTransient(ORIGINATING_ACTION_KEY, originatingActionHeader);
            }
        }
        AuthorizationInfo authorizationInfoHeader = threadContext.getTransient(AUTHORIZATION_INFO_KEY);
        if (authorizationInfoHeader == null)
            // If we have an originating action, we must also have origination authz info
            if (originatingActionHeader != null || randomBoolean()) {
                authorizationInfoHeader = mock(AuthorizationInfo.class);
                threadContext.putTransient(AUTHORIZATION_INFO_KEY, authorizationInfoHeader);
            }
        Mockito.reset(operatorPrivilegesService);
        final AtomicBoolean operatorPrivilegesChecked = new AtomicBoolean(false);
        final ElasticsearchSecurityException operatorPrivilegesException = new ElasticsearchSecurityException(
            "Operator privileges check failed"
        );
        if (shouldFailOperatorPrivilegesCheck) {
            when(operatorPrivilegesService.check(authentication, action, request, threadContext)).thenAnswer(invocationOnMock -> {
                operatorPrivilegesChecked.set(true);
                return operatorPrivilegesException;
            });
        }
        ActionListener<Void> listener = ActionListener.wrap(response -> {
            // extract the authorization transient headers from the thread context of the action
            // that has been authorized
            originatingAction.onResponse(threadContext.getTransient(ORIGINATING_ACTION_KEY));
            authorizationInfo.onResponse(threadContext.getTransient(AUTHORIZATION_INFO_KEY));
            indicesPermissions.onResponse(threadContext.getTransient(INDICES_PERMISSIONS_KEY));

            assertNull(verify(operatorPrivilegesService).check(authentication, action, request, threadContext));

            if (listenerBody != null) {
                listenerBody.run();
            }
            done.onResponse(threadContext.getTransient(someRandomHeader));
        }, e -> {
            if (shouldFailOperatorPrivilegesCheck && operatorPrivilegesChecked.get()) {
                assertSame(operatorPrivilegesException, e.getCause());
            }
            done.onFailure(e);
        });
        authorizationService.authorize(authentication, action, request, listener);
        Object someRandomHeaderValueInListener = done.actionGet();
        assertThat(someRandomHeaderValueInListener, sameInstance(someRandomHeaderValue));
        assertThat(threadContext.getTransient(someRandomHeader), sameInstance(someRandomHeaderValue));
        // authorization restores any previously existing transient headers
        if (mockAccessControlHeader != null) {
            assertThat(threadContext.getTransient(INDICES_PERMISSIONS_KEY), sameInstance(mockAccessControlHeader));
        } else {
            assertThat(threadContext.getTransient(INDICES_PERMISSIONS_KEY), nullValue());
        }
        if (originatingActionHeader != null) {
            assertThat(threadContext.getTransient(ORIGINATING_ACTION_KEY), sameInstance(originatingActionHeader));
        } else {
            assertThat(threadContext.getTransient(ORIGINATING_ACTION_KEY), nullValue());
        }
        if (authorizationInfoHeader != null) {
            assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), sameInstance(authorizationInfoHeader));
        } else {
            assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), nullValue());
        }
        if (expectCleanThreadContext) {
            // but the authorization listener observes the authorization-resulting headers, which are different
            if (mockAccessControlHeader != null) {
                assertThat(indicesPermissions.actionGet(), not(sameInstance(mockAccessControlHeader)));
            }
            if (authorizationInfoHeader != null) {
                assertThat(authorizationInfo.actionGet(), not(sameInstance(authorizationInfoHeader)));
            }
        }
        // except originating action, which is not overwritten
        if (originatingActionHeader != null) {
            assertThat(originatingAction.actionGet(), sameInstance(originatingActionHeader));
        }
    }

    public void testActionsForSystemUserIsAuthorized() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        // A failure would throw an exception
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String[] actions = {
            "indices:monitor/whatever",
            "internal:whatever",
            "cluster:monitor/whatever",
            "cluster:admin/reroute",
            "indices:admin/mapping/put",
            "indices:admin/template/put",
            "indices:admin/seq_no/global_checkpoint_sync",
            "indices:admin/seq_no/retention_lease_sync",
            "indices:admin/seq_no/retention_lease_background_sync",
            "indices:admin/seq_no/add_retention_lease",
            "indices:admin/seq_no/remove_retention_lease",
            "indices:admin/seq_no/renew_retention_lease",
            "indices:admin/settings/update" };
        for (String action : actions) {
            authorize(authentication, action, request);
            verify(auditTrail).accessGranted(
                eq(requestId),
                eq(authentication),
                eq(action),
                eq(request),
                authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
            );
        }

        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizationForSecurityChange() {
        final Authentication authentication = createAuthentication(new User("user", "manage_security_role"));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        RoleDescriptor role = new RoleDescriptor(
            "manage_security_role",
            new String[] { ClusterPrivilegeResolver.MANAGE_SECURITY.name() },
            null,
            null,
            null,
            null,
            null,
            null
        );
        roleMap.put("manage_security_role", role);
        for (String action : LoggingAuditTrail.SECURITY_CHANGE_ACTIONS) {
            TransportRequest request = mock(TransportRequest.class);
            authorize(authentication, action, request);
            verify(auditTrail).accessGranted(
                eq(requestId),
                eq(authentication),
                eq(action),
                eq(request),
                authzInfoRoles(new String[] { role.getName() })
            );
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testIndicesActionsForSystemUserWhichAreNotAuthorized() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:", request),
            "indices:",
            SystemUser.INSTANCE.principal()
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:"),
            eq(request),
            authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminActionsForSystemUserWhichAreNotAuthorized() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/whatever", request),
            "cluster:admin/whatever",
            SystemUser.INSTANCE.principal()
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("cluster:admin/whatever"),
            eq(request),
            authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testClusterAdminSnapshotStatusActionForSystemUserWhichIsNotAuthorized() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(SystemUser.INSTANCE);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/snapshot/status", request),
            "cluster:admin/snapshot/status",
            SystemUser.INSTANCE.principal()
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("cluster:admin/snapshot/status"),
            eq(request),
            authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeUsingConditionalPrivileges() throws IOException {
        final DeletePrivilegesRequest request = new DeletePrivilegesRequest();
        final Authentication authentication = createAuthentication(new User("user1", "role1"));

        final ConfigurableClusterPrivilege configurableClusterPrivilege = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                final Predicate<TransportRequest> requestPredicate = r -> r == request;
                builder.add(
                    this,
                    ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    requestPredicate
                );
                return builder;
            }
        };
        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = new ConfigurableClusterPrivilege[] {
            configurableClusterPrivilege };
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        RoleDescriptor role = new RoleDescriptor("role1", null, null, null, configurableClusterPrivileges, null, null, null);
        roleMap.put("role1", role);

        authorize(authentication, DeletePrivilegesAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(DeletePrivilegesAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizationDeniedWhenConditionalPrivilegesDoNotMatch() throws IOException {
        final DeletePrivilegesRequest request = new DeletePrivilegesRequest();
        final Authentication authentication = createAuthentication(new User("user1", "role1"));

        final ConfigurableClusterPrivilege configurableClusterPrivilege = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                final Predicate<TransportRequest> requestPredicate = r -> false;
                builder.add(
                    this,
                    ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    requestPredicate
                );
                return builder;
            }
        };
        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = new ConfigurableClusterPrivilege[] {
            configurableClusterPrivilege };
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        RoleDescriptor role = new RoleDescriptor("role1", null, null, null, configurableClusterPrivileges, null, null, null);
        roleMap.put("role1", role);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, DeletePrivilegesAction.NAME, request),
            DeletePrivilegesAction.NAME,
            "user1"
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(DeletePrivilegesAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testNoRolesCausesDenial() throws IOException {
        final TransportRequest request = new SearchRequest();
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(() -> authorize(authentication, "indices:a", request), "indices:a", "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCanPerformRemoteSearch() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("other_cluster:index1", "*_cluster:index2", "other_cluster:other_*");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, SearchAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchAction.NAME),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesPerformsRemoteSearchWithScroll() {
        final ParsedScrollId parsedScrollId = mock(ParsedScrollId.class);
        final SearchScrollRequest searchScrollRequest = mock(SearchScrollRequest.class);
        when(searchScrollRequest.parseScrollId()).thenReturn(parsedScrollId);
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        for (final boolean hasLocalIndices : org.elasticsearch.core.List.of(true, false)) {
            when(parsedScrollId.hasLocalIndices()).thenReturn(hasLocalIndices);
            if (hasLocalIndices) {
                assertThrowsAuthorizationException(
                    () -> authorize(authentication, SearchScrollAction.NAME, searchScrollRequest),
                    "indices:data/read/scroll",
                    "test user"
                );
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(authentication),
                    eq("indices:data/read/scroll"),
                    eq(searchScrollRequest),
                    authzInfoRoles(Role.EMPTY.names())
                );
            } else {
                authorize(authentication, SearchScrollAction.NAME, searchScrollRequest);
                verify(auditTrail).accessGranted(
                    eq(requestId),
                    eq(authentication),
                    eq(SearchScrollAction.NAME),
                    eq(searchScrollRequest),
                    authzInfoRoles(Role.EMPTY.names())
                );
            }
            verifyNoMoreInteractions(auditTrail);
        }
    }

    /**
     * This test mimics {@link #testUserWithNoRolesCanPerformRemoteSearch()} except that
     * while the referenced index _looks_ like a remote index, the remote cluster name has not
     * been defined, so it is actually a local index and access should be denied
     */
    public void testUserWithNoRolesCannotPerformLocalSearch() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("no_such_cluster:index");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(() -> authorize(authentication, SearchAction.NAME, request), SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(SearchAction.NAME),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    /**
     * This test mimics {@link #testUserWithNoRolesCannotPerformLocalSearch()} but includes
     * both local and remote indices, including wildcards
     */
    public void testUserWithNoRolesCanPerformMultiClusterSearch() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("local_index", "wildcard_*", "other_cluster:remote_index", "*:foo?");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(() -> authorize(authentication, SearchAction.NAME, request), SearchAction.NAME, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(SearchAction.NAME),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCannotSql() throws IOException {
        TransportRequest request = new SqlQueryRequest();
        Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(() -> authorize(authentication, SqlQueryAction.NAME, request), SqlQueryAction.NAME, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(SqlQueryAction.NAME),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    /**
     * Verifies that the behaviour tested in {@link #testUserWithNoRolesCanPerformRemoteSearch}
     * does not work for requests that are not remote-index-capable.
     */
    public void testRemoteIndicesOnlyWorkWithApplicableRequestTypes() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices("other_cluster:index1", "other_cluster:index2");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, DeleteIndexAction.NAME, request),
            DeleteIndexAction.NAME,
            "test user"
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(DeleteIndexAction.NAME),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesOpenPointInTimeWithRemoteIndices() {
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        for (final boolean hasLocalIndices : org.elasticsearch.core.List.of(true, false)) {
            final String[] indices = new String[] {
                hasLocalIndices
                    ? randomAlphaOfLength(5)
                    : "other_cluster:" + randomFrom(randomAlphaOfLength(5), "*", randomAlphaOfLength(4) + "*"),
                "other_cluster:" + randomFrom(randomAlphaOfLength(5), "*", randomAlphaOfLength(4) + "*") };
            final OpenPointInTimeRequest openPointInTimeRequest = new OpenPointInTimeRequest(indices).keepAlive(
                TimeValue.timeValueMinutes(randomLongBetween(1, 10))
            );
            if (randomBoolean()) {
                openPointInTimeRequest.routing(randomAlphaOfLength(5));
            }
            if (randomBoolean()) {
                openPointInTimeRequest.preference(randomAlphaOfLength(5));
            }
            if (hasLocalIndices) {
                assertThrowsAuthorizationException(
                    () -> authorize(authentication, OpenPointInTimeAction.NAME, openPointInTimeRequest),
                    "indices:data/read/open_point_in_time",
                    "test user"
                );
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(authentication),
                    eq("indices:data/read/open_point_in_time"),
                    eq(openPointInTimeRequest),
                    authzInfoRoles(Role.EMPTY.names())
                );
            } else {
                authorize(authentication, OpenPointInTimeAction.NAME, openPointInTimeRequest);
                verify(auditTrail).accessGranted(
                    eq(requestId),
                    eq(authentication),
                    eq("indices:data/read/open_point_in_time"),
                    eq(openPointInTimeRequest),
                    authzInfoRoles(Role.EMPTY.names())
                );
            }
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testUserWithNoRolesCanClosePointInTime() {
        final ClosePointInTimeRequest closePointInTimeRequest = new ClosePointInTimeRequest(randomAlphaOfLength(8));
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, ClosePointInTimeAction.NAME, closePointInTimeRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:data/read/close_point_in_time"),
            eq(closePointInTimeRequest),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() throws IOException {
        Tuple<String, TransportRequest> tuple = randomFrom(
            asList(new Tuple<>(SearchAction.NAME, new SearchRequest()), new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest()))
        );
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        final Authentication authentication = createAuthentication(new User("test user", "non-existent-role"));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, action, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString("[" + action + "] is unauthorized" + " for user [test user]" + " with roles [non-existent-role],")
            )
        );
        assertThat(securityException, throwableWithMessage(containsString("this action is granted by the index privileges [read,all]")));

        verify(auditTrail).accessDenied(eq(requestId), eq(authentication), eq(action), eq(request), authzInfoRoles(Role.EMPTY.names()));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testServiceAccountDenial() {
        Tuple<String, TransportRequest> tuple = randomFrom(
            asList(new Tuple<>(SearchAction.NAME, new SearchRequest()), new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest()))
        );
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();

        final User serviceUser = new User(randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8));
        final Authentication authentication = new Authentication(
            serviceUser,
            new RealmRef("_service_account", "_service_account", randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            org.elasticsearch.core.Map.of()
        );
        Mockito.reset(rolesStore);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[2];
            listener.onResponse(Role.EMPTY);
            return null;
        }).when(rolesStore).getRoles(any(User.class), any(Authentication.class), anyActionListener());

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, action, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("[" + action + "] is unauthorized" + " for user [" + serviceUser.principal() + "],"))
        );
        assertThat(securityException, throwableWithMessage(containsString("this action is granted by the index privileges [read,all]")));
        verify(auditTrail).accessDenied(eq(requestId), eq(authentication), eq(action), eq(request), authzInfoRoles(Role.EMPTY.names()));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        roleMap.put("a_all", role);

        assertThrowsAuthorizationException(() -> authorize(authentication, "whatever", request), "whatever", "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("whatever"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatRoleWithNoIndicesIsDenied() throws IOException {
        Tuple<String, TransportRequest> tuple = randomFrom(
            new Tuple<>(SearchAction.NAME, new SearchRequest()),
            new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())
        );
        final String action = tuple.v1();
        final TransportRequest request = tuple.v2();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        RoleDescriptor role = new RoleDescriptor("no_indices", null, null, null);
        roleMap.put("no_indices", role);
        mockEmptyMetadata();

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, action, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("[" + action + "] is unauthorized" + " for user [test user]" + " with roles [no_indices],"))
        );
        assertThat(securityException, throwableWithMessage(containsString("this action is granted by the index privileges [read,all]")));

        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testElasticUserAuthorizedForNonChangePasswordRequestsWhenNotInSetupMode() throws IOException {
        final Authentication authentication = createAuthentication(new ElasticUser(true));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Tuple<String, TransportRequest> request = randomCompositeRequest();
        authorize(authentication, request.v1(), request.v2());

        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(request.v1()),
            eq(request.v2()),
            authzInfoRoles(new String[] { ElasticUser.ROLE_NAME })
        );
    }

    public void testSearchAgainstEmptyCluster() throws Exception {
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        roleMap.put("a_all", role);
        mockEmptyMetadata();

        {
            // ignore_unavailable set to false, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist").indicesOptions(
                IndicesOptions.fromOptions(false, true, true, false)
            );

            assertThrowsAuthorizationException(
                () -> authorize(authentication, SearchAction.NAME, searchRequest),
                SearchAction.NAME,
                "test user"
            );
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq(SearchAction.NAME),
                eq(searchRequest),
                authzInfoRoles(new String[] { role.getName() })
            );
            verifyNoMoreInteractions(auditTrail);
        }

        {
            // ignore_unavailable and allow_no_indices both set to true, user is not authorized for this index nor does it exist
            SearchRequest searchRequest = new SearchRequest("does_not_exist").indicesOptions(
                IndicesOptions.fromOptions(true, true, true, false)
            );
            final ActionListener<Void> listener = ActionListener.wrap(ignore -> {
                final IndicesAccessControl indicesAccessControl = threadContext.getTransient(
                    AuthorizationServiceField.INDICES_PERMISSIONS_KEY
                );
                assertNotNull(indicesAccessControl);
                final IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(
                    IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER
                );
                assertFalse(indexAccessControl.getFieldPermissions().hasFieldLevelSecurity());
                assertFalse(indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions());
            }, e -> { fail(e.getMessage()); });
            final CountDownLatch latch = new CountDownLatch(1);
            authorizationService.authorize(authentication, SearchAction.NAME, searchRequest, new LatchedActionListener<>(listener, latch));
            latch.await();
            verify(auditTrail).accessGranted(
                eq(requestId),
                eq(authentication),
                eq(SearchAction.NAME),
                eq(searchRequest),
                authzInfoRoles(new String[] { role.getName() })
            );
        }
    }

    public void testSearchAgainstIndex() throws Exception {
        RoleDescriptor role = new RoleDescriptor(
            "search_index",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index-*").privileges("read").build() },
            null
        );
        roleMap.put(role.getName(), role);
        final User user = new User("test search user", role.getName());
        final Authentication authentication = createAuthentication(user);

        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final String indexName = "index-" + randomAlphaOfLengthBetween(1, 5);

        final ClusterState clusterState = mockMetadataWithIndex(indexName);
        final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);

        SearchRequest searchRequest = new SearchRequest(indexName).allowPartialSearchResults(false);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            new OriginalIndices(searchRequest),
            searchRequest,
            new ShardId(indexMetadata.getIndex(), 0),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            System.currentTimeMillis(),
            null
        );
        this.setFakeOriginatingAction = false;
        authorize(authentication, SearchAction.NAME, searchRequest, true, () -> {
            verify(rolesStore).getRoles(Mockito.same(user), Mockito.same(authentication), Mockito.any());
            IndicesAccessControl iac = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            // Within the action handler, execute a child action (the query phase of search)
            authorize(authentication, SearchTransportService.QUERY_ACTION_NAME, shardRequest, false, () -> {
                // This child action triggers a second interaction with the role store (which is cached)
                verify(rolesStore, times(2)).getRoles(Mockito.same(user), Mockito.same(authentication), Mockito.any());
                // But it does not create a new IndicesAccessControl
                assertThat(threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY), sameInstance(iac));
            });
        });
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchAction.NAME),
            eq(searchRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.QUERY_ACTION_NAME),
            eq(shardRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testScrollRelatedRequestsAllowed() {
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        authorize(authentication, ClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(ClearScrollAction.NAME),
            eq(clearScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );

        final ParsedScrollId parsedScrollId = mock(ParsedScrollId.class);
        when(parsedScrollId.hasLocalIndices()).thenReturn(true);
        final SearchScrollRequest searchScrollRequest = mock(SearchScrollRequest.class);
        when(searchScrollRequest.parseScrollId()).thenReturn(parsedScrollId);
        authorize(authentication, SearchScrollAction.NAME, searchScrollRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchScrollAction.NAME),
            eq(searchScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );

        // We have to use a mock request for other Scroll actions as the actual requests are package private to SearchTransportService
        final TransportRequest request = mock(TransportRequest.class);
        authorize(authentication, SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        authorize(authentication, SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        authorize(authentication, SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        authorize(authentication, SearchTransportService.QUERY_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.QUERY_SCROLL_ACTION_NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        authorize(authentication, SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizeIndicesFailures() throws IOException {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetadata();
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(() -> authorize(authentication, "indices:a", request), "indices:a", "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metadata();
    }

    public void testCreateIndexWithAliasWithoutPermissions() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetadata();
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(
            () -> authorize(authentication, CreateIndexAction.NAME, request),
            IndicesAliasesAction.NAME,
            "test user"
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(CreateIndexAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(IndicesAliasesAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metadata();
    }

    public void testCreateIndexWithAlias() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("a");
        request.alias(new Alias("a2"));
        ClusterState state = mockEmptyMetadata();
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a", "a2").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, CreateIndexAction.NAME, request);

        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(CreateIndexAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:admin/aliases"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metadata();
    }

    public void testDenialErrorMessagesForSearchAction() throws IOException {
        RoleDescriptor indexRole = new RoleDescriptor(
            "some_indices_" + randomAlphaOfLengthBetween(3, 6),
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().indices("all*").privileges("all").build(),
                IndicesPrivileges.builder().indices("read*").privileges("read").build(),
                IndicesPrivileges.builder().indices("write*").privileges("write").build() },
            null
        );
        RoleDescriptor emptyRole = new RoleDescriptor("empty_role_" + randomAlphaOfLengthBetween(1, 4), null, null, null);
        User user = new User(randomAlphaOfLengthBetween(6, 8), indexRole.getName(), emptyRole.getName());
        final Authentication authentication = createAuthentication(user);
        roleMap.put(indexRole.getName(), indexRole);
        roleMap.put(emptyRole.getName(), emptyRole);

        AuditUtil.getOrGenerateRequestId(threadContext);

        TransportRequest request = new SearchRequest("all-1", "read-2", "write-3", "other-4");

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, SearchAction.NAME, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString(
                    "["
                        + SearchAction.NAME
                        + "] is unauthorized"
                        + " for user ["
                        + user.principal()
                        + "]"
                        + " with roles ["
                        + indexRole.getName()
                        + ","
                        + emptyRole.getName()
                        + "]"
                        + " on indices ["
                )
            )
        );
        assertThat(securityException, throwableWithMessage(containsString("write-3")));
        assertThat(securityException, throwableWithMessage(containsString("other-4")));
        assertThat(securityException, throwableWithMessage(not(containsString("all-1"))));
        assertThat(securityException, throwableWithMessage(not(containsString("read-2"))));
        assertThat(securityException, throwableWithMessage(containsString(", this action is granted by the index privileges [read,all]")));
    }

    public void testDenialErrorMessagesForBulkIngest() throws Exception {
        final String index = randomAlphaOfLengthBetween(5, 12);
        RoleDescriptor role = new RoleDescriptor(
            "some_indices_" + randomAlphaOfLengthBetween(3, 6),
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices(index).privileges(BulkAction.NAME).build() },
            null
        );
        User user = new User(randomAlphaOfLengthBetween(6, 8), role.getName());
        final Authentication authentication = createAuthentication(user);
        roleMap.put(role.getName(), role);

        AuditUtil.getOrGenerateRequestId(threadContext);

        final BulkShardRequest request = new BulkShardRequest(
            new ShardId(index, randomAlphaOfLength(24), 1),
            WriteRequest.RefreshPolicy.NONE,
            new BulkItemRequest[] {
                new BulkItemRequest(
                    0,
                    new IndexRequest(index).id("doc-1").opType(DocWriteRequest.OpType.CREATE).source(singletonMap("field", "value"))
                ),
                new BulkItemRequest(
                    1,
                    new IndexRequest(index).id("doc-2").opType(DocWriteRequest.OpType.INDEX).source(singletonMap("field", "value"))
                ),
                new BulkItemRequest(2, new DeleteRequest(index, "doc-3")) }
        );

        authorize(authentication, TransportShardBulkAction.ACTION_NAME, request);

        MappingUpdatePerformer mappingUpdater = (m, s, t, l) -> l.onResponse(null);
        Consumer<ActionListener<Void>> waitForMappingUpdate = l -> l.onResponse(null);
        PlainActionFuture<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>> future = new PlainActionFuture<>();
        IndexShard indexShard = mock(IndexShard.class);
        TransportShardBulkAction.performOnPrimary(
            request,
            indexShard,
            new UpdateHelper(mock(ScriptService.class)),
            System::currentTimeMillis,
            mappingUpdater,
            waitForMappingUpdate,
            future,
            threadPool,
            ThreadPool.Names.WRITE
        );

        TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse> result = future.get();
        BulkShardResponse response = result.finalResponseIfSuccessful;
        assertThat(response, notNullValue());
        assertThat(response.getResponses(), arrayWithSize(3));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("unauthorized for user [" + user.principal() + "]"));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("on indices [" + index + "]"));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("[create_doc,create,index,write,all]"));
        assertThat(response.getResponses()[1].getFailureMessage(), containsString("[create,index,write,all]"));
        assertThat(response.getResponses()[2].getFailureMessage(), containsString("[delete,write,all]"));
    }

    public void testDenialErrorMessagesForClusterHealthAction() throws IOException {
        RoleDescriptor role = new RoleDescriptor(
            "role_" + randomAlphaOfLengthBetween(3, 6),
            new String[0], // no cluster privileges
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index-*").privileges("all").build() },
            null
        );
        User user = new User(randomAlphaOfLengthBetween(6, 8), role.getName());
        final Authentication authentication = createAuthentication(user);
        roleMap.put(role.getName(), role);

        AuditUtil.getOrGenerateRequestId(threadContext);

        TransportRequest request = new ClusterHealthRequest();

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, ClusterHealthAction.NAME, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("[" + ClusterHealthAction.NAME + "] is unauthorized for user [" + user.principal() + "]"))
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("this action is granted by the cluster privileges [monitor,manage,all]"))
        );
    }

    /**
     * Per {@link ClusterPrivilegeResolver#isClusterAction(String)}, there are some actions that start with "indices:" but are treated as
     * cluster level actions for the purposes of security.
     * This test case checks that the error message for these actions handles this edge-case.
     */
    public void testDenialErrorMessagesForIndexTemplateAction() throws IOException {
        RoleDescriptor role = new RoleDescriptor(
            "role_" + randomAlphaOfLengthBetween(3, 6),
            new String[0], // no cluster privileges
            new IndicesPrivileges[0], // no index privileges
            null
        );
        User user = new User(randomAlphaOfLengthBetween(6, 8), role.getName());
        final Authentication authentication = createAuthentication(user);
        roleMap.put(role.getName(), role);

        AuditUtil.getOrGenerateRequestId(threadContext);

        TransportRequest request = new PutIndexTemplateRequest(randomAlphaOfLengthBetween(4, 20));

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, PutIndexTemplateAction.NAME, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString("[" + PutIndexTemplateAction.NAME + "] is unauthorized for user [" + user.principal() + "]")
            )
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("this action is granted by the cluster privileges [manage_index_templates,manage,all]"))
        );
    }

    public void testDenialErrorMessagesForInvalidateApiKeyAction() throws IOException {
        RoleDescriptor role = new RoleDescriptor(
            "role_" + randomAlphaOfLengthBetween(3, 6),
            new String[0], // no cluster privileges
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index-*").privileges("all").build() },
            null
        );
        User user = new User(randomAlphaOfLengthBetween(6, 8), role.getName());
        final Authentication authentication = createAuthentication(user);
        roleMap.put(role.getName(), role);

        AuditUtil.getOrGenerateRequestId(threadContext);

        // Own API Key
        {
            TransportRequest request = new InvalidateApiKeyRequest(null, null, null, true, null);

            ElasticsearchSecurityException securityException = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authorize(authentication, InvalidateApiKeyAction.NAME, request)
            );
            assertThat(
                securityException,
                throwableWithMessage(
                    containsString("[" + InvalidateApiKeyAction.NAME + "] is unauthorized for user [" + user.principal() + "]")
                )
            );
            assertThat(
                securityException,
                throwableWithMessage(
                    containsString(
                        "this action is granted by the cluster privileges [manage_own_api_key,manage_api_key,manage_security,all]"
                    )
                )
            );
        }

        // All API Keys
        {
            TransportRequest request = new InvalidateApiKeyRequest(null, null, null, false, null);

            ElasticsearchSecurityException securityException = expectThrows(
                ElasticsearchSecurityException.class,
                () -> authorize(authentication, InvalidateApiKeyAction.NAME, request)
            );
            assertThat(
                securityException,
                throwableWithMessage(
                    containsString("[" + InvalidateApiKeyAction.NAME + "] is unauthorized for user [" + user.principal() + "]")
                )
            );
            assertThat(
                securityException,
                throwableWithMessage(
                    containsString("this action is granted by the cluster privileges [manage_api_key,manage_security,all]")
                )
            );
        }
    }

    public void testDenialForAnonymousUser() throws IOException {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetadata();
        Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "a_all").build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        authorizationService = new AuthorizationService(
            settings,
            rolesStore,
            fieldPermissionsCache,
            clusterService,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            anonymousUser,
            null,
            Collections.emptySet(),
            new XPackLicenseState(settings, () -> 0),
            TestIndexNameExpressionResolver.newInstance(),
            operatorPrivilegesService
        );

        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final Authentication authentication = createAuthentication(anonymousUser);
        assertThrowsAuthorizationException(() -> authorize(authentication, "indices:a", request), "indices:a", anonymousUser.principal());
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metadata();
    }

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() throws IOException {
        TransportRequest request = new GetIndexRequest().indices("b");
        ClusterState state = mockEmptyMetadata();
        Settings settings = Settings.builder()
            .put(AnonymousUser.ROLES_SETTING.getKey(), "a_all")
            .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false)
            .build();
        final Authentication authentication = createAuthentication(new AnonymousUser(settings));
        authorizationService = new AuthorizationService(
            settings,
            rolesStore,
            fieldPermissionsCache,
            clusterService,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            new AnonymousUser(settings),
            null,
            Collections.emptySet(),
            new XPackLicenseState(settings, () -> 0),
            TestIndexNameExpressionResolver.newInstance(),
            operatorPrivilegesService
        );

        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, "indices:a", request)
        );
        assertAuthenticationException(securityException, containsString("action [indices:a] requires authentication"));
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService, times(1)).state();
        verify(state, times(1)).metadata();
    }

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsError() throws IOException {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, true);
        TransportRequest request = new GetIndexRequest().indices("not-an-index-*").indicesOptions(options);
        ClusterState state = mockEmptyMetadata();
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final IndexNotFoundException nfe = expectThrows(
            IndexNotFoundException.class,
            () -> authorize(authentication, GetIndexAction.NAME, request)
        );
        assertThat(nfe.getIndex(), is(notNullValue()));
        assertThat(nfe.getIndex().getName(), is("not-an-index-*"));
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(GetIndexAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metadata();
    }

    public void testRunAsRequestWithNoRolesUser() throws IOException {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("run as me", null, new User("test user", "admin")));
        assertNotEquals(authentication.getUser().authenticatedUser(), authentication);
        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a",
            "test user",
            "run as me"
        ); // run as [run as me]
        verify(auditTrail).runAsDenied(eq(requestId), eq(authentication), eq("indices:a"), eq(request), authzInfoRoles(Role.EMPTY.names()));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithoutLookedUpBy() throws IOException {
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        AuthenticateRequest request = new AuthenticateRequest("run as me");
        roleMap.put("superuser", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        User user = new User("run as me", Strings.EMPTY_ARRAY, new User("test user", new String[] { "superuser" }));
        Authentication authentication = new Authentication(user, new RealmRef("foo", "bar", "baz"), null);
        authentication.writeToContext(threadContext);
        assertNotEquals(user.authenticatedUser(), user);
        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, AuthenticateAction.NAME, request),
            AuthenticateAction.NAME,
            "test user",
            "run as me"
        ); // run as [run as me]
        verify(auditTrail).runAsDenied(
            eq(requestId),
            eq(authentication),
            eq(AuthenticateAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() throws IOException {
        TransportRequest request = mock(TransportRequest.class);
        User user = new User("run as me", new String[] { "doesn't exist" }, new User("test user", "can run as"));
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor role = new RoleDescriptor(
            "can run as",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            new String[] { "not the right user" }
        );
        roleMap.put("can run as", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a",
            "test user",
            "run as me"
        );
        verify(auditTrail).runAsDenied(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithRunAsUserWithoutPermission() throws IOException {
        TransportRequest request = new GetIndexRequest().indices("a");
        User authenticatedUser = new User("test user", "can run as");
        User user = new User("run as me", new String[] { "b" }, authenticatedUser);
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor runAsRole = new RoleDescriptor(
            "can run as",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            new String[] { "run as me" }
        );
        roleMap.put("can run as", runAsRole);

        RoleDescriptor bRole = new RoleDescriptor(
            "b",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() },
            null
        );
        boolean indexExists = randomBoolean();
        if (indexExists) {
            mockMetadataWithIndex("a");
            roleMap.put("b", bRole);
        } else {
            mockEmptyMetadata();
        }
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationExceptionRunAs(
            () -> authorize(authentication, "indices:a", request),
            "indices:a",
            "test user",
            "run as me"
        );
        verify(auditTrail).runAsGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { runAsRole.getName() })
        );
        if (indexExists) {
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq("indices:a"),
                eq(request),
                authzInfoRoles(new String[] { bRole.getName() })
            );
        } else {
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq("indices:a"),
                eq(request),
                authzInfoRoles(Role.EMPTY.names())
            );
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithValidPermissions() throws IOException {
        TransportRequest request = new GetIndexRequest().indices("b");
        User authenticatedUser = new User("test user", new String[] { "can run as" });
        User user = new User("run as me", new String[] { "b" }, authenticatedUser);
        assertNotEquals(user.authenticatedUser(), user);
        final Authentication authentication = createAuthentication(user);
        final RoleDescriptor runAsRole = new RoleDescriptor(
            "can run as",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            new String[] { "run as me" }
        );
        roleMap.put("can run as", runAsRole);
        mockMetadataWithIndex("b");
        RoleDescriptor bRole = new RoleDescriptor(
            "b",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("all").build() },
            null
        );
        roleMap.put("b", bRole);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, "indices:a", request);
        verify(auditTrail).runAsGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { runAsRole.getName() })
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:a"),
            eq(request),
            authzInfoRoles(new String[] { bRole.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testGrantAllRestrictedUserCannotExecuteOperationAgainstSecurityIndices() throws IOException {
        RoleDescriptor role = new RoleDescriptor(
            "all access",
            new String[] { "all" },
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("all_access_user", "all_access"));
        roleMap.put("all_access", role);
        ClusterState state = mockClusterState(
            Metadata.builder()
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true
                )
                .build()
        );
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(
            new Tuple<>(BulkAction.NAME + "[s]", new DeleteRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id"))
        );
        requests.add(
            new Tuple<>(UpdateAction.NAME, new UpdateRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id"))
        );
        requests.add(
            new Tuple<>(BulkAction.NAME + "[s]", new IndexRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7)))
        );
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))));
        requests.add(
            new Tuple<>(
                TermVectorsAction.NAME,
                new TermVectorsRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "type", "id")
            )
        );
        requests.add(new Tuple<>(GetAction.NAME, new GetRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")));
        requests.add(
            new Tuple<>(
                IndicesAliasesAction.NAME,
                new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("security_alias").index(INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                UpdateSettingsAction.NAME,
                new UpdateSettingsRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        // cannot execute monitor operations
        requests.add(
            new Tuple<>(
                IndicesStatsAction.NAME,
                new IndicesStatsRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(RecoveryAction.NAME, new RecoveryRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7)))
        );
        requests.add(
            new Tuple<>(
                IndicesSegmentsAction.NAME,
                new IndicesSegmentsRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                GetSettingsAction.NAME,
                new GetSettingsRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                IndicesShardStoresAction.NAME,
                new IndicesShardStoresRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                UpgradeStatusAction.NAME,
                new UpgradeStatusRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );

        for (Tuple<String, TransportRequest> requestTuple : requests) {
            String action = requestTuple.v1();
            TransportRequest request = requestTuple.v2();
            assertThrowsAuthorizationException(() -> authorize(authentication, action, request), action, "all_access_user");
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq(action),
                eq(request),
                authzInfoRoles(new String[] { role.getName() })
            );
            verifyNoMoreInteractions(auditTrail);
        }

        // we should allow waiting for the health of the index or any index if the user has this permission
        ClusterHealthRequest request = new ClusterHealthRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7));
        authorize(authentication, ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(ClusterHealthAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        // multiple indices
        request = new ClusterHealthRequest(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7, "foo", "bar");
        authorize(authentication, ClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(ClusterHealthAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);

        final SearchRequest searchRequest = new SearchRequest("_all");
        authorize(authentication, SearchAction.NAME, searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolver.NO_INDICES_OR_ALIASES_LIST, Arrays.asList(searchRequest.indices()));
    }

    public void testMonitoringOperationsAgainstSecurityIndexRequireAllowRestricted() throws IOException {
        final RoleDescriptor restrictedMonitorRole = new RoleDescriptor(
            "restricted_monitor",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("monitor").build() },
            null
        );
        final RoleDescriptor unrestrictedMonitorRole = new RoleDescriptor(
            "unrestricted_monitor",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("monitor").allowRestrictedIndices(true).build() },
            null
        );
        roleMap.put("restricted_monitor", restrictedMonitorRole);
        roleMap.put("unrestricted_monitor", unrestrictedMonitorRole);
        ClusterState state = mockClusterState(
            Metadata.builder()
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true
                )
                .build()
        );

        List<Tuple<String, ? extends TransportRequest>> requests = new ArrayList<>();
        requests.add(new Tuple<>(IndicesStatsAction.NAME, new IndicesStatsRequest().indices(SECURITY_MAIN_ALIAS)));
        requests.add(new Tuple<>(RecoveryAction.NAME, new RecoveryRequest().indices(SECURITY_MAIN_ALIAS)));
        requests.add(new Tuple<>(IndicesSegmentsAction.NAME, new IndicesSegmentsRequest().indices(SECURITY_MAIN_ALIAS)));
        requests.add(new Tuple<>(GetSettingsAction.NAME, new GetSettingsRequest().indices(SECURITY_MAIN_ALIAS)));
        requests.add(new Tuple<>(IndicesShardStoresAction.NAME, new IndicesShardStoresRequest().indices(SECURITY_MAIN_ALIAS)));
        requests.add(new Tuple<>(UpgradeStatusAction.NAME, new UpgradeStatusRequest().indices(SECURITY_MAIN_ALIAS)));

        for (final Tuple<String, ? extends TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            try (StoredContext ignore = threadContext.stashContext()) {
                final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
                final Authentication restrictedUserAuthn = createAuthentication(new User("restricted_user", "restricted_monitor"));
                assertThrowsAuthorizationException(() -> authorize(restrictedUserAuthn, action, request), action, "restricted_user");
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(restrictedUserAuthn),
                    eq(action),
                    eq(request),
                    authzInfoRoles(new String[] { "restricted_monitor" })
                );
                verifyNoMoreInteractions(auditTrail);
            }
            try (StoredContext ignore = threadContext.stashContext()) {
                final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
                final Authentication unrestrictedUserAuthn = createAuthentication(new User("unrestricted_user", "unrestricted_monitor"));
                authorize(unrestrictedUserAuthn, action, request);
                verify(auditTrail).accessGranted(
                    eq(requestId),
                    eq(unrestrictedUserAuthn),
                    eq(action),
                    eq(request),
                    authzInfoRoles(new String[] { "unrestricted_monitor" })
                );
                verifyNoMoreInteractions(auditTrail);
            }
        }
    }

    public void testSuperusersCanExecuteOperationAgainstSecurityIndex() throws IOException {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mockClusterState(
            Metadata.builder()
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true
                )
                .build()
        );
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        List<Tuple<String, TransportRequest>> requests = new ArrayList<>();
        requests.add(
            new Tuple<>(DeleteAction.NAME, new DeleteRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id"))
        );
        requests.add(
            new Tuple<>(
                BulkAction.NAME + "[s]",
                createBulkShardRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), DeleteRequest::new)
            )
        );
        requests.add(
            new Tuple<>(UpdateAction.NAME, new UpdateRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id"))
        );
        requests.add(new Tuple<>(IndexAction.NAME, new IndexRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))));
        requests.add(
            new Tuple<>(
                BulkAction.NAME + "[s]",
                createBulkShardRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), IndexRequest::new)
            )
        );
        requests.add(new Tuple<>(SearchAction.NAME, new SearchRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))));
        requests.add(
            new Tuple<>(
                TermVectorsAction.NAME,
                new TermVectorsRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "type", "id")
            )
        );
        requests.add(
            new Tuple<>(GetAction.NAME, new GetRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "type", "id"))
        );
        requests.add(
            new Tuple<>(
                TermVectorsAction.NAME,
                new TermVectorsRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "type", "id")
            )
        );
        requests.add(
            new Tuple<>(
                IndicesAliasesAction.NAME,
                new IndicesAliasesRequest().addAliasAction(AliasActions.add().alias("security_alias").index(INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(ClusterHealthAction.NAME, new ClusterHealthRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7)))
        );
        requests.add(
            new Tuple<>(
                ClusterHealthAction.NAME,
                new ClusterHealthRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "foo", "bar")
            )
        );

        for (final Tuple<String, TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
                final Authentication authentication = createAuthentication(superuser);
                authorize(authentication, action, request);
                verify(auditTrail).accessGranted(
                    eq(requestId),
                    eq(authentication),
                    eq(action),
                    eq(request),
                    authzInfoRoles(superuser.roles())
                );
            }
        }
    }

    public void testSuperusersCanExecuteOperationAgainstSecurityIndexWithWildcard() throws IOException {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        final Authentication authentication = createAuthentication(superuser);
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        ClusterState state = mockClusterState(
            Metadata.builder()
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true
                )
                .build()
        );
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        String action = SearchAction.NAME;
        SearchRequest request = new SearchRequest("_all");
        authorize(authentication, action, request);
        verify(auditTrail).accessGranted(eq(requestId), eq(authentication), eq(action), eq(request), authzInfoRoles(superuser.roles()));
        assertThat(request.indices(), arrayContainingInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_MAIN_ALIAS));
    }

    public void testCompositeActionsAreImmediatelyRejected() {
        // if the user has no permission for composite actions against any index, the request fails straight-away in the main action
        final Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        final String action = compositeRequest.v1();
        final TransportRequest request = compositeRequest.v2();
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        final RoleDescriptor role = new RoleDescriptor("no_indices", null, null, null);
        roleMap.put("no_indices", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(() -> authorize(authentication, action, request), action, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsIndicesAreNotChecked() throws IOException {
        // if the user has permission for some index, the request goes through without looking at the indices, they will be checked later
        final Tuple<String, TransportRequest> compositeRequest = randomCompositeRequest();
        final String action = compositeRequest.v1();
        final TransportRequest request = compositeRequest.v2();
        final Authentication authentication = createAuthentication(new User("test user", "role"));
        final RoleDescriptor role = new RoleDescriptor(
            "role",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() },
            null
        );
        roleMap.put("role", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        authorize(authentication, action, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testCompositeActionsMustImplementCompositeIndicesRequest() throws IOException {
        String action = randomCompositeRequest().v1();
        TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("test user", "role");
        roleMap.put(
            "role",
            new RoleDescriptor(
                "role",
                null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices(randomBoolean() ? "a" : "index").privileges("all").build() },
                null
            )
        );
        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> authorize(createAuthentication(user), action, request)
        );
        assertThat(illegalStateException.getMessage(), containsString("Composite and bulk actions must implement CompositeIndicesRequest"));
    }

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() throws IOException {
        final MockIndicesRequest mockRequest = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        final TransportRequest request;
        final String action;
        switch (randomIntBetween(0, 4)) {
            case 0:
                action = MultiGetAction.NAME + "[shard]";
                request = mockRequest;
                break;
            case 1:
                // reindex, msearch, search template, and multi search template delegate to search
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
        roleMap.put(
            "roleAllowed",
            new RoleDescriptor(
                "roleAllowed",
                null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index").privileges("all").build() },
                null
            )
        );
        User userDenied = new User("userDenied", "roleDenied");
        roleMap.put(
            "roleDenied",
            new RoleDescriptor(
                "roleDenied",
                null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
                null
            )
        );
        AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();
        try (ThreadContext.StoredContext ignore = threadContext.newStoredContext(false)) {
            authorize(createAuthentication(userAllowed), action, request);
        }
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    public void testAuthorizationOfIndividualBulkItems() throws IOException {
        final String action = BulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new DeleteRequest("concrete-index", "doc", "c1")),
            new BulkItemRequest(2, new IndexRequest("concrete-index", "doc", "c2")),
            new BulkItemRequest(3, new DeleteRequest("alias-1", "doc", "a1a")),
            new BulkItemRequest(4, new IndexRequest("alias-1", "doc", "a1b")),
            new BulkItemRequest(5, new DeleteRequest("alias-2", "doc", "a2a")),
            new BulkItemRequest(6, new IndexRequest("alias-2", "doc", "a2b")) };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final TransportRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

        final Authentication authentication = createAuthentication(new User("user", "my-role"));
        RoleDescriptor role = new RoleDescriptor(
            "my-role",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().indices("concrete-index").privileges("all").build(),
                IndicesPrivileges.builder().indices("alias-1").privileges("index").build(),
                IndicesPrivileges.builder().indices("alias-2").privileges("delete").build() },
            null
        );
        roleMap.put("my-role", role);

        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, action, request);

        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(DeleteAction.NAME),
            eq("concrete-index"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(DeleteAction.NAME),
            eq("alias-2"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(IndexAction.NAME + ":op_type/index"),
            eq("concrete-index"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(IndexAction.NAME + ":op_type/index"),
            eq("alias-1"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(DeleteAction.NAME),
            eq("alias-1"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(IndexAction.NAME + ":op_type/index"),
            eq("alias-2"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        ); // bulk request is allowed
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthorizationOfIndividualBulkItemsWithDateMath() throws IOException {
        final String action = BulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new IndexRequest("<datemath-{now/M{YYYY}}>", "doc", "dy1")),
            new BulkItemRequest(2, new DeleteRequest("<datemath-{now/d{YYYY}}>", "doc", "dy2")), // resolves to same as above
            new BulkItemRequest(3, new IndexRequest("<datemath-{now/M{YYYY.MM}}>", "doc", "dm1")),
            new BulkItemRequest(4, new DeleteRequest("<datemath-{now/d{YYYY.MM}}>", "doc", "dm2")), // resolves to same as above
        };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final TransportRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

        final Authentication authentication = createAuthentication(new User("user", "my-role"));
        final RoleDescriptor role = new RoleDescriptor(
            "my-role",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("datemath-*").privileges("index").build() },
            null
        );
        roleMap.put("my-role", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        mockEmptyMetadata();
        authorize(authentication, action, request);

        // both deletes should fail
        verify(auditTrail, times(2)).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(DeleteAction.NAME),
            ArgumentMatchers.startsWith("datemath-"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail, times(2)).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(IndexAction.NAME + ":op_type/index"),
            ArgumentMatchers.startsWith("datemath-"),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        // bulk request is allowed
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    private BulkShardRequest createBulkShardRequest(String indexName, TriFunction<String, String, String, DocWriteRequest<?>> req) {
        final BulkItemRequest[] items = { new BulkItemRequest(1, req.apply(indexName, "type", "id")) };
        return new BulkShardRequest(new ShardId(indexName, UUID.randomUUID().toString(), 1), WriteRequest.RefreshPolicy.IMMEDIATE, items);
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

    private static class MockCompositeIndicesRequest extends TransportRequest implements CompositeIndicesRequest {}

    private Authentication createAuthentication(User user) {
        RealmRef lookedUpBy = user.authenticatedUser() == user ? null : new RealmRef("looked", "up", "by");
        Authentication authentication = new Authentication(user, new RealmRef("test", "test", "foo"), lookedUpBy);
        try {
            authentication.writeToContext(threadContext);
        } catch (IOException e) {
            throw new UncheckedIOException("caught unexpected IOException", e);
        }
        return authentication;
    }

    private ClusterState mockEmptyMetadata() {
        return mockClusterState(Metadata.EMPTY_METADATA);
    }

    private ClusterState mockMetadataWithIndex(String indexName) {
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
            Settings.builder().put("index.version.created", Version.CURRENT).build()
        ).numberOfShards(1).numberOfReplicas(0).build();
        final Metadata metadata = Metadata.builder().put(indexMetadata, true).build();
        return mockClusterState(metadata);
    }

    private ClusterState mockClusterState(Metadata metadata) {
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        return state;
    }

    public void testProxyRequestFailsOnNonProxyAction() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, request);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("test user", "role");
        ElasticsearchSecurityException ese = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(createAuthentication(user), "indices:some/action", transportRequest)
        );
        assertThat(ese.getCause(), instanceOf(IllegalStateException.class));
        IllegalStateException illegalStateException = (IllegalStateException) ese.getCause();
        assertThat(
            illegalStateException.getMessage(),
            startsWith("originalRequest is a proxy request for: [org.elasticsearch.transport.TransportRequest$")
        );
        assertThat(illegalStateException.getMessage(), endsWith("] but action: [indices:some/action] isn't"));
    }

    public void testProxyRequestFailsOnNonProxyRequest() {
        TransportRequest request = TransportRequest.Empty.INSTANCE;
        User user = new User("test user", "role");
        AuditUtil.getOrGenerateRequestId(threadContext);
        ElasticsearchSecurityException ese = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(createAuthentication(user), TransportActionProxy.getProxyAction("indices:some/action"), request)
        );
        assertThat(ese.getCause(), instanceOf(IllegalStateException.class));
        IllegalStateException illegalStateException = (IllegalStateException) ese.getCause();
        assertThat(
            illegalStateException.getMessage(),
            startsWith("originalRequest is not a proxy request: [org.elasticsearch.transport.TransportRequest$")
        );
        assertThat(
            illegalStateException.getMessage(),
            endsWith("] but action: [internal:transport/proxy/indices:some/action] is a proxy action")
        );
    }

    public void testProxyRequestAuthenticationDenied() throws IOException {
        final TransportRequest proxiedRequest = new SearchRequest();
        final DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, proxiedRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.QUERY_ACTION_NAME);
        final Authentication authentication = createAuthentication(new User("test user", "no_indices"));
        final RoleDescriptor role = new RoleDescriptor("no_indices", null, null, null);
        roleMap.put("no_indices", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationException(() -> authorize(authentication, action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(proxiedRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testProxyRequestAuthenticationGrantedWithAllPrivileges() {
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        mockEmptyMetadata();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(authentication, action, transportRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(clearScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
    }

    public void testProxyRequestAuthenticationGranted() {
        RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("read_cross_cluster").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        roleMap.put("a_all", role);
        mockEmptyMetadata();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        final TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        final String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        authorize(authentication, action, transportRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(clearScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
    }

    public void testProxyRequestAuthenticationDeniedWithReadPrivileges() throws IOException {
        final Authentication authentication = createAuthentication(new User("test user", "a_all"));
        final RoleDescriptor role = new RoleDescriptor(
            "a_all",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("read").build() },
            null
        );
        roleMap.put("a_all", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();
        DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, clearScrollRequest);
        String action = TransportActionProxy.getProxyAction(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
        assertThrowsAuthorizationException(() -> authorize(authentication, action, transportRequest), action, "test user");
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(clearScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );
    }

    public void testAuthorizationEngineSelection() {
        final AuthorizationEngine engine = new AuthorizationEngine() {
            @Override
            public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void authorizeRunAs(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                ActionListener<AuthorizationResult> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void authorizeClusterAction(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                ActionListener<AuthorizationResult> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void authorizeIndexAction(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                Map<String, IndexAbstraction> aliasOrIndexLookup,
                ActionListener<IndexAuthorizationResult> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void loadAuthorizedIndices(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                Map<String, IndexAbstraction> indicesLookup,
                ActionListener<Set<String>> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void validateIndexPermissionsAreSubset(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                Map<String, List<String>> indexNameToNewNames,
                ActionListener<AuthorizationResult> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void checkPrivileges(
                Authentication authentication,
                AuthorizationInfo authorizationInfo,
                HasPrivilegesRequest hasPrivilegesRequest,
                Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                ActionListener<HasPrivilegesResponse> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void getUserPrivileges(
                Authentication authentication,
                AuthorizationInfo authorizationInfo,
                GetUserPrivilegesRequest request,
                ActionListener<GetUserPrivilegesResponse> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }
        };

        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        authorizationService = new AuthorizationService(
            Settings.EMPTY,
            rolesStore,
            fieldPermissionsCache,
            clusterService,
            auditTrailService,
            new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool,
            new AnonymousUser(Settings.EMPTY),
            engine,
            Collections.emptySet(),
            licenseState,
            TestIndexNameExpressionResolver.newInstance(),
            operatorPrivilegesService
        );
        Authentication authentication;
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("test user", "a_all"));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("runas", new String[] { "runas_role" }, new User("runner", "runner_role")));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("runas", new String[] { "runas_role" }, new ElasticUser(true)));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertNotEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("elastic", new String[] { "superuser" }, new User("runner", "runner_role")));
            assertNotEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("kibana", new String[] { "kibana_system" }, new ElasticUser(true)));
            assertNotEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertNotEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(
                randomFrom(XPackUser.INSTANCE, XPackSecurityUser.INSTANCE, new ElasticUser(true), new KibanaUser(true))
            );
            assertNotEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }
    }

    public void testOperatorPrivileges() {
        shouldFailOperatorPrivilegesCheck = true;
        AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication authentication = createAuthentication(new User("user1", "role1"));
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/whatever", mock(TransportRequest.class)),
            "cluster:admin/whatever",
            "user1"
        );
        // The operator related exception is verified in the authorize(...) call
        verifyNoMoreInteractions(auditTrail);
    }

    static AuthorizationInfo authzInfoRoles(String[] expectedRoles) {
        return ArgumentMatchers.argThat(new RBACAuthorizationInfoRoleMatcher(expectedRoles));
    }

    private static class RBACAuthorizationInfoRoleMatcher implements ArgumentMatcher<AuthorizationInfo> {

        private final String[] wanted;

        RBACAuthorizationInfoRoleMatcher(String[] expectedRoles) {
            this.wanted = expectedRoles;
        }

        @Override
        public boolean matches(AuthorizationInfo other) {
            final String[] found = (String[]) other.asMap().get(PRINCIPAL_ROLES_FIELD_NAME);
            return Arrays.equals(wanted, found);
        }
    }

    private abstract static class MockConfigurableClusterPrivilege implements ConfigurableClusterPrivilege {
        @Override
        public Category getCategory() {
            return Category.APPLICATION;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public String getWriteableName() {
            return "mock";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
