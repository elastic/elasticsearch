/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.MockIndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.MappingUpdatePerformer;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.get.TransportMultiGetAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.ParsedScrollId;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.bulk.stats.BulkOperationListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
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
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ActionListenerUtils.anyCollection;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAsDenied;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionRunAsUnauthorizedAction;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult.ALL_CHECKS_SUCCESS_NO_DETAILS;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult.SOME_CHECKS_FAILURE_NO_DETAILS;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_KEY;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.ORIGINATING_ACTION_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.PRINCIPAL_ROLES_FIELD_NAME;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AuthorizationServiceTests extends ESTestCase {
    private ProjectId projectId;
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
    private SecurityContext securityContext;
    private ProjectResolver projectResolver;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        projectId = randomUniqueProjectId();
        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        rolesStore = mock(CompositeRolesStore.class);
        clusterService = mock(ClusterService.class);
        final Settings settings = Settings.builder().put("cluster.remote.other_cluster.seeds", "localhost:9999").build();
        final ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, LoadAuthorizedIndicesTimeChecker.Factory.getSettings())
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        mockEmptyMetadata();

        auditTrail = mock(AuditTrail.class);
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        auditTrailService = new AuditTrailService(auditTrail, licenseState);
        threadContext = new ThreadContext(settings);
        securityContext = new SecurityContext(settings, threadContext);
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(settings);

        final NativePrivilegeStore privilegesStore = mock(NativePrivilegeStore.class);
        doAnswer(i -> {
            assertThat(i.getArguments().length, equalTo(4));
            final Object arg2 = i.getArguments()[3];
            assertThat(arg2, instanceOf(ActionListener.class));
            ActionListener<Collection<ApplicationPrivilege>> listener = (ActionListener<Collection<ApplicationPrivilege>>) arg2;
            listener.onResponse(Collections.emptyList());
            return null;
        }).when(privilegesStore).getPrivileges(any(Collection.class), any(Collection.class), eq(false), anyActionListener());

        final Map<Set<String>, Role> roleCache = new HashMap<>();
        final AnonymousUser anonymousUser = mock(AnonymousUser.class);
        when(anonymousUser.enabled()).thenReturn(false);
        doAnswer(i -> {
            i.callRealMethod();
            return null;
        }).when(rolesStore).getRoles(any(Authentication.class), anyActionListener());
        doAnswer((i) -> {
            ActionListener<Role> callback = (ActionListener<Role>) i.getArguments()[1];
            User user = ((Subject) i.getArguments()[0]).getUser();
            buildRole(user, privilegesStore, fieldPermissionsCache, roleCache, callback);
            return null;
        }).when(rolesStore).getRole(any(Subject.class), anyActionListener());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        operatorPrivilegesService = mock(OperatorPrivileges.OperatorPrivilegesService.class);
        projectResolver = TestProjectResolvers.singleProject(projectId);
        indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(projectResolver);
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
            indexNameExpressionResolver,
            operatorPrivilegesService,
            RESTRICTED_INDICES,
            new AuthorizationDenialMessages.Default(),
            projectResolver
        );
    }

    private void buildRole(
        User user,
        NativePrivilegeStore privilegesStore,
        FieldPermissionsCache fieldPermissionsCache,
        Map<Set<String>, Role> roleCache,
        ActionListener<Role> listener
    ) {
        final Set<String> names = Set.of(user.roles());
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
            CompositeRolesStore.buildRoleFromDescriptors(
                roleDescriptors,
                fieldPermissionsCache,
                privilegesStore,
                RESTRICTED_INDICES,
                ActionListener.wrap(r -> {
                    roleCache.put(names, r);
                    listener.onResponse(r);
                }, listener::onFailure)
            );
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
            when(mockAccessControlHeader.isGranted()).thenReturn(true);
            securityContext.putIndicesAccessControl(mockAccessControlHeader);
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

    public void testActionsForSystemUserIsAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        // A failure would throw an exception
        final Authentication authentication = createAuthentication(InternalUsers.SYSTEM_USER);
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

    public void testActionsForUserMatchingSystemUserRoleNameDenied() {
        final Authentication authentication = createAuthentication(new User(SystemUser.NAME, SystemUser.ROLE_NAME));
        final IndexRequest request = mock(IndexRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        RoleDescriptor role = new RoleDescriptor(SystemUser.ROLE_NAME, new String[] {}, null, null, null, null, null, null);
        roleMap.put(SystemUser.ROLE_NAME, role);
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
            assertThrowsAuthorizationException(
                () -> authorize(authentication, action, request),
                action,
                authentication.getEffectiveSubject().getUser().principal()
            );
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq(action),
                eq(request),
                authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
            );
        }

        verifyNoMoreInteractions(auditTrail);
    }

    public void testSystemUserActionMatchingCustomRoleNameDenied() {
        final Authentication authentication = createAuthentication(InternalUsers.SYSTEM_USER);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        RoleDescriptor role = new RoleDescriptor(
            SystemUser.ROLE_NAME,
            new String[] { ClusterPrivilegeResolver.ALL.name() },
            null,
            null,
            null,
            null,
            null,
            null
        );
        roleMap.put(SystemUser.ROLE_NAME, role);

        String actionNotAuthorizedForSystemUser = PutUserAction.NAME;
        assertThat(SystemUser.isAuthorized(actionNotAuthorizedForSystemUser), is(false));
        TransportRequest request = mock(TransportRequest.class);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, actionNotAuthorizedForSystemUser, request),
            actionNotAuthorizedForSystemUser,
            authentication.getEffectiveSubject().getUser().principal()
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(actionNotAuthorizedForSystemUser),
            eq(request),
            authzInfoRoles(new String[] { SystemUser.ROLE_NAME })
        );
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

    public void testIndicesActionsForSystemUserWhichAreNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(InternalUsers.SYSTEM_USER);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "indices:", request),
            "indices:",
            InternalUsers.SYSTEM_USER.principal()
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

    public void testClusterAdminActionsForSystemUserWhichAreNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(InternalUsers.SYSTEM_USER);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/whatever", request),
            "cluster:admin/whatever",
            InternalUsers.SYSTEM_USER.principal()
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

    public void testClusterAdminSnapshotStatusActionForSystemUserWhichIsNotAuthorized() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = createAuthentication(InternalUsers.SYSTEM_USER);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, "cluster:admin/snapshot/status", request),
            "cluster:admin/snapshot/status",
            InternalUsers.SYSTEM_USER.principal()
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

    public void testAuthorizeUsingConditionalPrivileges() {
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

    public void testAuthorizationDeniedWhenConditionalPrivilegesDoNotMatch() {
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

    public void testNoRolesCausesDenial() {
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

    public void testUserWithNoRolesCanPerformRemoteSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("other_cluster:index1", "*_cluster:index2", "other_cluster:other_*");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, TransportSearchAction.TYPE.name(), request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchAction.TYPE.name()),
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
        for (final boolean hasLocalIndices : List.of(true, false)) {
            when(parsedScrollId.hasLocalIndices()).thenReturn(hasLocalIndices);
            if (hasLocalIndices) {
                assertThrowsAuthorizationException(
                    () -> authorize(authentication, TransportSearchScrollAction.TYPE.name(), searchScrollRequest),
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
                authorize(authentication, TransportSearchScrollAction.TYPE.name(), searchScrollRequest);
                verify(auditTrail).accessGranted(
                    eq(requestId),
                    eq(authentication),
                    eq(TransportSearchScrollAction.TYPE.name()),
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
    public void testUserWithNoRolesCannotPerformLocalSearch() {
        SearchRequest request = new SearchRequest();
        request.indices("no_such_cluster:index");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, TransportSearchAction.TYPE.name(), request),
            TransportSearchAction.TYPE.name(),
            "test user"
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchAction.TYPE.name()),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
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
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, TransportSearchAction.TYPE.name(), request),
            TransportSearchAction.TYPE.name(),
            "test user"
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchAction.TYPE.name()),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesCannotSql() {
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
    public void testRemoteIndicesOnlyWorkWithApplicableRequestTypes() {
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices("other_cluster:index1", "other_cluster:index2");
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        assertThrowsAuthorizationException(
            () -> authorize(authentication, TransportDeleteIndexAction.TYPE.name(), request),
            TransportDeleteIndexAction.TYPE.name(),
            "test user"
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(TransportDeleteIndexAction.TYPE.name()),
            eq(request),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUserWithNoRolesOpenPointInTimeWithRemoteIndices() {
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        for (final boolean hasLocalIndices : List.of(true, false)) {
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
                    () -> authorize(authentication, TransportOpenPointInTimeAction.TYPE.name(), openPointInTimeRequest),
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
                authorize(authentication, TransportOpenPointInTimeAction.TYPE.name(), openPointInTimeRequest);
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
        final ClosePointInTimeRequest closePointInTimeRequest = new ClosePointInTimeRequest(new BytesArray(randomAlphaOfLength(8)));
        final Authentication authentication = createAuthentication(new User("test user"));
        mockEmptyMetadata();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        authorize(authentication, TransportClosePointInTimeAction.TYPE.name(), closePointInTimeRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq("indices:data/read/close_point_in_time"),
            eq(closePointInTimeRequest),
            authzInfoRoles(Role.EMPTY.names())
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testUnknownRoleCausesDenial() {
        Tuple<String, TransportRequest> tuple = randomFrom(
            asList(
                new Tuple<>(TransportSearchAction.TYPE.name(), new SearchRequest()),
                new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())
            )
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
                containsString(
                    "["
                        + action
                        + "] is unauthorized"
                        + " for user [test user]"
                        + " with effective roles [] (assigned roles [non-existent-role] were not found)"
                )
            )
        );
        assertThat(securityException, throwableWithMessage(containsString("this action is granted by the index privileges [read,all]")));

        verify(auditTrail).accessDenied(eq(requestId), eq(authentication), eq(action), eq(request), authzInfoRoles(Role.EMPTY.names()));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testServiceAccountDenial() {
        Tuple<String, TransportRequest> tuple = randomFrom(
            asList(
                new Tuple<>(TransportSearchAction.TYPE.name(), new SearchRequest()),
                new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())
            )
        );
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();

        final User serviceUser = new User(randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8));
        final Authentication authentication = Authentication.newServiceAccountAuthentication(
            serviceUser,
            randomAlphaOfLengthBetween(3, 8),
            Map.of(
                "_token_name",
                randomAlphaOfLengthBetween(3, 8),
                "_token_source",
                randomFrom(TokenInfo.TokenSource.values()).name().toLowerCase(Locale.ROOT)
            )
        );
        final Role role = Role.EMPTY;
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[1];
            listener.onResponse(role);
            return null;
        }).when(rolesStore).getRole(any(Subject.class), anyActionListener());

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, action, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("[" + action + "] is unauthorized for service account [" + serviceUser.principal() + "]"))
        );
        verify(auditTrail).accessDenied(eq(requestId), eq(authentication), eq(action), eq(request), authzInfoRoles(role.names()));
        assertThat(securityException, throwableWithMessage(containsString("this action is granted by the index privileges [read,all]")));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testThatNonIndicesAndNonClusterActionIsDenied() {
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

    public void testThatRoleWithNoIndicesIsDenied() {
        Tuple<String, TransportRequest> tuple = randomFrom(
            new Tuple<>(TransportSearchAction.TYPE.name(), new SearchRequest()),
            new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())
        );
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
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
            throwableWithMessage(
                containsString("[" + action + "] is unauthorized" + " for user [test user]" + " with effective roles [no_indices]")
            )
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

    public void testElasticUserAuthorizedForNonChangePasswordRequestsWhenNotInSetupMode() {
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
                () -> authorize(authentication, TransportSearchAction.TYPE.name(), searchRequest),
                TransportSearchAction.TYPE.name(),
                "test user"
            );
            verify(auditTrail).accessDenied(
                eq(requestId),
                eq(authentication),
                eq(TransportSearchAction.TYPE.name()),
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
            final ActionListener<Void> listener = ActionTestUtils.assertNoFailureListener(ignore -> {
                final IndicesAccessControl indicesAccessControl = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
                assertNotNull(indicesAccessControl);
                final IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(
                    IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER
                );
                assertFalse(indexAccessControl.getFieldPermissions().hasFieldLevelSecurity());
                assertFalse(indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions());
            });
            final CountDownLatch latch = new CountDownLatch(1);
            authorizationService.authorize(
                authentication,
                TransportSearchAction.TYPE.name(),
                searchRequest,
                new LatchedActionListener<>(listener, latch)
            );
            latch.await();
            verify(auditTrail).accessGranted(
                eq(requestId),
                eq(authentication),
                eq(TransportSearchAction.TYPE.name()),
                eq(searchRequest),
                authzInfoRoles(new String[] { role.getName() })
            );
        }
    }

    public void testSearchAgainstIndex() {
        RoleDescriptor role = new RoleDescriptor(
            "search_index",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index-*").privileges("read").build() },
            null
        );
        roleMap.put(role.getName(), role);
        final Authentication authentication = createAuthentication(new User("test search user", role.getName()));

        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final String indexName = "index-" + randomAlphaOfLengthBetween(1, 5);

        final int additionalProjects = randomIntBetween(0, 5);
        final Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(ProjectMetadata.builder(projectId).put(createIndexMetadata(indexName), true));

        for (int p = 0; p < additionalProjects; p++) {
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomUniqueProjectId());
            int indices = randomIntBetween(1, 3);
            for (int i = 0; i < indices; i++) {
                projectBuilder.put(createIndexMetadata(i == 0 ? indexName : randomAlphaOfLength(12)), true);
            }
            metadataBuilder.put(projectBuilder);
        }
        final ClusterState clusterState = mockClusterState(metadataBuilder.build());
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(projectId).index(indexName);

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
        authorize(authentication, TransportSearchAction.TYPE.name(), searchRequest, true, () -> {
            verify(rolesStore).getRoles(Mockito.same(authentication), any());
            IndicesAccessControl iac = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
            // Successful search action authorization should set a parent authorization header.
            assertThat(securityContext.getParentAuthorization().action(), equalTo(TransportSearchAction.TYPE.name()));
            // Within the action handler, execute a child action (the query phase of search)
            authorize(authentication, SearchTransportService.QUERY_ACTION_NAME, shardRequest, false, () -> {
                // This child action triggers a second interaction with the role store (which is cached)
                verify(rolesStore, times(2)).getRoles(Mockito.same(authentication), any());
                // But it does not create a new IndicesAccessControl
                assertThat(threadContext.getTransient(INDICES_PERMISSIONS_KEY), sameInstance(iac));
                // The parent authorization header should only be present for direct child actions
                // and not be carried over for a child of a child actions.
                // Meaning, only query phase action should be pre-authorized in this case and potential sub-actions should not.
                assertThat(securityContext.getParentAuthorization(), nullValue());
            });
        });
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchAction.TYPE.name()),
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

    public void testSearchPITAgainstIndex() {
        RoleDescriptor role = new RoleDescriptor(
            "search_index",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("index-*").privileges("read").build() },
            null
        );
        roleMap.put(role.getName(), role);
        final Authentication authentication = createAuthentication(new User("test search user", role.getName()));

        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final String indexName = "index-" + randomAlphaOfLengthBetween(1, 5);

        final ClusterState clusterState = mockMetadataWithIndex(indexName);
        final IndexMetadata indexMetadata = clusterState.metadata().getProject(projectId).index(indexName);

        PointInTimeBuilder pit = new PointInTimeBuilder(createEncodedPIT(indexMetadata.getIndex()));
        SearchRequest searchRequest = new SearchRequest().source(new SearchSourceBuilder().pointInTimeBuilder(pit))
            .allowPartialSearchResults(false);
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            new OriginalIndices(new String[] { indexName }, searchRequest.indicesOptions()),
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
        authorize(authentication, TransportSearchAction.TYPE.name(), searchRequest, true, () -> {
            verify(rolesStore).getRoles(Mockito.same(authentication), any());
            IndicesAccessControl iac = threadContext.getTransient(INDICES_PERMISSIONS_KEY);
            // Successful search action authorization should set a parent authorization header.
            assertThat(securityContext.getParentAuthorization().action(), equalTo(TransportSearchAction.TYPE.name()));
            // Within the action handler, execute a child action (the query phase of search)
            authorize(authentication, SearchTransportService.QUERY_ACTION_NAME, shardRequest, false, () -> {
                // This child action triggers a second interaction with the role store (which is cached)
                verify(rolesStore, times(2)).getRoles(Mockito.same(authentication), any());
                // But it does not create a new IndicesAccessControl
                assertThat(threadContext.getTransient(INDICES_PERMISSIONS_KEY), sameInstance(iac));
                // The parent authorization header should only be present for direct child actions
                // and not be carried over for a child of a child actions.
                // Meaning, only query phase action should be pre-authorized in this case and potential sub-actions should not.
                assertThat(securityContext.getParentAuthorization(), nullValue());
            });
        });
        assertThat(searchRequest.indices().length, equalTo(0));
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchAction.TYPE.name()),
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
        authorize(authentication, TransportClearScrollAction.NAME, clearScrollRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportClearScrollAction.NAME),
            eq(clearScrollRequest),
            authzInfoRoles(new String[] { role.getName() })
        );

        final ParsedScrollId parsedScrollId = mock(ParsedScrollId.class);
        when(parsedScrollId.hasLocalIndices()).thenReturn(true);
        final SearchScrollRequest searchScrollRequest = mock(SearchScrollRequest.class);
        when(searchScrollRequest.parseScrollId()).thenReturn(parsedScrollId);
        authorize(authentication, TransportSearchScrollAction.TYPE.name(), searchScrollRequest);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportSearchScrollAction.TYPE.name()),
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

    public void testAuthorizeIndicesFailures() {
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("b");
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

    public void testCreateIndexWithAliasWithoutPermissions() {
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
            () -> authorize(authentication, TransportCreateIndexAction.TYPE.name(), request),
            TransportIndicesAliasesAction.NAME,
            "test user"
        );
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportCreateIndexAction.TYPE.name()),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).accessDenied(
            eq(requestId),
            eq(authentication),
            eq(TransportIndicesAliasesAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        verify(clusterService).state();
        verify(state, times(1)).metadata();
    }

    public void testCreateIndexWithAlias() {
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

        authorize(authentication, TransportCreateIndexAction.TYPE.name(), request);

        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportCreateIndexAction.TYPE.name()),
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

    public void testDenialErrorMessagesForSearchAction() {
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
            () -> authorize(authentication, TransportSearchAction.TYPE.name(), request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString(
                    "["
                        + TransportSearchAction.TYPE.name()
                        + "] is unauthorized"
                        + " for user ["
                        + user.principal()
                        + "]"
                        + " with effective roles ["
                        + emptyRole.getName()
                        + ","
                        + indexRole.getName()
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
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices(index).privileges(TransportBulkAction.NAME).build() },
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
                    new IndexRequest(index).id("doc-1").opType(DocWriteRequest.OpType.CREATE).source(Map.of("field", "value"))
                ),
                new BulkItemRequest(
                    1,
                    new IndexRequest(index).id("doc-2").opType(DocWriteRequest.OpType.INDEX).source(Map.of("field", "value"))
                ),
                new BulkItemRequest(2, new DeleteRequest(index, "doc-3")) }
        );

        authorize(authentication, TransportShardBulkAction.ACTION_NAME, request);

        MappingUpdatePerformer mappingUpdater = (m, s, l) -> l.onResponse(null);
        ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate = (l, mappingVersion) -> l.onResponse(null);
        PlainActionFuture<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>> future = new PlainActionFuture<>();
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getBulkOperationListener()).thenReturn(new BulkOperationListener() {
        });
        TransportShardBulkAction.performOnPrimary(
            request,
            indexShard,
            new UpdateHelper(mock(ScriptService.class)),
            System::currentTimeMillis,
            mappingUpdater,
            waitForMappingUpdate,
            future,
            threadPool.executor(Names.WRITE)
        );

        TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse> result = future.get();
        BulkShardResponse response = result.replicationResponse;
        assertThat(response, notNullValue());
        assertThat(response.getResponses(), arrayWithSize(3));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("unauthorized for user [" + user.principal() + "]"));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("on indices [" + index + "]"));
        assertThat(response.getResponses()[0].getFailureMessage(), containsString("[create_doc,create,index,write,all]"));
        assertThat(response.getResponses()[1].getFailureMessage(), containsString("[create,index,write,all]"));
        assertThat(response.getResponses()[2].getFailureMessage(), containsString("[delete,write,all]"));
    }

    public void testDenialErrorMessagesForClusterHealthAction() {
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

        TransportRequest request = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT);

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(authentication, TransportClusterHealthAction.NAME, request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString("[" + TransportClusterHealthAction.NAME + "] is unauthorized for user [" + user.principal() + "]")
            )
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
    public void testDenialErrorMessagesForIndexTemplateAction() {
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
            () -> authorize(authentication, TransportPutIndexTemplateAction.TYPE.name(), request)
        );
        assertThat(
            securityException,
            throwableWithMessage(
                containsString("[" + TransportPutIndexTemplateAction.TYPE.name() + "] is unauthorized for user [" + user.principal() + "]")
            )
        );
        assertThat(
            securityException,
            throwableWithMessage(containsString("this action is granted by the cluster privileges [manage_index_templates,manage,all]"))
        );
    }

    public void testDenialErrorMessagesForInvalidateApiKeyAction() {
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

    public void testDenialForAnonymousUser() {
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("b");
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
            new XPackLicenseState(() -> 0),
            indexNameExpressionResolver,
            operatorPrivilegesService,
            RESTRICTED_INDICES,
            new AuthorizationDenialMessages.Default(),
            projectResolver
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

    public void testDenialForAnonymousUserAuthorizationExceptionDisabled() {
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("b");
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
            new XPackLicenseState(() -> 0),
            indexNameExpressionResolver,
            operatorPrivilegesService,
            RESTRICTED_INDICES,
            new AuthorizationDenialMessages.Default(),
            projectResolver
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

    public void testAuditTrailIsRecordedWhenIndexWildcardThrowsError() {
        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, true);
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("not-an-index-*").indicesOptions(options);
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

    public void testRunAsRequestWithNoRolesUser() {
        final TransportRequest request = mock(TransportRequest.class);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        final User authenticatingUser = new User("test user");
        final Authentication authentication = createAuthentication(new User("run as me"), authenticatingUser);
        assertNotEquals(authentication.getAuthenticatingSubject().getUser(), authentication.getEffectiveSubject().getUser());
        assertThrowsAuthorizationExceptionRunAsDenied(
            () -> authorize(authentication, "indices:a", request),
            "indices:a",
            authenticatingUser,
            "run as me"
        ); // run as [run as me]
        verify(auditTrail).runAsDenied(eq(requestId), eq(authentication), eq("indices:a"), eq(request), authzInfoRoles(Role.EMPTY.names()));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestWithoutLookedUpBy() throws IOException {
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        roleMap.put("superuser", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        final User authUser = new User("test user", "superuser");
        Authentication authentication = AuthenticationTestHelper.builder()
            .realm()
            .user(authUser)
            .realmRef(new RealmRef("foo", "bar", "baz"))
            .build(false)
            .runAs(new User("run as me", Strings.EMPTY_ARRAY), null);
        authentication.writeToContext(threadContext);
        assertNotEquals(authUser, authentication.getEffectiveSubject().getUser());
        assertThrowsAuthorizationExceptionRunAsDenied(
            () -> authorize(authentication, AuthenticateAction.NAME, AuthenticateRequest.INSTANCE),
            AuthenticateAction.NAME,
            authUser,
            "run as me"
        ); // run as [run as me]
        verify(auditTrail).runAsDenied(
            eq(requestId),
            eq(authentication),
            eq(AuthenticateAction.NAME),
            eq(AuthenticateRequest.INSTANCE),
            authzInfoRoles(new String[] { ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRunAsRequestRunningAsUnAllowedUser() {
        TransportRequest request = mock(TransportRequest.class);
        final User authenticatingUser = new User("test user", "can run as");
        final Authentication authentication = createAuthentication(new User("run as me", "doesn't exist"), authenticatingUser);
        final RoleDescriptor role = new RoleDescriptor(
            "can run as",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a").privileges("all").build() },
            new String[] { "not the right user" }
        );
        roleMap.put("can run as", role);
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        assertThrowsAuthorizationExceptionRunAsDenied(
            () -> authorize(authentication, "indices:a", request),
            "indices:a",
            authenticatingUser,
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

    public void testRunAsRequestWithRunAsUserWithoutPermission() {
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("a");
        User authenticatedUser = new User("test user", "can run as");
        final Authentication authentication = createAuthentication(new User("run as me", "b"), authenticatedUser);
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

        assertThrowsAuthorizationExceptionRunAsUnauthorizedAction(
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

    public void testRunAsRequestWithValidPermissions() {
        TransportRequest request = new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices("b");
        User authenticatedUser = new User("test user", "can run as");
        final Authentication authentication = createAuthentication(new User("run as me", "b"), authenticatedUser);
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

    public void testGrantAllRestrictedUserCannotExecuteOperationAgainstSecurityIndices() {
        RoleDescriptor role = new RoleDescriptor(
            "all access",
            new String[] { "all" },
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("all").build() },
            null
        );
        final Authentication authentication = createAuthentication(new User("all_access_user", "all_access"));
        roleMap.put("all_access", role);
        ClusterState state = mockClusterState(
            ProjectMetadata.builder(projectId)
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
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
            new Tuple<>(
                TransportBulkAction.NAME + "[s]",
                new DeleteRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(
                TransportUpdateAction.NAME,
                new UpdateRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(TransportBulkAction.NAME + "[s]", new IndexRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7)))
        );
        requests.add(
            new Tuple<>(
                TransportSearchAction.TYPE.name(),
                new SearchRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                TermVectorsAction.NAME,
                new TermVectorsRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(
                TransportGetAction.TYPE.name(),
                new GetRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(
                TransportIndicesAliasesAction.NAME,
                new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                    AliasActions.add().alias("security_alias").index(INTERNAL_SECURITY_MAIN_INDEX_7)
                )
            )
        );
        requests.add(
            new Tuple<>(
                TransportUpdateSettingsAction.TYPE.name(),
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
                new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                TransportIndicesShardStoresAction.TYPE.name(),
                new IndicesShardStoresRequest().indices(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
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
        ClusterHealthRequest request = new ClusterHealthRequest(
            TEST_REQUEST_TIMEOUT,
            randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7)
        );
        authorize(authentication, TransportClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportClusterHealthAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );

        // multiple indices
        request = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7, "foo", "bar");
        authorize(authentication, TransportClusterHealthAction.NAME, request);
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(TransportClusterHealthAction.NAME),
            eq(request),
            authzInfoRoles(new String[] { role.getName() })
        );
        verifyNoMoreInteractions(auditTrail);

        final SearchRequest searchRequest = new SearchRequest("_all");
        authorize(authentication, TransportSearchAction.TYPE.name(), searchRequest);
        assertEquals(2, searchRequest.indices().length);
        assertEquals(IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_LIST, asList(searchRequest.indices()));
    }

    public void testMonitoringOperationsAgainstSecurityIndexRequireAllowRestricted() {
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
            ProjectMetadata.builder(projectId)
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
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
        requests.add(new Tuple<>(GetSettingsAction.NAME, new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(SECURITY_MAIN_ALIAS)));
        requests.add(
            new Tuple<>(TransportIndicesShardStoresAction.TYPE.name(), new IndicesShardStoresRequest().indices(SECURITY_MAIN_ALIAS))
        );

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

    public void testSuperusersCanExecuteReadOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        mockClusterState(
            ProjectMetadata.builder(projectId)
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
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
            new Tuple<>(
                TransportSearchAction.TYPE.name(),
                new SearchRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                TermVectorsAction.NAME,
                new TermVectorsRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(
                TransportGetAction.TYPE.name(),
                new GetRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), "id")
            )
        );
        requests.add(
            new Tuple<>(
                TransportClusterHealthAction.NAME,
                new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                TransportClusterHealthAction.NAME,
                new ClusterHealthRequest(
                    TEST_REQUEST_TIMEOUT,
                    randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7),
                    "foo",
                    "bar"
                )
            )
        );

        for (final Tuple<String, TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            try (StoredContext ignore = threadContext.newStoredContext()) {
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

    public void testSuperusersCannotExecuteWriteOperationAgainstSecurityIndex() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        mockClusterState(
            ProjectMetadata.builder(projectId)
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
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
            new Tuple<>(
                TransportBulkAction.NAME + "[s]",
                createBulkShardRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), DeleteRequest::new)
            )
        );
        requests.add(
            new Tuple<>(
                TransportBulkAction.NAME + "[s]",
                createBulkShardRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7), UpdateRequest::new)
            )
        );
        requests.add(
            new Tuple<>(
                TransportBulkAction.NAME + "[s]",
                createBulkShardRequest(
                    randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7),
                    (index, id) -> new IndexRequest(index).id(id)
                )
            )
        );
        requests.add(
            new Tuple<>(
                TransportIndicesAliasesAction.NAME,
                new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                    AliasActions.add().alias("security_alias").index(INTERNAL_SECURITY_MAIN_INDEX_7)
                )
            )
        );
        requests.add(
            new Tuple<>(
                TransportPutMappingAction.TYPE.name(),
                new PutMappingRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        requests.add(
            new Tuple<>(
                TransportDeleteIndexAction.TYPE.name(),
                new DeleteIndexRequest(randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7))
            )
        );
        for (final Tuple<String, TransportRequest> requestTuple : requests) {
            final String action = requestTuple.v1();
            final TransportRequest request = requestTuple.v2();
            try (StoredContext ignore = threadContext.newStoredContext()) {
                final Authentication authentication = createAuthentication(superuser);
                assertThrowsAuthorizationException(
                    "authentication=[" + authentication + "], action=[" + action + "], request=[" + request + "]",
                    () -> authorize(authentication, action, request),
                    action,
                    superuser.principal()
                );
                verify(auditTrail).accessDenied(
                    eq(requestId),
                    eq(authentication),
                    eq(action),
                    eq(request),
                    authzInfoRoles(superuser.roles())
                );
            }
        }
    }

    public void testSuperusersCanExecuteReadOperationAgainstSecurityIndexWithWildcard() {
        final User superuser = new User("custom_admin", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        final Authentication authentication = createAuthentication(superuser);
        roleMap.put(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName(), ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR);
        mockClusterState(
            ProjectMetadata.builder(projectId)
                .put(
                    new IndexMetadata.Builder(INTERNAL_SECURITY_MAIN_INDEX_7).putAlias(
                        new AliasMetadata.Builder(SECURITY_MAIN_ALIAS).build()
                    )
                        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(),
                    true
                )
                .build()
        );
        final String requestId = AuditUtil.getOrGenerateRequestId(threadContext);

        String action = TransportSearchAction.TYPE.name();
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

    public void testCompositeActionsIndicesAreNotChecked() {
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

    public void testCompositeActionsMustImplementCompositeIndicesRequest() {
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

    public void testCompositeActionsIndicesAreCheckedAtTheShardLevel() {
        final MockIndicesRequest mockRequest = new MockIndicesRequest(IndicesOptions.strictExpandOpen(), "index");
        final TransportRequest request;
        final String action;
        switch (randomIntBetween(0, 4)) {
            case 0 -> {
                action = TransportMultiGetAction.NAME + "[shard]";
                request = mockRequest;
            }
            case 1 -> {
                // reindex, msearch, search template, and multi search template delegate to search
                action = TransportSearchAction.TYPE.name();
                request = mockRequest;
            }
            case 2 -> {
                action = MultiTermVectorsAction.NAME + "[shard]";
                request = mockRequest;
            }
            case 3 -> {
                action = TransportBulkAction.NAME + "[s]";
                request = createBulkShardRequest("index", (index, id) -> new IndexRequest(index).id(id));
            }
            case 4 -> {
                action = "indices:data/read/mpercolate[s]";
                request = mockRequest;
            }
            default -> throw new UnsupportedOperationException();
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
        try (StoredContext ignore = threadContext.newStoredContext()) {
            authorize(createAuthentication(userAllowed), action, request);
        }
        assertThrowsAuthorizationException(() -> authorize(createAuthentication(userDenied), action, request), action, "userDenied");
    }

    public void testAuthorizationOfSingleActionMultipleIndicesBulkItems() {
        final String action = TransportBulkAction.NAME + "[s]";
        final BulkItemRequest[] items;
        final DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());
        // the "good role" authorizes all the bulk items
        final RoleDescriptor goodRole;
        // the "bad role" does not authorize any bulk items
        final RoleDescriptor badRole;
        final AtomicInteger idCounter = new AtomicInteger();
        final Set<String> allIndexNames = new HashSet<>();
        final Supplier<String> indexNameSupplier = () -> {
            String indexName = randomAlphaOfLengthBetween(1, 4);
            allIndexNames.add(indexName);
            return indexName;
        };
        // build a request with bulk items of the same action type, but multiple index names
        switch (opType) {
            case INDEX -> {
                items = randomArray(
                    1,
                    8,
                    BulkItemRequest[]::new,
                    () -> new BulkItemRequest(
                        idCounter.get(),
                        new IndexRequest(indexNameSupplier.get()).id("id" + idCounter.incrementAndGet())
                            .opType(DocWriteRequest.OpType.INDEX)
                    )
                );
                goodRole = new RoleDescriptor(
                    "good-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("all", "create", "index", "write"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
                badRole = new RoleDescriptor(
                    "bad-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("create_doc", "delete"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
            }
            case CREATE -> {
                items = randomArray(
                    1,
                    8,
                    BulkItemRequest[]::new,
                    () -> new BulkItemRequest(
                        idCounter.get(),
                        new IndexRequest(indexNameSupplier.get()).id("id" + idCounter.incrementAndGet())
                            .opType(DocWriteRequest.OpType.CREATE)
                    )
                );
                goodRole = new RoleDescriptor(
                    "good-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("all", "create_doc", "create", "index", "write"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
                badRole = new RoleDescriptor(
                    "bad-role",
                    null,
                    allIndexNames.stream()
                        .map(indexName -> IndicesPrivileges.builder().indices(indexName).privileges("delete").build())
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
            }
            case DELETE -> {
                items = randomArray(
                    1,
                    8,
                    BulkItemRequest[]::new,
                    () -> new BulkItemRequest(
                        idCounter.get(),
                        new DeleteRequest(indexNameSupplier.get(), "id" + idCounter.incrementAndGet())
                    )
                );
                goodRole = new RoleDescriptor(
                    "good-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("all", "delete", "write"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
                badRole = new RoleDescriptor(
                    "bad-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("index", "create", "create_doc"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
            }
            case UPDATE -> {
                items = randomArray(
                    1,
                    8,
                    BulkItemRequest[]::new,
                    () -> new BulkItemRequest(
                        idCounter.get(),
                        new UpdateRequest(indexNameSupplier.get(), "id" + idCounter.incrementAndGet())
                    )
                );
                goodRole = new RoleDescriptor(
                    "good-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("all", "index", "write"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
                badRole = new RoleDescriptor(
                    "bad-role",
                    null,
                    allIndexNames.stream()
                        .map(
                            indexName -> IndicesPrivileges.builder()
                                .indices(indexName)
                                .privileges(randomFrom("create", "create_doc", "delete"))
                                .build()
                        )
                        .toArray(IndicesPrivileges[]::new),
                    null
                );
            }
            default -> throw new IllegalStateException("Unexpected value: " + opType);
        }
        roleMap.put("good-role", goodRole);
        roleMap.put("bad-role", badRole);

        final ShardId shardId = new ShardId("some-concrete-shard-index-name", UUID.randomUUID().toString(), 1);
        final BulkShardRequest request = new BulkShardRequest(shardId, randomFrom(WriteRequest.RefreshPolicy.values()), items);

        mockEmptyMetadata();
        final Authentication authentication;
        final String requestId;
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("user", "good-role"));
            requestId = AuditUtil.getOrGenerateRequestId(threadContext);
            authorize(authentication, action, request);
        }

        // bulk shard request is authorized
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { goodRole.getName() })
        );
        // there's only one "access granted" record for all the bulk items
        verify(auditTrail).explicitIndexAccessEvent(eq(requestId), eq(AuditLevel.ACCESS_GRANTED), eq(authentication), eq(switch (opType) {
            case INDEX -> TransportIndexAction.NAME + ":op_type/index";
            case CREATE -> TransportIndexAction.NAME + ":op_type/create";
            case UPDATE -> TransportUpdateAction.NAME;
            case DELETE -> TransportDeleteAction.NAME;
        }),
            argThat(indicesArrays -> indicesArrays.length == allIndexNames.size() && allIndexNames.containsAll(asList(indicesArrays))),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { goodRole.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        // all bulk items go through as authorized
        for (BulkItemRequest bulkItemRequest : request.items()) {
            assertThat(bulkItemRequest.getPrimaryResponse(), nullValue());
        }

        final Authentication badAuthentication;
        final String badRequestId;
        try (StoredContext ignore = threadContext.stashContext()) {
            badAuthentication = createAuthentication(new User("bad-user", "bad-role"));
            badRequestId = AuditUtil.getOrGenerateRequestId(threadContext);
            // the bulk shard request is authorized, but the bulk items are not
            authorize(badAuthentication, action, request);
        }
        // bulk shard request is authorized
        verify(auditTrail).accessGranted(
            eq(badRequestId),
            eq(badAuthentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { badRole.getName() })
        );
        // there's only one "access denied" record for all the bulk items
        verify(auditTrail).explicitIndexAccessEvent(
            eq(badRequestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(badAuthentication),
            eq(switch (opType) {
                case INDEX -> TransportIndexAction.NAME + ":op_type/index";
                case CREATE -> TransportIndexAction.NAME + ":op_type/create";
                case UPDATE -> TransportUpdateAction.NAME;
                case DELETE -> TransportDeleteAction.NAME;
            }),
            argThat(indicesArrays -> indicesArrays.length == allIndexNames.size() && allIndexNames.containsAll(asList(indicesArrays))),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { badRole.getName() })
        );
        verifyNoMoreInteractions(auditTrail);
        // all bulk items are failures
        for (BulkItemRequest bulkItemRequest : request.items()) {
            assertThat(bulkItemRequest.getPrimaryResponse().isFailed(), is(true));
        }
    }

    public void testAuthorizationOfMultipleActionsSingleIndexBulkItems() {
        final String action = TransportBulkAction.NAME + "[s]";
        final AtomicInteger idCounter = new AtomicInteger();
        final Set<String> actionTypes = new HashSet<>();
        final Set<Integer> deleteItems = new HashSet<>();
        final String indexName = randomAlphaOfLengthBetween(1, 4);
        final BulkItemRequest[] items = randomArray(1, 8, BulkItemRequest[]::new, () -> {
            switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> {
                    actionTypes.add(TransportIndexAction.NAME + ":op_type/index");
                    return new BulkItemRequest(
                        idCounter.get(),
                        new IndexRequest(indexName).id("id" + idCounter.incrementAndGet()).opType(DocWriteRequest.OpType.INDEX)
                    );
                }
                case CREATE -> {
                    actionTypes.add(TransportIndexAction.NAME + ":op_type/create");
                    return new BulkItemRequest(
                        idCounter.get(),
                        new IndexRequest(indexName).id("id" + idCounter.incrementAndGet()).opType(DocWriteRequest.OpType.CREATE)
                    );
                }
                case DELETE -> {
                    actionTypes.add(TransportDeleteAction.NAME);
                    deleteItems.add(idCounter.get());
                    return new BulkItemRequest(idCounter.get(), new DeleteRequest(indexName, "id" + idCounter.incrementAndGet()));
                }
                case UPDATE -> {
                    actionTypes.add(TransportUpdateAction.NAME);
                    return new BulkItemRequest(idCounter.get(), new UpdateRequest(indexName, "id" + idCounter.incrementAndGet()));
                }
                default -> throw new IllegalStateException("Unexpected value");
            }
        });
        RoleDescriptor allRole = new RoleDescriptor(
            "all-role",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices(indexName).privileges(randomFrom("all", "write")).build() },
            null
        );
        RoleDescriptor indexRole = new RoleDescriptor(
            "index-role",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices(indexName).privileges("index").build() },
            null
        );
        roleMap.put("all-role", allRole);
        roleMap.put("index-role", indexRole);

        final ShardId shardId = new ShardId(indexName, UUID.randomUUID().toString(), 1);
        final BulkShardRequest request = new BulkShardRequest(shardId, randomFrom(WriteRequest.RefreshPolicy.values()), items);

        mockEmptyMetadata();
        final Authentication authentication;
        final String requestId;
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("user", "all-role"));
            requestId = AuditUtil.getOrGenerateRequestId(threadContext);
            authorize(authentication, action, request);
        }
        // bulk shard request is authorized
        verify(auditTrail).accessGranted(
            eq(requestId),
            eq(authentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { allRole.getName() })
        );
        // there's one granted audit entry for each action type
        actionTypes.forEach(actionType -> {
            verify(auditTrail).explicitIndexAccessEvent(
                eq(requestId),
                eq(AuditLevel.ACCESS_GRANTED),
                eq(authentication),
                eq(actionType),
                eq(new String[] { indexName }),
                eq(BulkItemRequest.class.getSimpleName()),
                eq(request.remoteAddress()),
                authzInfoRoles(new String[] { allRole.getName() })
            );
        });
        verifyNoMoreInteractions(auditTrail);
        // all bulk items go through as authorized
        for (BulkItemRequest bulkItemRequest : request.items()) {
            assertThat(bulkItemRequest.getPrimaryResponse(), nullValue());
        }

        // use the "index" role
        final Authentication indexAuthentication;
        final String indexRequestId;
        try (StoredContext ignore = threadContext.stashContext()) {
            indexAuthentication = createAuthentication(new User("index-user", "index-role"));
            indexRequestId = AuditUtil.getOrGenerateRequestId(threadContext);
            authorize(indexAuthentication, action, request);
        }
        // bulk shard request is authorized
        verify(auditTrail).accessGranted(
            eq(indexRequestId),
            eq(indexAuthentication),
            eq(action),
            eq(request),
            authzInfoRoles(new String[] { indexRole.getName() })
        );
        // there's a single granted audit entry for each action type, less the delete action (which is denied)
        actionTypes.forEach(actionType -> {
            if (actionType.equals(TransportDeleteAction.NAME) == false) {
                verify(auditTrail).explicitIndexAccessEvent(
                    eq(indexRequestId),
                    eq(AuditLevel.ACCESS_GRANTED),
                    eq(indexAuthentication),
                    eq(actionType),
                    eq(new String[] { indexName }),
                    eq(BulkItemRequest.class.getSimpleName()),
                    eq(request.remoteAddress()),
                    authzInfoRoles(new String[] { indexRole.getName() })
                );
            }
        });
        if (deleteItems.isEmpty() == false) {
            // there's one denied audit entry for all the delete action types
            verify(auditTrail).explicitIndexAccessEvent(
                eq(indexRequestId),
                eq(AuditLevel.ACCESS_DENIED),
                eq(indexAuthentication),
                eq(TransportDeleteAction.NAME),
                eq(new String[] { indexName }),
                eq(BulkItemRequest.class.getSimpleName()),
                eq(request.remoteAddress()),
                authzInfoRoles(new String[] { indexRole.getName() })
            );
        }
        verifyNoMoreInteractions(auditTrail);
        for (BulkItemRequest bulkItemRequest : request.items()) {
            if (deleteItems.contains(bulkItemRequest.id())) {
                assertThat(bulkItemRequest.getPrimaryResponse().isFailed(), is(true));
            } else {
                assertThat(bulkItemRequest.getPrimaryResponse(), nullValue());
            }
        }
    }

    public void testAuthorizationOfIndividualIndexAndDeleteBulkItems() {
        final String action = TransportBulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new DeleteRequest("concrete-index", "c1")),
            new BulkItemRequest(2, new IndexRequest("concrete-index").id("c2")),
            new BulkItemRequest(3, new DeleteRequest("alias-1", "a1a")),
            new BulkItemRequest(4, new IndexRequest("alias-1").id("a1b")),
            new BulkItemRequest(5, new DeleteRequest("alias-2", "a2a")),
            new BulkItemRequest(6, new IndexRequest("alias-2").id("a2b")) };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final BulkShardRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

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
            eq(TransportDeleteAction.NAME),
            argThat(indicesArrays -> {
                Arrays.sort(indicesArrays);
                return Arrays.equals(indicesArrays, new String[] { "alias-2", "concrete-index" });
            }),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(TransportIndexAction.NAME + ":op_type/index"),
            argThat(indicesArrays -> {
                Arrays.sort(indicesArrays);
                return Arrays.equals(indicesArrays, new String[] { "alias-1", "concrete-index" });
            }),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(TransportDeleteAction.NAME),
            eq(new String[] { "alias-1" }),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(TransportIndexAction.NAME + ":op_type/index"),
            eq(new String[] { "alias-2" }),
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
        assertThat(request.items()[0].getPrimaryResponse(), nullValue());
        assertThat(request.items()[1].getPrimaryResponse(), nullValue());
        assertThat(request.items()[2].getPrimaryResponse().isFailed(), is(true));
        assertThat(request.items()[3].getPrimaryResponse(), nullValue());
        assertThat(request.items()[4].getPrimaryResponse(), nullValue());
        assertThat(request.items()[5].getPrimaryResponse().isFailed(), is(true));
    }

    public void testAuthorizationOfIndividualBulkItemsWithDateMath() {
        final String action = TransportBulkAction.NAME + "[s]";
        final BulkItemRequest[] items = {
            new BulkItemRequest(1, new IndexRequest("<datemath-{now/M{YYYY}}>").id("dy1")),
            new BulkItemRequest(2, new DeleteRequest("<datemath-{now/d{YYYY}}>", "dy2")), // resolves to same as above
            new BulkItemRequest(3, new IndexRequest("<datemath-{now/M{YYYY.MM}}>").id("dm1")),
            new BulkItemRequest(4, new DeleteRequest("<datemath-{now/d{YYYY.MM}}>", "dm2")), // resolves to same as above
        };
        final ShardId shardId = new ShardId("concrete-index", UUID.randomUUID().toString(), 1);
        final BulkShardRequest request = new BulkShardRequest(shardId, WriteRequest.RefreshPolicy.IMMEDIATE, items);

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
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_DENIED),
            eq(authentication),
            eq(TransportDeleteAction.NAME),
            argThat(indices -> indices.length == 2 && indices[0].startsWith("datemath-") && indices[1].startsWith("datemath-")),
            eq(BulkItemRequest.class.getSimpleName()),
            eq(request.remoteAddress()),
            authzInfoRoles(new String[] { role.getName() })
        );
        // both indexing should go through
        verify(auditTrail).explicitIndexAccessEvent(
            eq(requestId),
            eq(AuditLevel.ACCESS_GRANTED),
            eq(authentication),
            eq(TransportIndexAction.NAME + ":op_type/index"),
            argThat(indices -> indices.length == 2 && indices[0].startsWith("datemath-") && indices[1].startsWith("datemath-")),
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
        assertThat(request.items()[0].getPrimaryResponse(), nullValue());
        assertThat(request.items()[1].getPrimaryResponse().isFailed(), is(true));
        assertThat(request.items()[2].getPrimaryResponse(), nullValue());
        assertThat(request.items()[3].getPrimaryResponse().isFailed(), is(true));
    }

    private BulkShardRequest createBulkShardRequest(String indexName, BiFunction<String, String, DocWriteRequest<?>> req) {
        final BulkItemRequest[] items = { new BulkItemRequest(1, req.apply(indexName, "id")) };
        return new BulkShardRequest(new ShardId(indexName, UUID.randomUUID().toString(), 1), WriteRequest.RefreshPolicy.IMMEDIATE, items);
    }

    private static Tuple<String, TransportRequest> randomCompositeRequest() {
        return switch (randomIntBetween(0, 7)) {
            case 0 -> Tuple.tuple(TransportMultiGetAction.NAME, new MultiGetRequest().add("index", "id"));
            case 1 -> Tuple.tuple(TransportMultiSearchAction.TYPE.name(), new MultiSearchRequest().add(new SearchRequest()));
            case 2 -> Tuple.tuple(MultiTermVectorsAction.NAME, new MultiTermVectorsRequest().add("index", "id"));
            case 3 -> Tuple.tuple(TransportBulkAction.NAME, new BulkRequest().add(new DeleteRequest("index", "id")));
            case 4 -> Tuple.tuple("indices:data/read/mpercolate", new MockCompositeIndicesRequest());
            case 5 -> Tuple.tuple("indices:data/read/msearch/template", new MockCompositeIndicesRequest());
            case 6 -> Tuple.tuple("indices:data/read/search/template", new MockCompositeIndicesRequest());
            case 7 -> Tuple.tuple("indices:data/write/reindex", new MockCompositeIndicesRequest());
            default -> throw new UnsupportedOperationException();
        };
    }

    private static class MockCompositeIndicesRequest extends AbstractTransportRequest implements CompositeIndicesRequest {}

    private Authentication createAuthentication(User user) {
        return createAuthentication(user, null);
    }

    private Authentication createAuthentication(User user, @Nullable User authenticatingUser) {
        final Authentication authentication;
        if (user instanceof InternalUser internalUser) {
            assert authenticatingUser == null;
            authentication = AuthenticationTestHelper.builder().internal(internalUser).build();
        } else if (user instanceof AnonymousUser) {
            assert authenticatingUser == null;
            authentication = AuthenticationTestHelper.builder().anonymous(user).build(false);
        } else {
            if (authenticatingUser != null) {
                authentication = AuthenticationTestHelper.builder()
                    .user(authenticatingUser)
                    .realmRef(new RealmRef("test", "test", "foo"))
                    .runAs()
                    .user(user)
                    .realmRef(new RealmRef("looked", "up", "by"))
                    .build();
            } else {
                authentication = AuthenticationTestHelper.builder().user(user).realmRef(new RealmRef("test", "test", "foo")).build(false);
            }
        }
        try {
            authentication.writeToContext(threadContext);
        } catch (IOException e) {
            throw new UncheckedIOException("caught unexpected IOException", e);
        }
        return authentication;
    }

    private ClusterState mockEmptyMetadata() {
        return mockClusterState(ProjectMetadata.builder(projectId).build());
    }

    private ClusterState mockMetadataWithIndex(String indexName) {
        final IndexMetadata indexMetadata = createIndexMetadata(indexName);
        final ProjectMetadata project = ProjectMetadata.builder(projectId).put(indexMetadata, true).build();
        return mockClusterState(project);
    }

    private static IndexMetadata createIndexMetadata(String indexName) {
        return new IndexMetadata.Builder(indexName).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_INDEX_UUID, randomUUID())
                .build()
        ).numberOfShards(1).numberOfReplicas(0).build();
    }

    private ClusterState mockClusterState(ProjectMetadata project) {
        final Metadata metadata = Metadata.builder().put(project).build();
        return mockClusterState(metadata);
    }

    private ClusterState mockClusterState(Metadata metadata) {
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        return state;
    }

    public void testProxyRequestFailsOnNonProxyAction() {
        TransportRequest request = new EmptyRequest();
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
        TransportRequest transportRequest = TransportActionProxy.wrapRequest(node, request);
        AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("test user", "role");
        final var authentication = createAuthentication(user);
        assertEquals(
            """
                originalRequest is a proxy request for: [org.elasticsearch.transport.EmptyRequest/unset] \
                but action: [indices:some/action] isn't""",
            expectThrows(
                ElasticsearchSecurityException.class,
                IllegalStateException.class,
                () -> authorize(authentication, "indices:some/action", transportRequest)
            ).getMessage()
        );
    }

    public void testProxyRequestFailsOnNonProxyRequest() {
        TransportRequest request = new EmptyRequest();
        User user = new User("test user", "role");
        AuditUtil.getOrGenerateRequestId(threadContext);
        final var authentication = createAuthentication(user);
        assertEquals(
            """
                originalRequest is not a proxy request: [org.elasticsearch.transport.EmptyRequest/unset] \
                but action: [internal:transport/proxy/indices:some/action] is a proxy action""",
            expectThrows(
                ElasticsearchSecurityException.class,
                IllegalStateException.class,
                () -> authorize(authentication, TransportActionProxy.getProxyAction("indices:some/action"), request)
            ).getMessage()
        );
    }

    public void testProxyRequestAuthenticationDenied() {
        final TransportRequest proxiedRequest = new SearchRequest();
        final DiscoveryNode node = DiscoveryNodeUtils.create("foo");
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
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");

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
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
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

    public void testProxyRequestAuthenticationDeniedWithReadPrivileges() {
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
        DiscoveryNode node = DiscoveryNodeUtils.create("foo");
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

    @SuppressWarnings("unchecked")
    public void testAuthorizationEngineSelectionForCheckPrivileges() throws Exception {
        AuthorizationEngine engine = mock(AuthorizationEngine.class);
        MockLicenseState licenseState = mock(MockLicenseState.class);
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
            indexNameExpressionResolver,
            operatorPrivilegesService,
            RESTRICTED_INDICES,
            new AuthorizationDenialMessages.Default(),
            projectResolver
        );

        Subject subject = new Subject(new User("test", "a role"), mock(RealmRef.class));
        AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        doAnswer(i -> {
            assertThat(i.getArguments().length, equalTo(2));
            final Object arg1 = i.getArguments()[0];
            assertThat(arg1, instanceOf(Subject.class));
            Subject subjectArg = (Subject) arg1;
            final Object arg2 = i.getArguments()[1];
            assertThat(arg2, instanceOf(ActionListener.class));
            ActionListener<AuthorizationInfo> listener = (ActionListener<AuthorizationInfo>) arg2;
            if (subjectArg.equals(subject)) {
                listener.onResponse(authorizationInfo);
            } else {
                listener.onResponse(null);
            }
            return null;
        }).when(engine).resolveAuthorizationInfo(any(Subject.class), anyActionListener());
        AuthorizationEngine.PrivilegesCheckResult privilegesCheckResult = randomFrom(
            ALL_CHECKS_SUCCESS_NO_DETAILS,
            SOME_CHECKS_FAILURE_NO_DETAILS
        );
        doAnswer(i -> {
            assertThat(i.getArguments().length, equalTo(4));
            final Object arg1 = i.getArguments()[0];
            assertThat(arg1, instanceOf(AuthorizationInfo.class));
            AuthorizationInfo authorizationInfoArg = (AuthorizationInfo) arg1;
            final Object arg4 = i.getArguments()[3];
            assertThat(arg4, instanceOf(ActionListener.class));
            ActionListener<AuthorizationEngine.PrivilegesCheckResult> listener = (ActionListener<
                AuthorizationEngine.PrivilegesCheckResult>) arg4;
            if (authorizationInfoArg.equals(authorizationInfo)) {
                listener.onResponse(privilegesCheckResult);
            } else {
                listener.onResponse(null);
            }
            return null;
        }).when(engine)
            .checkPrivileges(
                any(AuthorizationInfo.class),
                any(AuthorizationEngine.PrivilegesToCheck.class),
                anyCollection(),
                anyActionListener()
            );

        PlainActionFuture<AuthorizationEngine.PrivilegesCheckResult> future = new PlainActionFuture<>();
        authorizationService.checkPrivileges(
            subject,
            new AuthorizationEngine.PrivilegesToCheck(
                new String[0],
                new IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[0],
                randomBoolean()
            ),
            List.of(),
            future
        );
        assertThat(future.get(), is(privilegesCheckResult));
    }

    public void testAuthorizationEngineSelection() {
        final AuthorizationEngine engine = new AuthorizationEngine() {
            @Override
            public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void resolveAuthorizationInfo(Subject subject, ActionListener<AuthorizationInfo> listener) {
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
            public SubscribableListener<IndexAuthorizationResult> authorizeIndexAction(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                ProjectMetadata metadata
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void loadAuthorizedIndices(
                RequestInfo requestInfo,
                AuthorizationInfo authorizationInfo,
                Map<String, IndexAbstraction> indicesLookup,
                ActionListener<AuthorizedIndices> listener
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
                AuthorizationInfo authorizationInfo,
                PrivilegesToCheck privilegesToCheck,
                Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                ActionListener<PrivilegesCheckResult> listener
            ) {
                throw new UnsupportedOperationException("not implemented");
            }

            @Override
            public void getUserPrivileges(AuthorizationInfo authorizationInfo, ActionListener<GetUserPrivilegesResponse> listener) {
                throw new UnsupportedOperationException("not implemented");
            }
        };

        MockLicenseState licenseState = mock(MockLicenseState.class);
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
            indexNameExpressionResolver,
            operatorPrivilegesService,
            RESTRICTED_INDICES,
            new AuthorizationDenialMessages.Default(),
            projectResolver
        );
        Authentication authentication;
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("test user", "a_all"));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("runas", "runas_role"), new User("runner", "runner_role"));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("runas", "runas_role"), new ElasticUser(true));
            assertEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertNotEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("elastic", "superuser"), new User("runner", "runner_role"));
            assertNotEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(new User("kibana", "kibana_system"), new ElasticUser(true));
            assertNotEquals(engine, authorizationService.getAuthorizationEngine(authentication));
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertNotEquals(engine, authorizationService.getRunAsAuthorizationEngine(authentication));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(false);
            assertThat(authorizationService.getAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
            assertThat(authorizationService.getRunAsAuthorizationEngine(authentication), instanceOf(RBACEngine.class));
        }

        when(licenseState.isAllowed(Security.AUTHORIZATION_ENGINE_FEATURE)).thenReturn(true);
        try (StoredContext ignore = threadContext.stashContext()) {
            authentication = createAuthentication(
                randomFrom(InternalUsers.XPACK_USER, InternalUsers.XPACK_SECURITY_USER, new ElasticUser(true), new KibanaUser(true))
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

    public void testRemoteActionDenied() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(
            Map.of(PRINCIPAL_ROLES_FIELD_NAME, randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(5, 8)))
        );
        String actionPrefix = randomFrom("indices", "cluster");
        threadContext.putTransient(AUTHORIZATION_INFO_KEY, authorizationInfo);
        final String action = actionPrefix + ":/some/action/" + randomAlphaOfLengthBetween(0, 8);
        final String clusterAlias = randomAlphaOfLengthBetween(5, 12);
        final ElasticsearchSecurityException e = authorizationService.remoteActionDenied(authentication, action, clusterAlias);
        assertThat(e.getCause(), nullValue());
        assertThat(
            e.getMessage(),
            equalTo(
                Strings.format(
                    "action [%s] towards remote cluster [%s] is unauthorized for %s"
                        + " because no remote %s privileges apply for the target cluster",
                    action,
                    clusterAlias,
                    new AuthorizationDenialMessages.Default().successfulAuthenticationDescription(authentication, authorizationInfo),
                    actionPrefix
                )
            )
        );
    }

    public void testActionDeniedForCrossClusterAccessAuthentication() {
        final Authentication authentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
        final AuthorizationInfo authorizationInfo = mock(AuthorizationInfo.class);
        when(authorizationInfo.asMap()).thenReturn(
            Map.of(PRINCIPAL_ROLES_FIELD_NAME, randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(5, 8)))
        );
        threadContext.putTransient(AUTHORIZATION_INFO_KEY, authorizationInfo);
        String actionPrefix = randomFrom("indices", "cluster");
        final String action = actionPrefix + ":/some/action/" + randomAlphaOfLengthBetween(0, 8);
        final ElasticsearchSecurityException e = authorizationService.actionDenied(authentication, authorizationInfo, action, mock());
        assertThat(e.getCause(), nullValue());
        assertThat(
            e.getMessage(),
            containsString(
                Strings.format(
                    "action [%s] towards remote cluster is unauthorized for %s",
                    action,
                    new AuthorizationDenialMessages.Default().successfulAuthenticationDescription(authentication, authorizationInfo)
                )
            )
        );
    }

    public void testRoleRestrictionAccessDenial() {
        Tuple<String, TransportRequest> tuple = randomFrom(
            asList(
                new Tuple<>(TransportSearchAction.TYPE.name(), new SearchRequest()),
                new Tuple<>(SqlQueryAction.NAME, new SqlQueryRequest())
            )
        );
        String action = tuple.v1();
        TransportRequest request = tuple.v2();
        AuditUtil.getOrGenerateRequestId(threadContext);
        mockEmptyMetadata();

        final Authentication apiKeyAuthentication = Authentication.newApiKeyAuthentication(
            AuthenticationResult.success(new User(randomAlphaOfLengthBetween(3, 8)), Map.of(API_KEY_ID_KEY, randomAlphaOfLength(20))),
            randomAlphaOfLengthBetween(3, 8)
        );
        final Role role = Role.EMPTY_RESTRICTED_BY_WORKFLOW;
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Role> listener = (ActionListener<Role>) invocationOnMock.getArguments()[1];
            listener.onResponse(role);
            return null;
        }).when(rolesStore).getRole(any(Subject.class), anyActionListener());

        ElasticsearchSecurityException securityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(apiKeyAuthentication, action, request)
        );
        assertThat(securityException, throwableWithMessage(containsString("[" + action + "] is unauthorized for API key")));
        assertThat(securityException.getRootCause(), throwableWithMessage(containsString("access restricted by workflow")));
    }

    static AuthorizationInfo authzInfoRoles(String[] expectedRoles) {
        return argThat(new RBACAuthorizationInfoRoleMatcher(expectedRoles));
    }

    private static class TestSearchPhaseResult extends SearchPhaseResult {
        final DiscoveryNode node;

        TestSearchPhaseResult(ShardSearchContextId contextId, DiscoveryNode node) {
            this.contextId = contextId;
            this.node = node;
        }
    }

    private static BytesReference createEncodedPIT(Index index) {
        DiscoveryNode node1 = DiscoveryNodeUtils.create("node_1");
        TestSearchPhaseResult testSearchPhaseResult1 = new TestSearchPhaseResult(new ShardSearchContextId("a", 1), node1);
        testSearchPhaseResult1.setSearchShardTarget(
            new SearchShardTarget("node_1", new ShardId(index.getName(), index.getUUID(), 0), null)
        );
        List<SearchPhaseResult> results = new ArrayList<>();
        results.add(testSearchPhaseResult1);
        return SearchContextId.encode(results, Collections.emptyMap(), TransportVersion.current(), ShardSearchFailure.EMPTY_ARRAY);
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
