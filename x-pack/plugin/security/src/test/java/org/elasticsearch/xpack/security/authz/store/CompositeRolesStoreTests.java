/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Level;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ActionClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexComponentSelectorPredicate;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilegeTests;
import org.elasticsearch.xpack.core.security.authz.privilege.NamedClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReferenceIntersection;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowServiceTests.TestBaseRestHandler;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.security.authc.ApiKeyServiceTests.Utils.createApiKeyAuthentication;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CompositeRolesStoreTests extends ESTestCase {

    private static final Settings SECURITY_ENABLED_SETTINGS = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();

    private final FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
    private final String concreteSecurityIndexName = randomFrom(
        TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
        TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
    );

    private Executor mockRoleBuildingExecutor;

    @Before
    public void setup() {
        mockRoleBuildingExecutor = mock(Executor.class);
        Mockito.doAnswer(invocationOnMock -> {
            final AbstractRunnable actionRunnable = (AbstractRunnable) invocationOnMock.getArguments()[0];
            actionRunnable.run();
            return null;
        }).when(mockRoleBuildingExecutor).execute(any(Runnable.class));
    }

    @After
    public void clear() {
        clearInvocations(mockRoleBuildingExecutor);
    }

    public void testRolesWhenDlsFlsUnlicensed() throws IOException {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().grantedFields("*").deniedFields("foo").indices("*").privileges("read").build() },
            null
        );
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);
        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("read").query(matchAllBytes).build() },
            null
        );
        RoleDescriptor flsDlsRole = new RoleDescriptor(
            "fls_dls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("read")
                    .grantedFields("*")
                    .deniedFields("foo")
                    .query(matchAllBytes)
                    .build() },
            null
        );
        RoleDescriptor noFlsDlsRole = new RoleDescriptor(
            "no_fls_dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("read").build() },
            null
        );
        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());

        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            fileRolesStore,
            null,
            null,
            null,
            licenseState,
            null,
            null,
            null,
            effectiveRoleDescriptors::set
        );

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("fls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("dls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("fls_dls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("no_fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(noFlsDlsRole));
        effectiveRoleDescriptors.set(null);
    }

    public void testLoggingWarnWhenDlsUnlicensed() throws IOException, IllegalAccessException {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(false);
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);
        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("read").query(matchAllBytes).build() },
            null
        );

        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());

        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<>();
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            fileRolesStore,
            null,
            null,
            null,
            licenseState,
            null,
            null,
            null,
            effectiveRoleDescriptors::set
        );

        try (var mockLog = MockLog.capture(RoleDescriptorStore.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "disabled role warning",
                    RoleDescriptorStore.class.getName(),
                    Level.WARN,
                    "User roles [dls] are disabled because they require field or document level security. "
                        + "The current license is non-compliant for [field and document level security]."
                )
            );
            PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
            getRoleForRoleNames(compositeRolesStore, Collections.singleton("dls"), roleFuture);
            assertEquals(Role.EMPTY, roleFuture.actionGet());
            assertThat(effectiveRoleDescriptors.get(), empty());
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testRolesWhenDlsFlsLicensed() throws IOException {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().grantedFields("*").deniedFields("foo").indices("*").privileges("read").build() },
            null
        );
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);
        RoleDescriptor dlsRole = new RoleDescriptor(
            "dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("read").query(matchAllBytes).build() },
            null
        );
        RoleDescriptor flsDlsRole = new RoleDescriptor(
            "fls_dls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("read")
                    .grantedFields("*")
                    .deniedFields("foo")
                    .query(matchAllBytes)
                    .build() },
            null
        );
        RoleDescriptor noFlsDlsRole = new RoleDescriptor(
            "no_fls_dls",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("*").privileges("read").build() },
            null
        );
        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            fileRolesStore,
            null,
            null,
            null,
            licenseState,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("fls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(flsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(dlsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(flsDlsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton("no_fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(noFlsDlsRole));
        effectiveRoleDescriptors.set(null);
    }

    public void testSuperuserIsEffectiveWhenOtherRolesUnavailable() {
        final boolean criticalFailure = randomBoolean();
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> {
            final RuntimeException exception = new RuntimeException("Test(" + getTestName() + ") - native roles unavailable");
            if (criticalFailure) {
                callback.onFailure(exception);
            } else {
                callback.onResponse(RoleRetrievalResult.failure(exception));
            }
        };
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            Collections.emptyList()
        );

        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);
        trySuccessfullyLoadSuperuserRole(compositeRolesStore);
        if (criticalFailure) {
            // A failure RoleRetrievalResult doesn't block role building, only a throw exception does
            tryFailOnNonSuperuserRole(compositeRolesStore, throwableWithMessage(containsString("native roles unavailable")));
        }
    }

    public void testSuperuserIsEffectiveWhenApplicationPrivilegesAreUnavailable() {
        final RoleDescriptor role = new RoleDescriptor(
            "_mock_role",
            new String[0],
            new IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(randomAlphaOfLengthBetween(5, 12))
                    .privileges("all")
                    .resources("*")
                    .build() },
            new ConfigurableClusterPrivilege[0],
            new String[0],
            Map.of(),
            Map.of()
        );
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> callback.onResponse(
            RoleRetrievalResult.success(Set.of(role))
        );
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onFailure(
            new RuntimeException("No privileges for you!")
        );

        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);
        trySuccessfullyLoadSuperuserRole(compositeRolesStore);
        tryFailOnNonSuperuserRole(compositeRolesStore, throwableWithMessage(containsString("No privileges for you!")));
    }

    public void testErrorForInvalidIndexNameRegex() {
        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            "_mock_role",
            null,
            new IndicesPrivileges[] {
                // invalid regex missing closing bracket
                IndicesPrivileges.builder().indices("/~(([.]|ilm-history-).*/").privileges("read").build() },
            null
        );

        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> callback.onResponse(
            RoleRetrievalResult.success(Set.of(roleDescriptor))
        );
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            List.of()
        );
        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Set.of("_mock_role"), future);

        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("The set of patterns [/~(([.]|ilm-history-).*/] is invalid"));
        assertThat(e.getCause().getClass(), is(IllegalArgumentException.class));
    }

    private CompositeRolesStore setupRolesStore(
        Consumer<ActionListener<RoleRetrievalResult>> rolesHandler,
        Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler
    ) {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());

        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            rolesHandler.accept(callback);
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        final NativePrivilegeStore nativePrivilegeStore = mock(NativePrivilegeStore.class);
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            privilegesHandler.accept(callback);
            return null;
        }).when(nativePrivilegeStore).getPrivileges(anySet(), anySet(), eq(false), anyActionListener());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivilegeStore,
            null,
            null,
            null,
            null,
            null
        );
        return compositeRolesStore;
    }

    private void trySuccessfullyLoadSuperuserRole(CompositeRolesStore compositeRolesStore) {
        final Set<String> roles = Set.of(randomAlphaOfLengthBetween(1, 6), "superuser", randomAlphaOfLengthBetween(7, 12));
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roles, future);

        final Role role = future.actionGet();
        assertThat(role.names(), arrayContaining("superuser"));
        assertThat(role.application().getApplicationNames(), containsInAnyOrder("*"));
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        randomAlphaOfLengthBetween(2, 10),
                        randomAlphaOfLengthBetween(2, 10),
                        randomAlphaOfLengthBetween(2, 10)
                    ),
                    "*"
                ),
            is(true)
        );

        assertThat(role.cluster().privileges(), containsInAnyOrder(ClusterPrivilegeResolver.ALL));
        assertThat(role.indices().check(TransportSearchAction.TYPE.name()), Matchers.is(true));
        assertThat(role.indices().check(TransportIndexAction.NAME), Matchers.is(true));

        final Predicate<String> indexActionPredicate = Automatons.predicate(
            role.indices().allowedActionsMatcher("index-" + randomAlphaOfLengthBetween(1, 12))
        );
        assertThat(indexActionPredicate.test(TransportSearchAction.TYPE.name()), is(true));
        assertThat(indexActionPredicate.test(TransportIndexAction.NAME), is(true));

        final Predicate<String> securityActionPredicate = Automatons.predicate(role.indices().allowedActionsMatcher(".security"));
        assertThat(securityActionPredicate.test(TransportSearchAction.TYPE.name()), is(true));
        assertThat(securityActionPredicate.test(TransportIndexAction.NAME), is(false));
    }

    private void tryFailOnNonSuperuserRole(CompositeRolesStore compositeRolesStore, Matcher<? super Exception> exceptionMatcher) {
        final Set<String> roles = Set.of(randomAlphaOfLengthBetween(1, 6), randomAlphaOfLengthBetween(7, 12));
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roles, future);
        final Exception exception = expectThrows(Exception.class, future::actionGet);
        assertThat(exception, exceptionMatcher);
    }

    public void testNegativeLookupsAreCached() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());

        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        final NativePrivilegeStore nativePrivilegeStore = mock(NativePrivilegeStore.class);
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            callback.onResponse(Collections.emptyList());
            return null;
        }).when(nativePrivilegeStore).getPrivileges(anySet(), anySet(), eq(false), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivilegeStore,
            null,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(eq(Set.of(roleName)), anyActionListener());
        verify(fileRolesStore).accept(eq(Set.of(roleName)), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Set.of(roleName)));
        verify(nativeRolesStore).accept(eq(Set.of(roleName)), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(eq(Set.of(roleName)), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final boolean getSuperuserRole = randomBoolean()
            && roleName.equals(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()) == false;
        final Set<String> names = getSuperuserRole
            ? Sets.newHashSet(roleName, ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())
            : Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            getRoleForRoleNames(compositeRolesStore, names, future);
            final Role role1 = future.actionGet();
            if (getSuperuserRole) {
                assertThat(role1.names(), arrayContaining("superuser"));
                final Collection<RoleDescriptor> descriptors = effectiveRoleDescriptors.get();
                assertThat(descriptors, hasSize(1));
                assertThat(descriptors.iterator().next().getName(), is("superuser"));
            } else {
                assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
            }
        }
        if (numberOfTimesToCall > 0 && getSuperuserRole) {
            verify(nativePrivilegeStore).getPrivileges(eq(Set.of("*")), eq(Set.of("*")), eq(false), anyActionListener());
            // We can't verify the contents of the Set here because the set is mutated inside the method
            verify(reservedRolesStore, times(2)).accept(anySet(), anyActionListener());
        }
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore, nativePrivilegeStore);
    }

    public void testNegativeLookupsCacheDisabled() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final Settings settings = Settings.builder()
            .put(SECURITY_ENABLED_SETTINGS)
            .put("xpack.security.authz.store.roles.negative_lookup_cache.max_size", 0)
            .build();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            settings,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(anySet(), anyActionListener());
        verify(fileRolesStore).accept(anySet(), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).accept(anySet(), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        assertFalse(compositeRolesStore.isValueInNegativeLookupCache(roleName, Metadata.DEFAULT_PROJECT_ID));
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);
    }

    public void testShouldForkRoleBuilding() {
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            mock(RoleProviders.class),
            mock(NativePrivilegeStore.class),
            new ThreadContext(SECURITY_ENABLED_SETTINGS),
            mock(),
            cache,
            mock(ApiKeyService.class),
            mock(ServiceAccountService.class),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            buildBitsetCache(),
            TestRestrictedIndices.RESTRICTED_INDICES,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            mock()
        );

        assertFalse(compositeRolesStore.shouldForkRoleBuilding(Set.of()));
        assertFalse(
            compositeRolesStore.shouldForkRoleBuilding(
                Set.of(
                    randomValueOtherThanMany(
                        rd -> rd.isUsingDocumentOrFieldLevelSecurity() || rd.hasApplicationPrivileges(),
                        RoleDescriptorTestHelper::randomRoleDescriptor
                    )
                )
            )
        );

        assertTrue(compositeRolesStore.shouldForkRoleBuilding(generateRoleDescriptors(101))); // RD count above threshold
        assertTrue(
            compositeRolesStore.shouldForkRoleBuilding(
                Set.of(
                    randomValueOtherThanMany(
                        rd -> false == rd.isUsingDocumentOrFieldLevelSecurity(),
                        RoleDescriptorTestHelper::randomRoleDescriptor
                    )
                )
            )
        );
        assertTrue(
            compositeRolesStore.shouldForkRoleBuilding(
                Set.of(
                    randomValueOtherThanMany(rd -> false == rd.hasApplicationPrivileges(), RoleDescriptorTestHelper::randomRoleDescriptor)
                )
            )
        );
    }

    private static Set<RoleDescriptor> generateRoleDescriptors(int numRoleDescriptors) {
        Set<RoleDescriptor> roleDescriptors = new HashSet<>();
        for (int i = 0; i < numRoleDescriptors; i++) {
            roleDescriptors.add(RoleDescriptorTestHelper.randomRoleDescriptor());
        }
        return roleDescriptors;
    }

    public void testNegativeLookupsAreNotCachedWithFailures() {
        final var projectId = randomProjectIdOrDefault();
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final RoleProviders roleProviders = buildRolesProvider(fileRolesStore, nativeRolesStore, reservedRolesStore, null, licenseState);
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            roleProviders,
            mock(NativePrivilegeStore.class),
            new ThreadContext(SECURITY_ENABLED_SETTINGS),
            licenseState,
            cache,
            mock(ApiKeyService.class),
            mock(ServiceAccountService.class),
            TestProjectResolvers.singleProject(projectId),
            documentSubsetBitsetCache,
            TestRestrictedIndices.RESTRICTED_INDICES,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            effectiveRoleDescriptors::set
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), empty());
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(anySet(), anyActionListener());
        verify(fileRolesStore).accept(anySet(), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).accept(anySet(), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final Set<String> names = Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            getRoleForRoleNames(compositeRolesStore, names, future);
            future.actionGet();
            assertThat(effectiveRoleDescriptors.get(), empty());
            effectiveRoleDescriptors.set(null);
        }

        assertFalse(compositeRolesStore.isValueInNegativeLookupCache(roleName, projectId));
        assertFalse(compositeRolesStore.isValueInNegativeLookupCache(roleName, Metadata.DEFAULT_PROJECT_ID));
        verify(reservedRolesStore, times(numberOfTimesToCall + 1)).accept(anySet(), anyActionListener());
        verify(fileRolesStore, times(numberOfTimesToCall + 1)).accept(anySet(), anyActionListener());
        verify(fileRolesStore, times(numberOfTimesToCall + 1)).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore, times(numberOfTimesToCall + 1)).accept(anySet(), anyActionListener());
        verify(nativeRolesStore, times(numberOfTimesToCall + 1)).getRoleDescriptors(isASet(), anyActionListener());
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);
    }

    public void testCustomRolesProviders() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final RoleDescriptor roleAProvider1 = new RoleDescriptor(
            "roleA",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build() },
            null
        );
        final InMemoryRolesProvider inMemoryProvider1 = spy(new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(roleAProvider1);
            }
            return RoleRetrievalResult.success(descriptors);
        }));

        final RoleDescriptor roleBProvider2 = new RoleDescriptor(
            "roleB",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("READ").indices("bar").grantedFields("*").build() },
            null
        );
        final InMemoryRolesProvider inMemoryProvider2 = spy(new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                // both role providers can resolve role A, this makes sure that if the first
                // role provider in order resolves a role, the second provider does not override it
                descriptors.add(
                    new RoleDescriptor(
                        "roleA",
                        null,
                        new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("WRITE").indices("*").grantedFields("*").build() },
                        null
                    )
                );
            }
            if (roles.contains("roleB")) {
                descriptors.add(roleBProvider2);
            }
            return RoleRetrievalResult.success(descriptors);
        }));

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders = Map.of(
            "custom",
            List.of(inMemoryProvider1, inMemoryProvider2)
        );
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            customRoleProviders,
            null,
            null,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds),
            null,
            null,
            null
        );

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roleNames, future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(roleAProvider1, roleBProvider2));
        effectiveRoleDescriptors.set(null);

        // make sure custom roles providers populate roles correctly
        assertEquals(2, role.indices().groups().length);
        assertEquals(IndexPrivilege.READ, role.indices().groups()[0].privilege());
        assertThat(role.indices().groups()[0].indices()[0], anyOf(equalTo("foo"), equalTo("bar")));
        assertEquals(IndexPrivilege.READ, role.indices().groups()[1].privilege());
        assertThat(role.indices().groups()[1].indices()[0], anyOf(equalTo("foo"), equalTo("bar")));

        // make sure negative lookups are cached
        verify(inMemoryProvider1).accept(anySet(), anyActionListener());
        verify(inMemoryProvider2).accept(anySet(), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(1, 8);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            getRoleForRoleNames(compositeRolesStore, Collections.singleton("unknown"), future);
            future.actionGet();
            if (i == 0) {
                assertThat(effectiveRoleDescriptors.get(), empty());
            } else {
                assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
            }
            effectiveRoleDescriptors.set(null);
        }

        verifyNoMoreInteractions(inMemoryProvider1, inMemoryProvider2);
    }

    /**
     * This test is a direct result of a issue where field level security permissions were not
     * being merged correctly. The improper merging resulted in an allow all result when merging
     * permissions from different roles instead of properly creating a union of their languages
     */
    public void testMergingRolesWithFls() {
        RoleDescriptor flsRole = new RoleDescriptor(
            "fls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .grantedFields("*")
                    .deniedFields("L1.*", "L2.*")
                    .indices("*")
                    .privileges("read")
                    .query("{ \"match\": {\"eventType.typeCode\": \"foo\"} }")
                    .build() },
            null
        );
        RoleDescriptor addsL1Fields = new RoleDescriptor(
            "dls",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .indices("*")
                    .grantedFields("L1.*")
                    .privileges("read")
                    .query("{ \"match\": {\"eventType.typeCode\": \"foo\"} }")
                    .build() },
            null
        );
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        CompositeRolesStore.buildRoleFromDescriptors(
            Sets.newHashSet(flsRole, addsL1Fields),
            cache,
            null,
            TestRestrictedIndices.RESTRICTED_INDICES,
            future
        );
        Role role = future.actionGet();

        Metadata metadata = Metadata.builder()
            .put(
                new IndexMetadata.Builder("test").settings(
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
                ).numberOfShards(1).numberOfReplicas(0).build(),
                true
            )
            .build();
        IndicesAccessControl iac = role.indices()
            .authorize("indices:data/read/search", Collections.singleton("test"), metadata.getProject(), cache);
        assertTrue(iac.getIndexPermissions("test").getFieldPermissions().grantsAccessTo("L1.foo"));
        assertFalse(iac.getIndexPermissions("test").getFieldPermissions().grantsAccessTo("L2.foo"));
        assertTrue(iac.getIndexPermissions("test").getFieldPermissions().grantsAccessTo("L3.foo"));
    }

    public void testMergingBasicRoles() {
        final TransportRequest request1 = mock(TransportRequest.class);
        final TransportRequest request2 = mock(TransportRequest.class);
        final TransportRequest request3 = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        ConfigurableClusterPrivilege ccp1 = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                builder.add(
                    this,
                    ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    req -> req == request1
                );
                return builder;
            }
        };
        RoleDescriptor role1 = new RoleDescriptor(
            "r1",
            new String[] { "monitor" },
            new IndicesPrivileges[] {
                IndicesPrivileges.builder().indices("abc-*", "xyz-*").privileges("read").build(),
                IndicesPrivileges.builder().indices("ind-1-*").privileges("all").build(), },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("app1")
                    .resources("user/*")
                    .privileges("read", "write")
                    .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("app1")
                    .resources("settings/*")
                    .privileges("read")
                    .build() },
            new ConfigurableClusterPrivilege[] { ccp1 },
            new String[] { "app-user-1" },
            null,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                RoleDescriptor.RemoteIndicesPrivileges.builder("remote-*", "remote").indices("abc-*", "xyz-*").privileges("read").build(),
                RoleDescriptor.RemoteIndicesPrivileges.builder("remote-*").indices("remote-idx-1-*").privileges("read").build(), },
            getValidRemoteClusterPermissions(new String[] { "remote-*" }),
            null,
            randomAlphaOfLengthBetween(0, 20)
        );

        ConfigurableClusterPrivilege ccp2 = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                builder.add(
                    this,
                    ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    req -> req == request2
                );
                return builder;
            }
        };
        RoleDescriptor role2 = new RoleDescriptor(
            "r2",
            new String[] { "manage_saml" },
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("abc-*", "ind-2-*").privileges("all").build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder().application("app2a").resources("*").privileges("all").build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder().application("app2b").resources("*").privileges("read").build() },
            new ConfigurableClusterPrivilege[] { ccp2 },
            new String[] { "app-user-2" },
            null,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("remote-idx-2-*").privileges("read").build(),
                RoleDescriptor.RemoteIndicesPrivileges.builder("remote-*").indices("remote-idx-3-*").privileges("read").build() },
            null,
            null,
            randomAlphaOfLengthBetween(0, 20)
        );

        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer(inv -> {
            assertEquals(4, inv.getArguments().length);
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) inv.getArguments()[3];
            Set<ApplicationPrivilegeDescriptor> set = new HashSet<>();
            Arrays.asList("app1", "app2a", "app2b")
                .forEach(
                    app -> Arrays.asList("read", "write", "all")
                        .forEach(
                            perm -> set.add(new ApplicationPrivilegeDescriptor(app, perm, Collections.emptySet(), Collections.emptyMap()))
                        )
                );
            listener.onResponse(set);
            return null;
        }).when(privilegeStore).getPrivileges(anyCollection(), anyCollection(), eq(false), anyActionListener());
        CompositeRolesStore.buildRoleFromDescriptors(
            Sets.newHashSet(role1, role2),
            cache,
            privilegeStore,
            TestRestrictedIndices.RESTRICTED_INDICES,
            future
        );
        Role role = future.actionGet();

        assertThat(role.cluster().check(ClusterStateAction.NAME, randomFrom(request1, request2, request3), authentication), equalTo(true));
        assertThat(
            role.cluster().check(SamlAuthenticateAction.NAME, randomFrom(request1, request2, request3), authentication),
            equalTo(true)
        );
        assertThat(
            role.cluster().check(ClusterUpdateSettingsAction.NAME, randomFrom(request1, request2, request3), authentication),
            equalTo(false)
        );

        assertThat(role.cluster().check(PutUserAction.NAME, randomFrom(request1, request2), authentication), equalTo(true));
        assertThat(role.cluster().check(PutUserAction.NAME, request3, authentication), equalTo(false));

        final IsResourceAuthorizedPredicate allowedRead = role.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name());
        assertThat(allowedRead.test(mockIndexAbstraction("abc-123")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("xyz-000")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-1-a")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-2-a")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("foo")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("abc")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("xyz")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-3-a")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("remote-idx-1-1")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("remote-idx-2-1")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("remote-idx-3-1")), equalTo(false));

        final IsResourceAuthorizedPredicate allowedWrite = role.indices().allowedIndicesMatcher(TransportIndexAction.NAME);
        assertThat(allowedWrite.test(mockIndexAbstraction("abc-123")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("xyz-000")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-1-a")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-2-a")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("foo")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("abc")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("xyz")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-3-a")), equalTo(false));

        role.application().grants(ApplicationPrivilegeTests.createPrivilege("app1", "app1-read", "write"), "user/joe");
        role.application().grants(ApplicationPrivilegeTests.createPrivilege("app1", "app1-read", "read"), "settings/hostname");
        role.application().grants(ApplicationPrivilegeTests.createPrivilege("app2a", "app2a-all", "all"), "user/joe");
        role.application().grants(ApplicationPrivilegeTests.createPrivilege("app2b", "app2b-read", "read"), "settings/hostname");

        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-*", "remote"), Set.of("*"), Set.of("remote-*"));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of("remote-*"),
            indexGroup("remote-idx-1-*"),
            indexGroup("remote-idx-3-*")
        );
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("remote-*", "remote"), indexGroup("xyz-*", "abc-*"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("*"), indexGroup("remote-idx-2-*"));

        final RemoteIndicesPermission forRemote = role.remoteIndices().forCluster("remote");
        assertHasRemoteIndexGroupsForClusters(forRemote, Set.of("remote-*", "remote"), indexGroup("xyz-*", "abc-*"));
        assertHasRemoteIndexGroupsForClusters(forRemote, Set.of("*"), indexGroup("remote-idx-2-*"));
        assertValidRemoteClusterPermissions(role.remoteCluster(), new String[] { "remote-*" });
        assertThat(
            role.remoteCluster().collapseAndRemoveUnsupportedPrivileges("remote-foobar", TransportVersion.current()),
            equalTo(RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]))
        );
    }

    public void testBuildRoleWithSingleRemoteIndicesDefinition() {
        final String clusterAlias = randomFrom("remote-1", "*");
        final Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder(clusterAlias).indices("index-1").privileges("read").build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias), indexGroup("index-1"));
    }

    public void testBuildRoleWithSingleRemoteClusterDefinition() {
        final String[] clusterAliases = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final Role role = buildRole(roleDescriptorWithRemoteClusterPrivileges("r1", getValidRemoteClusterPermissions(clusterAliases)));
        assertValidRemoteClusterPermissions(role.remoteCluster(), clusterAliases);
    }

    public void testBuildRoleFromDescriptorsWithSingleRestriction() {
        Role role = buildRole(
            RoleDescriptorTestHelper.builder()
                .allowReservedMetadata(randomBoolean())
                .allowRemoteIndices(randomBoolean())
                .allowRestriction(true)
                .allowDescription(randomBoolean())
                .allowRemoteClusters(randomBoolean())
                .build()
        );
        assertThat(role.hasWorkflowsRestriction(), equalTo(true));
    }

    public void testBuildRoleFromDescriptorsWithViolationOfRestrictionValidation() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> buildRole(
                RoleDescriptorTestHelper.builder()
                    .allowReservedMetadata(randomBoolean())
                    .allowRemoteIndices(randomBoolean())
                    .allowRestriction(true)
                    .allowDescription(randomBoolean())
                    .allowRemoteClusters(randomBoolean())
                    .build(),
                RoleDescriptorTestHelper.builder()
                    .allowReservedMetadata(randomBoolean())
                    .allowRemoteIndices(randomBoolean())
                    .allowRestriction(true)
                    .allowDescription(randomBoolean())
                    .allowRemoteClusters(randomBoolean())
                    .build()
            )
        );
        assertThat(e.getMessage(), containsString("more than one role descriptor with restriction is not allowed"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> buildRole(
                RoleDescriptorTestHelper.builder()
                    .allowReservedMetadata(randomBoolean())
                    .allowRemoteIndices(randomBoolean())
                    .allowRestriction(true)
                    .allowDescription(randomBoolean())
                    .allowRemoteClusters(randomBoolean())
                    .build(),
                RoleDescriptorTestHelper.builder()
                    .allowReservedMetadata(randomBoolean())
                    .allowRemoteIndices(randomBoolean())
                    .allowRestriction(false)
                    .allowDescription(randomBoolean())
                    .allowRemoteClusters(randomBoolean())
                    .build(),
                RoleDescriptorTestHelper.builder()
                    .allowReservedMetadata(randomBoolean())
                    .allowRemoteIndices(randomBoolean())
                    .allowRestriction(false)
                    .allowDescription(randomBoolean())
                    .allowRemoteClusters(randomBoolean())
                    .build()
            )
        );
        assertThat(e.getMessage(), containsString("combining role descriptors with and without restriction is not allowed"));
    }

    public void testBuildRoleWithFlsAndDlsInRemoteIndicesDefinition() {
        String clusterAlias = randomFrom("remote-1", "*");
        Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder(clusterAlias)
                        .indices("index-1")
                        .privileges("read")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .grantedFields("field")
                        .build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of(clusterAlias),
            indexGroup(
                IndexPrivilege.READ,
                false,
                "{\"match\":{\"field\":\"a\"}}",
                new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null),
                "index-1"
            )
        );

        role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder(clusterAlias)
                        .indices("index-1")
                        .privileges("read")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .grantedFields("field")
                        .build() }
            ),
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder(clusterAlias)
                        .indices("index-1")
                        .privileges("read")
                        .query("{\"match\":{\"field\":\"b\"}}")
                        .grantedFields("other")
                        .build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of(clusterAlias),
            indexGroup(
                IndexPrivilege.READ,
                false,
                "{\"match\":{\"field\":\"a\"}}",
                new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null),
                "index-1"
            ),
            indexGroup(
                IndexPrivilege.READ,
                false,
                "{\"match\":{\"field\":\"b\"}}",
                new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "other" }, null),
                "index-1"
            )
        );
    }

    public void testBuildRoleWithEmptyOrNoneRemoteIndices() {
        Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("none").build() }
            )
        );
        assertThat(role.remoteIndices().remoteIndicesGroups(), empty());

        role = buildRole(roleDescriptorWithRemoteIndicesPrivileges("r1", new RoleDescriptor.RemoteIndicesPrivileges[] {}));
        assertThat(role.remoteIndices().remoteIndicesGroups(), empty());
    }

    public void testBuildRoleWithoutRemoteCluster() {
        final Role role = buildRole(roleDescriptorWithRemoteClusterPrivileges("r1", null));
        assertThat(role.remoteCluster(), equalTo(RemoteClusterPermissions.NONE));
    }

    public void testBuildRoleWithSingleRemoteIndicesDefinitionWithAllowRestricted() {
        final String clusterAlias = randomFrom("remote-1", "*");
        final Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder(clusterAlias)
                        .indices("index-1")
                        .allowRestrictedIndices(true)
                        .privileges("read")
                        .build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of(clusterAlias), indexGroup(IndexPrivilege.READ, true, "index-1"));
    }

    public void testBuildRoleWithRemoteIndicesDoesNotMergeWhenNothingToMerge() {
        Role role = buildRole(
            roleDescriptorWithIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("index-1").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(), },
                new IndicesPrivileges[] { RoleDescriptor.IndicesPrivileges.builder().indices("index-1").privileges("all").build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), Set.of("*"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("*"), indexGroup("index-1"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), indexGroup("index-1"));
        final IsResourceAuthorizedPredicate allowedRead = role.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name());
        assertThat(allowedRead.test(mockIndexAbstraction("index-1")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("foo")), equalTo(false));
    }

    public void testBuildRoleWithRemoteIndicesDoesNotCombineRemotesAndLocals() {
        Role role = buildRole(
            roleDescriptorWithIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("index-1").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("index-1").privileges("read").build(), },
                new IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("index-1").privileges("write").build(),
                    RoleDescriptor.IndicesPrivileges.builder().indices("index-1").privileges("read").build(), }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("*"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("*"), indexGroup("index-1"));
        final IsResourceAuthorizedPredicate allowedRead = role.indices().allowedIndicesMatcher(TransportGetAction.TYPE.name());
        assertThat(allowedRead.test(mockIndexAbstraction("index-1")), equalTo(true));
        final IsResourceAuthorizedPredicate allowedWrite = role.indices().allowedIndicesMatcher(TransportIndexAction.NAME);
        assertThat(allowedWrite.test(mockIndexAbstraction("index-1")), equalTo(true));
    }

    public void testBuildRoleWithRemoteIndicesDoesNotMergeRestrictedAndNonRestricted() {
        final Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1")
                        .indices("index-1")
                        .privileges("read")
                        .allowRestrictedIndices(false)
                        .build() }
            ),
            roleDescriptorWithRemoteIndicesPrivileges(
                "r2",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1")
                        .indices("index-1")
                        .privileges("read")
                        .allowRestrictedIndices(true)
                        .build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of("remote-1"),
            indexGroup(IndexPrivilege.READ, true, "index-1"),
            indexGroup("index-1")
        );
    }

    public void testBuildRoleWithMultipleRemoteMergedAcrossPrivilegesAndDescriptors() {
        Role role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1", "index-2").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1", "remote-2")
                        .indices("index-1", "index-2")
                        .privileges("read")
                        .build(), }
            ),
            roleDescriptorWithRemoteIndicesPrivileges(
                "r2",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build() }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), Set.of("remote-1", "remote-2"));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of("remote-1"),
            indexGroup("index-1", "index-2"),
            indexGroup("index-1")
        );
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("remote-1", "remote-2"), indexGroup("index-1", "index-2"));

        role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("*").privileges("read").build(), }
            ),
            roleDescriptorWithRemoteIndicesPrivileges(
                "r2",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("*").indices("*").privileges("read").build(), }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), Set.of("*"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), indexGroup("index-1"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("*"), indexGroup("*"));

        role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(), }
            ),
            roleDescriptorWithRemoteIndicesPrivileges(
                "r2",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("none").build(), }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"));
        assertHasRemoteIndexGroupsForClusters(role.remoteIndices(), Set.of("remote-1"), indexGroup("index-1"));

        role = buildRole(
            roleDescriptorWithRemoteIndicesPrivileges(
                "r1",
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("none").build(),
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-1").indices("index-1").privileges("read").build(), }
            )
        );
        assertHasRemoteIndicesGroupsForClusters(role.remoteIndices(), Set.of("remote-1"));
        assertHasRemoteIndexGroupsForClusters(
            role.remoteIndices(),
            Set.of("remote-1"),
            indexGroup(IndexPrivilege.get("read"), false, "index-1"),
            indexGroup(IndexPrivilege.get("none"), false, "index-1")
        );
    }

    public void testBuildRoleWithMultipleRemoteClusterMerged() {
        final String[] clusterAliases1 = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] clusterAliases2 = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] clusterAliases3 = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final Role role = buildRole(
            roleDescriptorWithRemoteClusterPrivileges("r1", getValidRemoteClusterPermissions(clusterAliases1)),
            roleDescriptorWithRemoteClusterPrivileges("r2", getValidRemoteClusterPermissions(clusterAliases2)),
            roleDescriptorWithRemoteClusterPrivileges("r3", getValidRemoteClusterPermissions(clusterAliases3))
        );
        assertValidRemoteClusterPermissionsParent(role.remoteCluster(), clusterAliases1);
        assertValidRemoteClusterPermissionsParent(role.remoteCluster(), clusterAliases2);
        assertValidRemoteClusterPermissionsParent(role.remoteCluster(), clusterAliases3);
        assertValidRemoteClusterPermissionsParent(role.remoteCluster(), clusterAliases3);
        assertValidRemoteClusterPermissionsParent(
            role.remoteCluster(),
            Stream.of(clusterAliases1, clusterAliases2, clusterAliases3).flatMap(Arrays::stream).toArray(String[]::new)
        );

        assertThat(role.remoteCluster().groups().size(), equalTo(3));
        for (RemoteClusterPermissionGroup group : role.remoteCluster().groups()) {
            // order here is not guaranteed, so try them all
            if (Arrays.equals(group.remoteClusterAliases(), clusterAliases1)) {
                assertValidRemoteClusterPermissionsGroups(List.of(group), clusterAliases1);
            } else if (Arrays.equals(group.remoteClusterAliases(), clusterAliases2)) {
                assertValidRemoteClusterPermissionsGroups(List.of(group), clusterAliases2);
            } else if (Arrays.equals(group.remoteClusterAliases(), clusterAliases3)) {
                assertValidRemoteClusterPermissionsGroups(List.of(group), clusterAliases3);
            } else {
                fail("unexpected remote cluster group: " + Arrays.toString(group.remoteClusterAliases()));
            }
        }
    }

    public void testBuildRoleWithReadFailureStorePrivilegeOnly() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final Role role = buildRole(
            roleDescriptorWithIndicesPrivileges(
                "r1",
                new IndicesPrivileges[] {
                    IndicesPrivileges.builder()
                        .indices(indexPattern)
                        .privileges("read_failure_store")
                        .allowRestrictedIndices(allowRestrictedIndices)
                        .build() }
            )
        );
        assertHasIndexGroups(role.indices(), indexGroup(IndexPrivilege.READ_FAILURE_STORE, allowRestrictedIndices, indexPattern));
    }

    public void testBuildRoleWithReadFailureStorePrivilegeDuplicatesMerged() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            assertHasIndexGroups(role.indices(), indexGroup(IndexPrivilege.READ_FAILURE_STORE, allowRestrictedIndices, indexPattern));
        }
    }

    public void testBuildRoleWithReadFailureStoreAndReadPrivilegeSplit() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final Role role = buildRole(
            roleDescriptorWithIndicesPrivileges(
                "r1",
                new IndicesPrivileges[] {
                    IndicesPrivileges.builder()
                        .indices(indexPattern)
                        .privileges("read", "read_failure_store")
                        .allowRestrictedIndices(allowRestrictedIndices)
                        .build() }
            )
        );
        assertHasIndexGroups(
            role.indices(),
            indexGroup(IndexPrivilege.READ_FAILURE_STORE, allowRestrictedIndices, indexPattern),
            indexGroup(IndexPrivilege.READ, allowRestrictedIndices, indexPattern)
        );
    }

    public void testBuildRoleWithReadFailureStoreAndReadPrivilegeAndMultipleIndexPatternsSplit() {
        String indexPattern = randomAlphanumericOfLength(10);
        String otherIndexPattern = randomValueOtherThan(indexPattern, () -> randomAlphanumericOfLength(10));
        boolean allowRestrictedIndices = randomBoolean();
        List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "write")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(otherIndexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "write")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(otherIndexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            assertHasIndexGroups(
                role.indices(),
                indexGroup(
                    IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(Set.of("read", "write")),
                    allowRestrictedIndices,
                    indexPattern
                ),
                indexGroup(IndexPrivilege.READ, allowRestrictedIndices, otherIndexPattern),
                indexGroup(IndexPrivilege.READ_FAILURE_STORE, allowRestrictedIndices, otherIndexPattern)
            );
        }
    }

    public void testBuildRoleWithReadOnRestrictedAndNonRestrictedIndices() {
        String indexPattern = randomAlphanumericOfLength(10);
        List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(true)
                            .build(),
                        IndicesPrivileges.builder().indices(indexPattern).privileges("read").allowRestrictedIndices(false).build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(true)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().indices(indexPattern).privileges("read").allowRestrictedIndices(false).build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            IndicesPermission indices = role.indices();
            assertHasIndexGroups(
                indices,
                indexGroup(IndexPrivilege.get("read"), false, indexPattern),
                indexGroup(IndexPrivilege.get("read"), true, indexPattern),
                indexGroup(IndexPrivilege.get("read_failure_store"), true, indexPattern)
            );
        }
    }

    public void testBuildRoleWithReadFailureStoreOnRestrictedAndNonRestrictedIndices() {
        String indexPattern = randomAlphanumericOfLength(10);
        List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(true)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(false)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store")
                            .allowRestrictedIndices(true)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(false)
                            .build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            IndicesPermission indices = role.indices();
            assertHasIndexGroups(
                indices,
                indexGroup(IndexPrivilege.get("read_failure_store"), false, indexPattern),
                indexGroup(IndexPrivilege.get("read"), true, indexPattern),
                indexGroup(IndexPrivilege.get("read_failure_store"), true, indexPattern)
            );
        }
    }

    public void testBuildRoleWithMultipleReadFailureStoreAndReadPrivilegeSplit() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            assertHasIndexGroups(
                role.indices(),
                indexGroup(IndexPrivilege.READ_FAILURE_STORE, allowRestrictedIndices, indexPattern),
                indexGroup(IndexPrivilege.READ, allowRestrictedIndices, indexPattern)
            );
        }
    }

    public void testBuildRoleWithAllPrivilegeIsNeverSplit() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store", "all")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "read_failure_store", "all")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            )
        );
        // the roles are different "format" but the same so should produce the same index groups
        for (var role : roles) {
            assertHasIndexGroups(
                role.indices(),
                indexGroup(
                    IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(Set.of("read", "read_failure_store", "all")),
                    allowRestrictedIndices,
                    indexPattern
                )
            );
        }
    }

    public void testBuildRoleWithFailureStorePrivilegeCollatesToRemoveDlsFlsFromAnotherGroup() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final List<Role> roles = List.of(
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build(),
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "view_index_metadata")
                            .query("{\"match\":{\"field\":\"a\"}}")
                            .grantedFields("field")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            ),
            buildRole(
                roleDescriptorWithIndicesPrivileges(
                    "r1",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read_failure_store")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                ),
                roleDescriptorWithIndicesPrivileges(
                    "r2",
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder()
                            .indices(indexPattern)
                            .privileges("read", "view_index_metadata")
                            .query("{\"match\":{\"field\":\"a\"}}")
                            .grantedFields("field")
                            .allowRestrictedIndices(allowRestrictedIndices)
                            .build() }
                )
            )
        );
        for (var role : roles) {
            assertHasIndexGroups(
                role.indices(),
                indexGroup(
                    IndexPrivilege.get("read_failure_store"),
                    allowRestrictedIndices,
                    null,
                    new FieldPermissionsDefinition(
                        Set.of(
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null),
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null)
                        )
                    ),
                    indexPattern
                ),
                indexGroup(
                    IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(Set.of("read", "view_index_metadata")),
                    allowRestrictedIndices,
                    null,
                    new FieldPermissionsDefinition(
                        Set.of(
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null),
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null)
                        )
                    ),
                    indexPattern
                )
            );

        }
    }

    public void testBuildRoleWithFailureStorePrivilegeCollatesToKeepDlsFlsFromAnotherGroup() {
        String indexPattern = randomAlphanumericOfLength(10);
        boolean allowRestrictedIndices = randomBoolean();
        final Role role = buildRole(
            roleDescriptorWithIndicesPrivileges(
                "r1",
                new IndicesPrivileges[] {
                    IndicesPrivileges.builder()
                        .indices(indexPattern)
                        .privileges("read_failure_store")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .grantedFields("field")
                        .allowRestrictedIndices(allowRestrictedIndices)
                        .build(),
                    IndicesPrivileges.builder()
                        .indices(indexPattern)
                        .privileges("read", "view_index_metadata")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .grantedFields("field")
                        .allowRestrictedIndices(allowRestrictedIndices)
                        .build() }
            )
        );
        assertHasIndexGroups(
            role.indices(),
            indexGroup(
                IndexPrivilege.get("read_failure_store"),
                allowRestrictedIndices,
                "{\"match\":{\"field\":\"a\"}}",
                new FieldPermissionsDefinition(
                    Set.of(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null))
                ),
                indexPattern
            ),
            indexGroup(
                IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(Set.of("read", "view_index_metadata")),
                allowRestrictedIndices,
                "{\"match\":{\"field\":\"a\"}}",
                new FieldPermissionsDefinition(
                    Set.of(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "field" }, null))
                ),
                indexPattern
            )
        );
    }

    public void testBuildRoleDoesNotSplitIfAllPrivilegesHaveTheSameSelector() {
        String indexPattern = randomAlphanumericOfLength(10);
        IndexComponentSelectorPredicate predicate = randomFrom(
            IndexComponentSelectorPredicate.ALL,
            IndexComponentSelectorPredicate.DATA,
            IndexComponentSelectorPredicate.FAILURES,
            IndexComponentSelectorPredicate.DATA_AND_FAILURES
        );

        List<String> privilegesWithSelector = IndexPrivilege.names()
            .stream()
            .filter(p -> IndexPrivilege.getNamedOrNull(p).getSelectorPredicate() == predicate)
            .toList();
        Set<String> usedPrivileges = new HashSet<>();

        int n = randomIntBetween(1, 5);
        IndicesPrivileges[] indicesPrivileges = new IndicesPrivileges[n];
        for (int i = 0; i < n; i++) {
            IndicesPrivileges.Builder builder = IndicesPrivileges.builder();
            // TODO this is due to an unrelated bug in index collation logic
            List<String> privileges = randomValueOtherThanMany(
                p -> p.get(0).equals("none"),
                () -> randomNonEmptySubsetOf(privilegesWithSelector)
            );
            usedPrivileges.addAll(privileges);
            indicesPrivileges[i] = builder.indices(indexPattern).privileges(privileges).build();
        }

        final Role role = buildRole(roleDescriptorWithIndicesPrivileges("r1", indicesPrivileges));
        final IndicesPermission actual = role.indices();
        assertHasIndexGroups(
            actual,
            indexGroup(IndexPrivilegeTests.resolvePrivilegeAndAssertSingleton(usedPrivileges), false, indexPattern)
        );
    }

    public void testCustomRolesProviderFailures() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();

        final InMemoryRolesProvider inMemoryProvider1 = new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(
                    new RoleDescriptor(
                        "roleA",
                        null,
                        new IndicesPrivileges[] {
                            IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build() },
                        null
                    )
                );
            }
            return RoleRetrievalResult.success(descriptors);
        });

        final BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> failingProvider = (roles, listener) -> listener.onFailure(
            new Exception("fake failure")
        );

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders = randomBoolean()
            ? Map.of("custom", List.of(inMemoryProvider1, failingProvider))
            : Map.of("custom", List.of(inMemoryProvider1), "failing", List.of(failingProvider));
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            customRoleProviders,
            null,
            null,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds),
            null,
            null,
            null
        );

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roleNames, future);
        try {
            future.get();
            fail("provider should have thrown a failure");
        } catch (ExecutionException e) {
            assertEquals("fake failure", e.getCause().getMessage());
            assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        }
    }

    public void testCustomRolesProvidersLicensing() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();

        final RoleDescriptor roleA = new RoleDescriptor(
            "roleA",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build() },
            null
        );
        final InMemoryRolesProvider inMemoryProvider = new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(roleA);
            }
            return RoleRetrievalResult.success(descriptors);
        });

        final MockLicenseState xPackLicenseState = MockLicenseState.createMock();
        when(xPackLicenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(false);
        final AtomicReference<LicenseStateListener> licenseListener = new AtomicReference<>(null);
        MockLicenseState.acceptListeners(xPackLicenseState, licenseListener::set);

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders = Map.of(
            "custom",
            List.of(inMemoryProvider)
        );
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            customRoleProviders,
            null,
            xPackLicenseState,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds),
            null,
            null,
            null
        );

        Set<String> roleNames = Sets.newHashSet("roleA");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roleNames, future);
        Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), hasSize(0));
        effectiveRoleDescriptors.set(null);
        verify(xPackLicenseState).disableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, "custom");

        // no roles should've been populated, as the license doesn't permit custom role providers
        assertEquals(0, role.indices().groups().length);

        when(xPackLicenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(true);
        licenseListener.get().licenseStateChanged();

        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roleNames, future);
        role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(roleA));
        effectiveRoleDescriptors.set(null);
        verify(xPackLicenseState).enableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, "custom");

        // roleA should've been populated by the custom role provider, because the license allows it
        assertEquals(1, role.indices().groups().length);

        when(xPackLicenseState.isAllowed(Security.CUSTOM_ROLE_PROVIDERS_FEATURE)).thenReturn(false);
        licenseListener.get().licenseStateChanged();

        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roleNames, future);
        role = future.actionGet();
        assertEquals(0, role.indices().groups().length);
        assertThat(effectiveRoleDescriptors.get(), hasSize(0));
        verify(xPackLicenseState, times(2)).disableUsageTracking(Security.CUSTOM_ROLE_PROVIDERS_FEATURE, "custom");
    }

    private SecurityIndexManager.IndexState dummyState(ClusterHealthStatus indexStatus) {
        return dummyIndexState(true, indexStatus);
    }

    public SecurityIndexManager.IndexState dummyIndexState(boolean isIndexUpToDate, ClusterHealthStatus healthStatus) {
        var mgr = mock(SecurityIndexManager.class);
        return mgr.new IndexState(
            Metadata.DEFAULT_PROJECT_ID, SecurityIndexManager.ProjectStatus.PROJECT_AVAILABLE, Instant.now(), isIndexUpToDate, true, true,
            true, true, null, null, null, null, concreteSecurityIndexName, healthStatus, IndexMetadata.State.OPEN, "my_uuid", Set.of()
        );
    }

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        ReservedRolesStore reservedRolesStore = mock(ReservedRolesStore.class);
        doCallRealMethod().when(reservedRolesStore).accept(anySet(), anyActionListener());
        NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());

        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            (store, project) -> numInvalidation.incrementAndGet(),
            null,
            null
        );

        int expectedInvalidation = 0;
        // existing to no longer present
        SecurityIndexManager.IndexState previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.IndexState currentState = dummyState(null);
        compositeRolesStore.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        compositeRolesStore.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        compositeRolesStore.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        compositeRolesStore.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(
            previousState.indexHealth == ClusterHealthStatus.GREEN ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN
        );
        compositeRolesStore.onSecurityIndexStateChange(Metadata.DEFAULT_PROJECT_ID, previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());
    }

    public void testCacheClearOnIndexOutOfDateChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        ReservedRolesStore reservedRolesStore = mock(ReservedRolesStore.class);
        doCallRealMethod().when(reservedRolesStore).accept(anySet(), anyActionListener());
        NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            (store, project) -> numInvalidation.incrementAndGet(),
            null,
            null
        );

        compositeRolesStore.onSecurityIndexStateChange(
            Metadata.DEFAULT_PROJECT_ID,
            dummyIndexState(false, null),
            dummyIndexState(true, null)
        );
        assertEquals(1, numInvalidation.get());

        compositeRolesStore.onSecurityIndexStateChange(
            Metadata.DEFAULT_PROJECT_ID,
            dummyIndexState(true, null),
            dummyIndexState(false, null)
        );
        assertEquals(2, numInvalidation.get());
    }

    public void testDefaultRoleUserWithoutRoles() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            mock(NativePrivilegeStore.class),
            null,
            mock(ApiKeyService.class),
            mock(ServiceAccountService.class),
            null,
            null
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        final User user = new User("no role user");
        compositeRolesStore.getRole(new Subject(user, new RealmRef("name", "type", "node")), rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertEquals(Role.EMPTY, roles);
    }

    public void testAnonymousUserEnabledRoleAdded() {
        Settings settings = Settings.builder()
            .put(SECURITY_ENABLED_SETTINGS)
            .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role")
            .build();
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Set<String> names = (Set<String>) invocationOnMock.getArguments()[0];
            if (names.size() == 1 && names.contains("anonymous_user_role")) {
                RoleDescriptor rd = new RoleDescriptor("anonymous_user_role", null, null, null);
                return Collections.singleton(rd);
            }
            return Collections.emptySet();
        }).when(fileRolesStore).roleDescriptors(anySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            settings,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            mock(NativePrivilegeStore.class),
            null,
            mock(ApiKeyService.class),
            mock(ServiceAccountService.class),
            null,
            null
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        final User user = new User("no role user");
        Subject subject = new Subject(user, new RealmRef("name", "type", "node"));
        compositeRolesStore.getRole(subject, rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(Arrays.asList(roles.names()), hasItem("anonymous_user_role"));
    }

    public void testDoesNotUseRolesStoreForInternalUsers() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            null,
            null,
            null,
            effectiveRoleDescriptors::set
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        for (var internalUser : AuthenticationTestHelper.internalUsersWithLocalRoleDescriptor()) {
            Role expectedRole = compositeRolesStore.getInternalUserRole(internalUser);
            Subject subject = new Subject(internalUser, new RealmRef("name", "type", "node"));
            PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
            compositeRolesStore.getRole(subject, rolesFuture);
            Role roles = rolesFuture.actionGet();

            assertThat(roles, equalTo(expectedRole));
            assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
            verifyNoMoreInteractions(fileRolesStore, nativeRolesStore, reservedRolesStore);
        }
    }

    public void testRoleWithInternalRoleNameResolvesToRoleDefinedInRoleStore() {
        String roleName = AuthenticationTestHelper.randomInternalRoleName();
        RoleDescriptor expected = new RoleDescriptor(roleName, null, null, null);
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> {
            callback.onResponse(RoleRetrievalResult.success(Set.of(expected)));
        };
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            Collections.emptyList()
        );

        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);
        final Set<String> roles = Set.of(roleName);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, roles, future);

        final Role role = future.actionGet();
        assertThat(role.names(), arrayContaining(roleName));
        assertThat(role.application().getApplicationNames(), empty());
        assertThat(role.cluster().privileges(), empty());
        assertThat(role.indices(), is(IndicesPermission.NONE));

        final InternalUser internalUser = InternalUsers.getUser(roleName);
        assertThat(internalUser, notNullValue());
        if (internalUser.getLocalClusterRoleDescriptor().isPresent()) {
            Role internalRole = compositeRolesStore.getInternalUserRole(internalUser);
            assertThat(internalRole, notNullValue());
            assertThat(role, not(internalRole));
        }

        final Role[] internalRoles = InternalUsers.get()
            .stream()
            .filter(u -> u.getLocalClusterRoleDescriptor().isPresent())
            .map(compositeRolesStore::getInternalUserRole)
            .toArray(Role[]::new);
        // Check that we're actually testing something here...
        assertThat(internalRoles, arrayWithSize(greaterThan(1)));
        assertThat(role, not(is(oneOf(internalRoles))));
    }

    public void testGetRolesForSystemUserThrowsException() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            null,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> compositeRolesStore.getRole(
                new Subject(InternalUsers.SYSTEM_USER, new RealmRef("__attach", "__attach", randomAlphaOfLengthBetween(3, 8))),
                null
            )
        );
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        assertEquals("the internal user [_system] should never have its roles resolved", iae.getMessage());
    }

    public void testApiKeyAuthUsesApiKeyService() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(SECURITY_ENABLED_SETTINGS, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL))
        );
        ApiKeyService apiKeyService = spy(
            new ApiKeyService(
                SECURITY_ENABLED_SETTINGS,
                Clock.systemUTC(),
                mock(Client.class),
                mock(SecurityIndexManager.class),
                clusterService,
                mock(CacheInvalidatorRegistry.class),
                mock(ThreadPool.class),
                MeterRegistry.NOOP
            )
        );
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollection(), anyCollection(), eq(false), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivStore,
            null,
            apiKeyService,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        AuditUtil.getOrGenerateRequestId(threadContext);
        final TransportVersion version = randomFrom(
            TransportVersion.current(),
            TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_7_0_0, TransportVersions.V_7_8_1)
        );
        final Authentication authentication = createApiKeyAuthentication(
            apiKeyService,
            randomValueOtherThanMany(
                authc -> authc.getAuthenticationType() == AuthenticationType.API_KEY,
                () -> AuthenticationTestHelper.builder().build()
            ),
            Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[] { "manage" }, null, null)),
            null,
            version
        );

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        Role role = roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));

        if (version == TransportVersion.current()) {
            verify(apiKeyService, times(1)).parseRoleDescriptorsBytes(anyString(), any(BytesReference.class), any());
        } else {
            verify(apiKeyService, times(1)).parseRoleDescriptors(anyString(), anyMap(), any());
        }
        assertThat(role.names().length, is(1));
        assertThat(role.names()[0], containsString("user_role_"));
    }

    @SuppressWarnings("unchecked")
    public void testApiKeyAuthUsesApiKeyServiceWithScopedRole() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(SECURITY_ENABLED_SETTINGS, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL))
        );
        ApiKeyService apiKeyService = spy(
            new ApiKeyService(
                SECURITY_ENABLED_SETTINGS,
                Clock.systemUTC(),
                mock(Client.class),
                mock(SecurityIndexManager.class),
                clusterService,
                mock(CacheInvalidatorRegistry.class),
                mock(ThreadPool.class),
                MeterRegistry.NOOP
            )
        );
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollection(), anyCollection(), eq(false), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivStore,
            null,
            apiKeyService,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        AuditUtil.getOrGenerateRequestId(threadContext);
        final TransportVersion version = randomFrom(
            TransportVersion.current(),
            TransportVersionUtils.randomVersionBetween(random(), TransportVersions.V_7_0_0, TransportVersions.V_7_8_1)
        );
        final Authentication authentication = createApiKeyAuthentication(
            apiKeyService,
            randomValueOtherThanMany(
                authc -> authc.getAuthenticationType() == AuthenticationType.API_KEY,
                () -> AuthenticationTestHelper.builder().build()
            ),
            Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[] { "manage" }, null, null)),
            Collections.singletonList(new RoleDescriptor("key_role_" + randomAlphaOfLength(8), new String[] { "monitor" }, null, null)),
            version
        );
        final String apiKeyId = (String) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_ID_KEY);

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        Role role = roleFuture.actionGet();
        assertThat(role.checkClusterAction("cluster:admin/foo", new EmptyRequest(), AuthenticationTestHelper.builder().build()), is(false));
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        if (version == TransportVersion.current()) {
            verify(apiKeyService).parseRoleDescriptorsBytes(
                apiKeyId,
                (BytesReference) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_ROLE_DESCRIPTORS_KEY),
                RoleReference.ApiKeyRoleType.ASSIGNED
            );
            verify(apiKeyService).parseRoleDescriptorsBytes(
                apiKeyId,
                (BytesReference) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
                RoleReference.ApiKeyRoleType.LIMITED_BY
            );
        } else {
            verify(apiKeyService).parseRoleDescriptors(
                apiKeyId,
                (Map<String, Object>) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_ROLE_DESCRIPTORS_KEY),
                RoleReference.ApiKeyRoleType.ASSIGNED
            );
            verify(apiKeyService).parseRoleDescriptors(
                apiKeyId,
                (Map<String, Object>) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY),
                RoleReference.ApiKeyRoleType.LIMITED_BY
            );
        }
        assertThat(role.names().length, is(1));
        assertThat(role.names()[0], containsString("user_role_"));
    }

    public void testGetRoleForCrossClusterAccessAuthentication() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(SECURITY_ENABLED_SETTINGS, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL))
        );
        final ApiKeyService apiKeyService = spy(
            new ApiKeyService(
                SECURITY_ENABLED_SETTINGS,
                Clock.systemUTC(),
                mock(Client.class),
                mock(SecurityIndexManager.class),
                clusterService,
                mock(CacheInvalidatorRegistry.class),
                mock(ThreadPool.class),
                MeterRegistry.NOOP
            )
        );
        final NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollection(), anyCollection(), eq(false), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivStore,
            null,
            apiKeyService,
            null,
            null,
            effectiveRoleDescriptors::set
        );
        AuditUtil.getOrGenerateRequestId(threadContext);
        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder()
            .crossClusterApiKey(randomAlphaOfLength(20))
            .metadata(Map.of(API_KEY_ROLE_DESCRIPTORS_KEY, new BytesArray("""
                {
                  "cross_cluster": {
                    "cluster": ["cross_cluster_search"],
                    "indices": [
                      { "names":["index*"], "privileges":["read","read_cross_cluster","view_index_metadata"] }
                    ]
                  }
                }""")))
            .build(false);
        final boolean emptyRemoteRole = randomBoolean();
        Authentication authentication = apiKeyAuthentication.toCrossClusterAccess(
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(
                emptyRemoteRole
                    ? RoleDescriptorsIntersection.EMPTY
                    : new RoleDescriptorsIntersection(
                        new RoleDescriptor(
                            Role.REMOTE_USER_ROLE_NAME,
                            null,
                            new RoleDescriptor.IndicesPrivileges[] {
                                RoleDescriptor.IndicesPrivileges.builder().indices("index1").privileges("read").build() },
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        )
                    )
            )
        );

        // Randomly serialize and deserialize the authentication object to simulate authentication getting sent across nodes
        if (randomBoolean()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                authentication.writeTo(out);
                final StreamInput in = out.bytes().streamInput();
                authentication = new Authentication(in);
            }
        }

        final PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        final Role role = roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));

        verify(apiKeyService, times(1)).parseRoleDescriptorsBytes(anyString(), any(BytesReference.class), any());
        assertThat(role.names().length, is(1));
        assertThat(role.names()[0], equalTo("cross_cluster"));

        // Smoke-test for authorization
        final Metadata indexMetadata = Metadata.builder()
            .put(IndexMetadata.builder("index1").settings(indexSettings(IndexVersion.current(), 1, 1)))
            .put(IndexMetadata.builder("index2").settings(indexSettings(IndexVersion.current(), 1, 1)))
            .build();
        final var emptyCache = new FieldPermissionsCache(Settings.EMPTY);
        assertThat(
            role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("index1"), indexMetadata.getProject(), emptyCache)
                .isGranted(),
            is(false == emptyRemoteRole)
        );
        assertThat(
            role.authorize(TransportCreateIndexAction.TYPE.name(), Sets.newHashSet("index1"), indexMetadata.getProject(), emptyCache)
                .isGranted(),
            is(false)
        );
        assertThat(
            role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("index2"), indexMetadata.getProject(), emptyCache)
                .isGranted(),
            is(false)
        );
    }

    public void testGetRolesForRunAs() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ServiceAccountService serviceAccountService = mock(ServiceAccountService.class);
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            null,
            apiKeyService,
            serviceAccountService,
            null,
            null
        );

        // API key run as
        final String apiKeyId = randomAlphaOfLength(20);
        final BytesReference roleDescriptorBytes = new BytesArray("{}");
        final BytesReference limitedByRoleDescriptorBytes = new BytesArray("{\"a\":{\"cluster\":[\"all\"]}}");

        final User authenticatedUser1 = new User("authenticated_user");
        final Authentication authentication1 = AuthenticationTestHelper.builder()
            .apiKey(apiKeyId)
            .metadata(
                Map.of(
                    API_KEY_ROLE_DESCRIPTORS_KEY,
                    roleDescriptorBytes,
                    API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    limitedByRoleDescriptorBytes
                )
            )
            .runAs()
            .build();

        final PlainActionFuture<Role> future1 = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication1.getAuthenticatingSubject(), future1);
        future1.actionGet();
        verify(apiKeyService).parseRoleDescriptorsBytes(apiKeyId, limitedByRoleDescriptorBytes, RoleReference.ApiKeyRoleType.LIMITED_BY);
    }

    public void testGetRoleForWorkflowWithRestriction() {
        final Settings settings = Settings.EMPTY;
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL))
        );
        final ApiKeyService apiKeyService = new ApiKeyService(
            settings,
            Clock.systemUTC(),
            mock(Client.class),
            mock(SecurityIndexManager.class),
            clusterService,
            mock(CacheInvalidatorRegistry.class),
            mock(ThreadPool.class),
            MeterRegistry.NOOP
        );
        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            callback.onResponse(Collections.emptyList());
            return null;
        }).when(privilegeStore).getPrivileges(isASet(), isASet(), eq(false), anyActionListener());
        final ThreadContext threadContext = new ThreadContext(settings);
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            settings,
            buildRolesProvider(null, null, null, null, licenseState),
            privilegeStore,
            threadContext,
            licenseState,
            cache,
            apiKeyService,
            mock(ServiceAccountService.class),
            TestProjectResolvers.singleProject(randomProjectIdOrDefault()),
            buildBitsetCache(),
            TestRestrictedIndices.RESTRICTED_INDICES,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            rds -> {}
        );

        final Workflow workflow = randomFrom(WorkflowResolver.allWorkflows());
        final String apiKeyId = randomAlphaOfLength(20);
        final BytesReference roleDescriptorBytes = new BytesArray(Strings.format("""
            {
                "base-role": {
                    "indices": [
                      {
                        "names": ["index-a"],
                        "privileges": ["read"]
                      }
                    ],
                    "restriction": {
                        "workflows": ["%s"]
                    }
                }
            }
            """, workflow.name()));
        final BytesReference limitedByRoleDescriptorBytes = new BytesArray("""
            {
                "limited-role": {
                    "indices": [
                      {
                        "names": ["index-a"],
                        "privileges": ["read"]
                      }
                    ]
                }
            }
            """);

        final User authenticatedUser1 = new User("authenticated_user");
        final Authentication authentication1 = AuthenticationTestHelper.builder()
            .apiKey(apiKeyId)
            .metadata(
                Map.of(
                    API_KEY_ROLE_DESCRIPTORS_KEY,
                    roleDescriptorBytes,
                    API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    limitedByRoleDescriptorBytes
                )
            )
            .user(authenticatedUser1)
            .build();

        // Tests that for a role with restriction, getRole returns:
        // 1. a usable role when originating workflow matches
        try (var ignored = threadContext.stashContext()) {
            WorkflowService.resolveWorkflowAndStoreInThreadContext(
                new TestBaseRestHandler(randomFrom(workflow.allowedRestHandlers())),
                threadContext
            );

            final PlainActionFuture<Role> future1 = new PlainActionFuture<>();
            compositeRolesStore.getRole(authentication1.getEffectiveSubject(), future1);
            Role role = future1.actionGet();
            assertThat(role.hasWorkflowsRestriction(), equalTo(true));
            assertThat(role, not(sameInstance(Role.EMPTY_RESTRICTED_BY_WORKFLOW)));
            assertThat(role.checkIndicesAction(TransportSearchAction.TYPE.name()), is(true));
        }

        // 2. an "empty-restricted" role if originating workflow does not match (or is null)
        try (var ignored = threadContext.stashContext()) {
            WorkflowService.resolveWorkflowAndStoreInThreadContext(new TestBaseRestHandler(randomAlphaOfLength(10)), threadContext);

            final PlainActionFuture<Role> future1 = new PlainActionFuture<>();
            compositeRolesStore.getRole(authentication1.getEffectiveSubject(), future1);
            Role role = future1.actionGet();
            assertThat(role.hasWorkflowsRestriction(), equalTo(true));
            assertThat(role, sameInstance(Role.EMPTY_RESTRICTED_BY_WORKFLOW));

        }
    }

    public void testGetRoleForWorkflowWithoutRestriction() {
        final Settings settings = Settings.EMPTY;
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL))
        );
        final ApiKeyService apiKeyService = new ApiKeyService(
            settings,
            Clock.systemUTC(),
            mock(Client.class),
            mock(SecurityIndexManager.class),
            clusterService,
            mock(CacheInvalidatorRegistry.class),
            mock(ThreadPool.class),
            MeterRegistry.NOOP
        );
        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            callback.onResponse(Collections.emptyList());
            return null;
        }).when(privilegeStore).getPrivileges(isASet(), isASet(), eq(false), anyActionListener());
        final ThreadContext threadContext = new ThreadContext(settings);
        final XPackLicenseState licenseState = new XPackLicenseState(() -> 0);
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            settings,
            buildRolesProvider(null, null, null, null, licenseState),
            privilegeStore,
            threadContext,
            licenseState,
            cache,
            apiKeyService,
            mock(ServiceAccountService.class),
            TestProjectResolvers.singleProject(randomProjectIdOrDefault()),
            buildBitsetCache(),
            TestRestrictedIndices.RESTRICTED_INDICES,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            rds -> {}
        );

        final String apiKeyId = randomAlphaOfLength(20);
        final BytesReference roleDescriptorBytes = new BytesArray(randomBoolean() ? """
            {
                "base-role": {
                    "indices": [
                      {
                        "names": ["index-a"],
                        "privileges": ["read"]
                      }
                    ]
                }
            }
            """ : "{}");
        final BytesReference limitedByRoleDescriptorBytes = new BytesArray("""
            {
                "limited-role": {
                    "cluster": ["all"],
                    "indices": [
                      {
                        "names": ["index-a", "index-b"],
                        "privileges": ["read"]
                      }
                    ]
                }
            }
            """);

        final User authenticatedUser1 = new User("authenticated_user");
        final Authentication authentication1 = AuthenticationTestHelper.builder()
            .apiKey(apiKeyId)
            .metadata(
                Map.of(
                    API_KEY_ROLE_DESCRIPTORS_KEY,
                    roleDescriptorBytes,
                    API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                    limitedByRoleDescriptorBytes
                )
            )
            .user(authenticatedUser1)
            .build();

        // Tests that for a role without restriction, getRole returns the same role regardless of the originating workflow.
        try (var ignored = threadContext.stashContext()) {
            boolean useExistingWorkflowAsOriginating = randomBoolean();
            Workflow existingWorkflow = randomFrom(WorkflowResolver.allWorkflows());
            WorkflowService.resolveWorkflowAndStoreInThreadContext(
                new TestBaseRestHandler(
                    useExistingWorkflowAsOriginating ? randomFrom(existingWorkflow.allowedRestHandlers()) : randomAlphaOfLengthBetween(4, 8)
                ),
                threadContext
            );

            final PlainActionFuture<Role> future1 = new PlainActionFuture<>();
            compositeRolesStore.getRole(authentication1.getEffectiveSubject(), future1);
            Role role = future1.actionGet();
            assertThat(role.hasWorkflowsRestriction(), equalTo(false));
            assertThat(role, not(sameInstance(Role.EMPTY_RESTRICTED_BY_WORKFLOW)));
            assertThat(role.checkIndicesAction(TransportSearchAction.TYPE.name()), is(true));
        }
    }

    public void testUsageStats() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        final Map<String, Object> fileRolesStoreUsageStats = Map.of("size", "1", "fls", Boolean.FALSE, "dls", Boolean.TRUE);
        when(fileRolesStore.usageStats()).thenReturn(fileRolesStoreUsageStats);

        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        final Map<String, Object> nativeRolesStoreUsageStats = Map.of();
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Map<String, Object>> usageStats = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            usageStats.onResponse(nativeRolesStoreUsageStats);
            return Void.TYPE;
        }).when(nativeRolesStore).usageStats(anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            null,
            mock(ApiKeyService.class),
            mock(ServiceAccountService.class),
            documentSubsetBitsetCache,
            null
        );

        PlainActionFuture<Map<String, Object>> usageStatsListener = new PlainActionFuture<>();
        compositeRolesStore.usageStats(usageStatsListener);
        Map<String, Object> usageStats = usageStatsListener.actionGet();
        assertThat(usageStats.get("file"), is(fileRolesStoreUsageStats));
        assertThat(usageStats.get("native"), is(nativeRolesStoreUsageStats));
        assertThat(usageStats.get("dls"), is(Map.of("bit_set_cache", documentSubsetBitsetCache.usageStats())));
    }

    public void testLoggingOfDeprecatedRoles() {
        List<RoleDescriptor> descriptors = new ArrayList<>();
        Function<Map<String, Object>, RoleDescriptor> newRole = metadata -> new RoleDescriptor(
            randomAlphaOfLengthBetween(4, 9),
            generateRandomStringArray(5, 5, false, true),
            null,
            null,
            null,
            null,
            metadata,
            null
        );

        RoleDescriptor deprecated1 = newRole.apply(MetadataUtils.getDeprecatedReservedMetadata("some reason"));
        RoleDescriptor deprecated2 = newRole.apply(MetadataUtils.getDeprecatedReservedMetadata("a different reason"));

        // Can't use getDeprecatedReservedMetadata because `Map.of` doesn't accept null values,
        // so we clone metadata with a real value and then remove that key
        final Map<String, Object> nullReasonMetadata = new HashMap<>(deprecated2.getMetadata());
        nullReasonMetadata.remove(MetadataUtils.DEPRECATED_REASON_METADATA_KEY);
        assertThat(nullReasonMetadata.keySet(), hasSize(deprecated2.getMetadata().size() - 1));
        RoleDescriptor deprecated3 = newRole.apply(nullReasonMetadata);

        descriptors.add(deprecated1);
        descriptors.add(deprecated2);
        descriptors.add(deprecated3);

        for (int i = randomIntBetween(2, 10); i > 0; i--) {
            // the non-deprecated metadata is randomly one of:
            // {}, {_deprecated:null}, {_deprecated:false},
            // {_reserved:true}, {_reserved:true,_deprecated:null}, {_reserved:true,_deprecated:false}
            Map<String, Object> metadata = randomBoolean() ? Map.of() : MetadataUtils.DEFAULT_RESERVED_METADATA;
            if (randomBoolean()) {
                metadata = new HashMap<>(metadata);
                metadata.put(MetadataUtils.DEPRECATED_METADATA_KEY, randomBoolean() ? null : false);
            }
            descriptors.add(newRole.apply(metadata));
        }
        Collections.shuffle(descriptors, random());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            null,
            null,
            null,
            null,
            null,
            null,
            mock(ServiceAccountService.class),
            null,
            null
        );

        // Use a LHS so that the random-shufle-order of the list is preserved
        compositeRolesStore.getRoleReferenceResolver().logDeprecatedRoles(new LinkedHashSet<>(descriptors));

        assertWarnings(
            "The role ["
                + deprecated1.getName()
                + "] is deprecated and will be removed in a future version of Elasticsearch."
                + " some reason",
            "The role ["
                + deprecated2.getName()
                + "] is deprecated and will be removed in a future version of Elasticsearch."
                + " a different reason",
            "The role ["
                + deprecated3.getName()
                + "] is deprecated and will be removed in a future version of Elasticsearch."
                + " Please check the documentation"
        );
    }

    public void testRoleResolutionIsProjectAware() {
        final AtomicReference<ProjectId> activeProject = new AtomicReference<>();
        final ProjectResolver projectResolver = new ProjectResolver() {
            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                throw new UnsupportedOperationException();
            }

            @Override
            public ProjectId getProjectId() {
                return activeProject.get();
            }
        };

        final String roleName = randomAlphaOfLengthBetween(4, 12);
        final Supplier<RoleDescriptor> descriptor = () -> new RoleDescriptor(
            roleName,
            randomSubsetOf(
                Set.of(
                    ClusterPrivilegeResolver.ALL,
                    ClusterPrivilegeResolver.MANAGE_SECURITY,
                    ClusterPrivilegeResolver.MANAGE,
                    ClusterPrivilegeResolver.MONITOR
                )
            ).stream().map(NamedClusterPrivilege::name).toArray(String[]::new),
            null,
            null,
            null,
            new String[] { activeProject.get().id() },
            Map.of("project.id", activeProject.get().id()),
            Map.of()
        );

        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());

        final Map<ProjectId, Integer> roleRetrievalCount = new HashMap<>();
        doAnswer((invocationOnMock) -> {
            Set<String> names = invocationOnMock.getArgument(0);
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> listener = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            if (names.contains(roleName)) {
                listener.onResponse(RoleRetrievalResult.success(Set.of(descriptor.get())));
            } else {
                listener.onResponse(RoleRetrievalResult.success(Set.of()));
            }
            roleRetrievalCount.compute(activeProject.get(), (k, v) -> v == null ? 1 : v + 1);
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(anySet(), anyActionListener());

        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            Settings.EMPTY,
            null,
            nativeRolesStore,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            projectResolver
        );

        final var project1 = randomUniqueProjectId();
        final var project2 = randomUniqueProjectId();
        final var project3 = randomUniqueProjectId();

        activeProject.set(project1);
        final Role role1a = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role1a.checkRunAs(project1.id()), is(true));
        assertThat(role1a.checkRunAs(project2.id()), is(false));
        assertThat(role1a.checkRunAs(project3.id()), is(false));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, aMapWithSize(1));

        activeProject.set(project2);
        final Role role2a = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role2a.checkRunAs(project1.id()), is(false));
        assertThat(role2a.checkRunAs(project2.id()), is(true));
        assertThat(role2a.checkRunAs(project3.id()), is(false));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, hasEntry(project2, 1));
        assertThat(roleRetrievalCount, aMapWithSize(2));

        activeProject.set(project1);
        final Role role1b = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role1b, sameInstance(role1a));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, hasEntry(project2, 1));
        assertThat(roleRetrievalCount, aMapWithSize(2));

        activeProject.set(project3);
        final Role role3a = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role3a.checkRunAs(project1.id()), is(false));
        assertThat(role3a.checkRunAs(project2.id()), is(false));
        assertThat(role3a.checkRunAs(project3.id()), is(true));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, hasEntry(project2, 1));
        assertThat(roleRetrievalCount, hasEntry(project3, 1));
        assertThat(roleRetrievalCount, aMapWithSize(3));

        final Role role3b = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role3b, sameInstance(role3a));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, hasEntry(project2, 1));
        assertThat(roleRetrievalCount, hasEntry(project3, 1));

        activeProject.set(project2);
        final Role role2b = getRoleForRoleNames(compositeRolesStore, roleName);
        assertThat(role2b, sameInstance(role2a));
        assertThat(roleRetrievalCount, hasEntry(project1, 1));
        assertThat(roleRetrievalCount, hasEntry(project2, 1));
        assertThat(roleRetrievalCount, hasEntry(project3, 1));
    }

    public void testCacheEntryIsReusedForIdenticalApiKeyRoles() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(anySet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);
        ApiKeyService apiKeyService = mock(ApiKeyService.class);
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollection(), anyCollection(), eq(false), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivStore,
            null,
            apiKeyService,
            null,
            null,
            rds -> effectiveRoleDescriptors.set(rds)
        );
        AuditUtil.getOrGenerateRequestId(threadContext);
        final BytesArray roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        final BytesArray limitedByRoleBytes = new BytesArray("{\"limitedBy role\": {\"cluster\": [\"all\"]}}");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(API_KEY_ID_KEY, "key-id-1");
        metadata.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        metadata.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, roleBytes);
        metadata.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleBytes);
        Authentication authentication = AuthenticationTestHelper.builder().apiKey().metadata(metadata).build();

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService).parseRoleDescriptorsBytes("key-id-1", roleBytes, RoleReference.ApiKeyRoleType.ASSIGNED);
        verify(apiKeyService).parseRoleDescriptorsBytes("key-id-1", limitedByRoleBytes, RoleReference.ApiKeyRoleType.LIMITED_BY);

        // Different API key with the same roles should read from cache
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put(API_KEY_ID_KEY, "key-id-2");
        metadata2.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        metadata2.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, roleBytes);
        metadata2.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleBytes);
        authentication = AuthenticationTestHelper.builder().apiKey().metadata(metadata2).build();
        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService, never()).parseRoleDescriptorsBytes(eq("key-id-2"), any(BytesReference.class), any());

        // Different API key with the same limitedBy role should read from cache, new role should be built
        final BytesArray anotherRoleBytes = new BytesArray("{\"b role\": {\"cluster\": [\"manage_security\"]}}");
        final Map<String, Object> metadata3 = new HashMap<>();
        metadata3.put(API_KEY_ID_KEY, "key-id-3");
        metadata3.put(AuthenticationField.API_KEY_NAME_KEY, randomBoolean() ? null : randomAlphaOfLengthBetween(1, 16));
        metadata3.put(AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY, anotherRoleBytes);
        metadata3.put(AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY, limitedByRoleBytes);
        authentication = AuthenticationTestHelper.builder().apiKey().metadata(metadata3).build();
        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRole(authentication.getEffectiveSubject(), roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService).parseRoleDescriptorsBytes("key-id-3", anotherRoleBytes, RoleReference.ApiKeyRoleType.ASSIGNED);
    }

    public void testXPackSecurityUserCanAccessAnyIndex() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            IsResourceAuthorizedPredicate predicate = getXPackSecurityRole().indices().allowedIndicesMatcher(action);

            IndexAbstraction index = mockIndexAbstraction(randomAlphaOfLengthBetween(3, 12));
            assertThat(predicate.test(index), Matchers.is(true));

            index = mockIndexAbstraction("." + randomAlphaOfLengthBetween(3, 12));
            assertThat(predicate.test(index), Matchers.is(true));

            index = mockIndexAbstraction(".security-" + randomIntBetween(1, 16));
            assertThat(predicate.test(index), Matchers.is(true));
        }
    }

    public void testSecurityProfileUserHasAccessForOnlyProfileIndex() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            IsResourceAuthorizedPredicate predicate = getSecurityProfileRole().indices().allowedIndicesMatcher(action);

            List.of(
                ".security-profile",
                ".security-profile-8",
                ".security-profile-" + randomIntBetween(0, 16) + randomAlphaOfLengthBetween(0, 10)
            ).forEach(name -> assertThat(predicate.test(mockIndexAbstraction(name)), is(true)));

            List.of(
                ".security-profile" + randomAlphaOfLengthBetween(1, 10),
                ".security-profile-" + randomAlphaOfLengthBetween(1, 10),
                ".security",
                ".security-" + randomIntBetween(0, 16) + randomAlphaOfLengthBetween(0, 10),
                "." + randomAlphaOfLengthBetween(1, 20)
            ).forEach(name -> assertThat(predicate.test(mockIndexAbstraction(name)), is(false)));
        }

        final Subject subject = mock(Subject.class);
        when(subject.getUser()).thenReturn(InternalUsers.SECURITY_PROFILE_USER);
        assertThat(CompositeRolesStore.tryGetRoleDescriptorForInternalUser(subject).get().getClusterPrivileges(), emptyArray());
    }

    public void testXPackUserCanAccessNonRestrictedIndices() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            IsResourceAuthorizedPredicate predicate = getXPackUserRole().indices().allowedIndicesMatcher(action);
            IndexAbstraction index = mockIndexAbstraction(randomAlphaOfLengthBetween(3, 12));
            if (false == TestRestrictedIndices.RESTRICTED_INDICES.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(true));
            }
            index = mockIndexAbstraction("." + randomAlphaOfLengthBetween(3, 12));
            if (false == TestRestrictedIndices.RESTRICTED_INDICES.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(true));
            }
        }
    }

    public void testXPackUserCannotAccessSecurityOrAsyncSearch() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            IsResourceAuthorizedPredicate predicate = getXPackUserRole().indices().allowedIndicesMatcher(action);
            for (String index : TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES) {
                assertThat(predicate.test(mockIndexAbstraction(index)), Matchers.is(false));
            }
            assertThat(
                predicate.test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
                Matchers.is(false)
            );
        }
    }

    public void testAsyncSearchUserCannotAccessNonRestrictedIndices() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            IsResourceAuthorizedPredicate predicate = getAsyncSearchUserRole().indices().allowedIndicesMatcher(action);
            IndexAbstraction index = mockIndexAbstraction(randomAlphaOfLengthBetween(3, 12));
            if (false == TestRestrictedIndices.RESTRICTED_INDICES.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(false));
            }
            index = mockIndexAbstraction("." + randomAlphaOfLengthBetween(3, 12));
            if (false == TestRestrictedIndices.RESTRICTED_INDICES.isRestricted(index.getName())) {
                assertThat(predicate.test(index), Matchers.is(false));
            }
        }
    }

    public void testAsyncSearchUserCanAccessOnlyAsyncSearchRestrictedIndices() {
        for (String action : Arrays.asList(
            TransportGetAction.TYPE.name(),
            TransportDeleteAction.NAME,
            TransportSearchAction.TYPE.name(),
            TransportIndexAction.NAME
        )) {
            final IsResourceAuthorizedPredicate predicate = getAsyncSearchUserRole().indices().allowedIndicesMatcher(action);
            for (String index : TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES) {
                assertThat(predicate.test(mockIndexAbstraction(index)), Matchers.is(false));
            }
            assertThat(
                predicate.test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 3))),
                Matchers.is(true)
            );
        }
    }

    public void testAsyncSearchUserHasNoClusterPrivileges() {
        for (String action : Arrays.asList(
            ClusterStateAction.NAME,
            GetWatchAction.NAME,
            TransportClusterStatsAction.TYPE.name(),
            TransportNodesStatsAction.TYPE.name()
        )) {
            assertThat(
                getAsyncSearchUserRole().cluster().check(action, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
                Matchers.is(false)
            );
        }
    }

    public void testXpackUserHasClusterPrivileges() {
        for (String action : Arrays.asList(
            ClusterStateAction.NAME,
            GetWatchAction.NAME,
            TransportClusterStatsAction.TYPE.name(),
            TransportNodesStatsAction.TYPE.name()
        )) {
            assertThat(
                getXPackUserRole().cluster().check(action, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
                Matchers.is(true)
            );
        }
    }

    public void testGetRoleDescriptorsListForInternalUsers() {
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            null,
            null,
            null,
            null,
            null,
            null,
            mock(ServiceAccountService.class),
            null,
            null
        );

        final Subject subject = mock(Subject.class);
        when(subject.getUser()).thenReturn(InternalUsers.SYSTEM_USER);
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> compositeRolesStore.getRoleDescriptors(subject, ActionListener.noop())
        );
        assertThat(e1.getMessage(), equalTo("should never try to get the roles for internal user [" + SystemUser.NAME + "]"));

        for (var internalUser : AuthenticationTestHelper.internalUsersWithLocalRoleDescriptor()) {
            when(subject.getUser()).thenReturn(internalUser);
            final PlainActionFuture<Set<RoleDescriptor>> future = new PlainActionFuture<>();
            compositeRolesStore.getRoleDescriptors(subject, future);
            assertThat(future.actionGet(), equalTo(Set.of(internalUser.getLocalClusterRoleDescriptor().get())));
        }
    }

    public void testForkOnExpensiveRole() {
        final RoleDescriptor expectedRoleDescriptor = randomValueOtherThanMany(
            rd -> false == rd.hasApplicationPrivileges(),
            // skip workflow restrictions since these can produce empty, nameless roles
            () -> RoleDescriptorTestHelper.builder().allowRestriction(false).build()
        );
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> {
            callback.onResponse(RoleRetrievalResult.success(Set.of(expectedRoleDescriptor)));
        };
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            Collections.emptyList()
        );
        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, List.of(expectedRoleDescriptor.getName()), future);
        assertThat(future.actionGet().names(), equalTo(new String[] { expectedRoleDescriptor.getName() }));

        verify(mockRoleBuildingExecutor, times(1)).execute(any());
    }

    public void testDoNotForkOnInexpensiveRole() {
        final RoleDescriptor expectedRoleDescriptor = randomValueOtherThanMany(
            rd -> rd.isUsingDocumentOrFieldLevelSecurity() || rd.hasApplicationPrivileges(),
            // skip workflow restrictions since these can produce empty, nameless roles
            () -> RoleDescriptorTestHelper.builder().allowRestriction(false).build()
        );
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> {
            callback.onResponse(RoleRetrievalResult.success(Set.of(expectedRoleDescriptor)));
        };
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            Collections.emptyList()
        );
        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(compositeRolesStore, List.of(expectedRoleDescriptor.getName()), future);
        assertThat(future.actionGet().names(), equalTo(new String[] { expectedRoleDescriptor.getName() }));

        verify(mockRoleBuildingExecutor, never()).execute(any());
    }

    public void testGetRoleDescriptorsListUsesRoleStoreToResolveRoleWithInternalRoleName() {
        String roleName = AuthenticationTestHelper.randomInternalRoleName();
        RoleDescriptor expectedRoleDescriptor = new RoleDescriptor(roleName, null, null, null);
        final Consumer<ActionListener<RoleRetrievalResult>> rolesHandler = callback -> {
            callback.onResponse(RoleRetrievalResult.success(Set.of(expectedRoleDescriptor)));
        };
        final Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> privilegesHandler = callback -> callback.onResponse(
            Collections.emptyList()
        );
        final CompositeRolesStore compositeRolesStore = setupRolesStore(rolesHandler, privilegesHandler);

        final Subject subject = buildSubjectWithRoles(new String[] { roleName });
        final PlainActionFuture<Set<RoleDescriptor>> future = new PlainActionFuture<>();
        compositeRolesStore.getRoleDescriptors(subject, future);
        assertThat(future.actionGet(), equalTo(Set.of(expectedRoleDescriptor)));
    }

    private Role getRoleForRoleNames(CompositeRolesStore store, String... roleNames) {
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        getRoleForRoleNames(store, Set.of(roleNames), future);
        return future.actionGet();
    }

    private void getRoleForRoleNames(CompositeRolesStore rolesStore, Collection<String> roleNames, ActionListener<Role> listener) {
        final Subject subject = buildSubjectWithRoles(roleNames.toArray(String[]::new));
        rolesStore.getRole(subject, listener);
    }

    private Subject buildSubjectWithRoles(String[] roleNames) {
        final Subject subject = mock(Subject.class);
        when(subject.getRoleReferenceIntersection(any())).thenReturn(
            new RoleReferenceIntersection(new RoleReference.NamedRoleReference(roleNames))
        );
        return subject;
    }

    private Role getXPackSecurityRole() {
        return getInternalUserRole(InternalUsers.XPACK_SECURITY_USER);
    }

    private Role getSecurityProfileRole() {
        return getInternalUserRole(InternalUsers.SECURITY_PROFILE_USER);
    }

    private Role getXPackUserRole() {
        return getInternalUserRole(InternalUsers.XPACK_USER);
    }

    private Role getAsyncSearchUserRole() {
        return getInternalUserRole(InternalUsers.ASYNC_SEARCH_USER);
    }

    private Role getInternalUserRole(User internalUser) {
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(
            SECURITY_ENABLED_SETTINGS,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        final Subject subject = new Subject(internalUser, new RealmRef("__attach", "__attach", randomAlphaOfLength(8)));
        final Role role = compositeRolesStore.tryGetRoleForInternalUser(subject);
        assertThat("Role for " + subject, role, notNullValue());
        return role;
    }

    private CompositeRolesStore buildCompositeRolesStore(
        Settings settings,
        @Nullable FileRolesStore fileRolesStore,
        @Nullable NativeRolesStore nativeRolesStore,
        @Nullable ReservedRolesStore reservedRolesStore,
        @Nullable NativePrivilegeStore privilegeStore,
        @Nullable XPackLicenseState licenseState,
        @Nullable ApiKeyService apiKeyService,
        @Nullable ServiceAccountService serviceAccountService,
        @Nullable DocumentSubsetBitsetCache documentSubsetBitsetCache,
        @Nullable Consumer<Collection<RoleDescriptor>> roleConsumer
    ) {
        return buildCompositeRolesStore(
            settings,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            null,
            privilegeStore,
            licenseState,
            apiKeyService,
            serviceAccountService,
            documentSubsetBitsetCache,
            roleConsumer,
            null,
            null,
            null
        );
    }

    private CompositeRolesStore buildCompositeRolesStore(
        Settings settings,
        @Nullable FileRolesStore fileRolesStore,
        @Nullable NativeRolesStore nativeRolesStore,
        @Nullable ReservedRolesStore reservedRolesStore,
        @Nullable Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders,
        @Nullable NativePrivilegeStore privilegeStore,
        @Nullable XPackLicenseState licenseState,
        @Nullable ApiKeyService apiKeyService,
        @Nullable ServiceAccountService serviceAccountService,
        @Nullable DocumentSubsetBitsetCache documentSubsetBitsetCache,
        @Nullable Consumer<Collection<RoleDescriptor>> roleConsumer,
        @Nullable BiConsumer<CompositeRolesStore, ProjectId> onInvalidation,
        @Nullable WorkflowService workflowService,
        @Nullable ProjectResolver projectResolver
    ) {
        if (licenseState == null) {
            licenseState = new XPackLicenseState(() -> 0);
        }

        final RoleProviders roleProviders = buildRolesProvider(
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            customRoleProviders,
            licenseState
        );

        if (privilegeStore == null) {
            privilegeStore = mock(NativePrivilegeStore.class);
            doAnswer((invocationOnMock) -> {
                @SuppressWarnings("unchecked")
                ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                    Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
                callback.onResponse(Collections.emptyList());
                return null;
            }).when(privilegeStore).getPrivileges(isASet(), isASet(), eq(false), anyActionListener());
        }
        if (apiKeyService == null) {
            apiKeyService = mock(ApiKeyService.class);
        }
        if (serviceAccountService == null) {
            serviceAccountService = mock(ServiceAccountService.class);
        }
        if (documentSubsetBitsetCache == null) {
            documentSubsetBitsetCache = buildBitsetCache();
        }
        if (roleConsumer == null) {
            roleConsumer = rds -> {};
        }
        if (workflowService == null) {
            workflowService = mock(WorkflowService.class);
        }
        if (projectResolver == null) {
            projectResolver = TestProjectResolvers.singleProject(randomProjectIdOrDefault());
        }

        return new CompositeRolesStore(
            settings,
            roleProviders,
            privilegeStore,
            new ThreadContext(settings),
            licenseState,
            cache,
            apiKeyService,
            serviceAccountService,
            projectResolver,
            documentSubsetBitsetCache,
            TestRestrictedIndices.RESTRICTED_INDICES,
            mockRoleBuildingExecutor,
            roleConsumer
        ) {
            @Override
            public void invalidateAll() {
                if (onInvalidation == null) {
                    super.invalidateAll();
                } else {
                    onInvalidation.accept(this, null);
                }
            }

            @Override
            public void invalidateProject(ProjectId projectId) {
                if (onInvalidation == null) {
                    super.invalidateProject(projectId);
                } else {
                    onInvalidation.accept(this, projectId);
                }
            }
        };
    }

    private RoleProviders buildRolesProvider(
        @Nullable FileRolesStore fileRolesStore,
        @Nullable NativeRolesStore nativeRolesStore,
        @Nullable ReservedRolesStore reservedRolesStore,
        @Nullable Map<String, List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>> customRoleProviders,
        @Nullable XPackLicenseState licenseState
    ) {
        if (fileRolesStore == null) {
            fileRolesStore = mock(FileRolesStore.class);
            doCallRealMethod().when(fileRolesStore).accept(anySet(), anyActionListener());
            when(fileRolesStore.roleDescriptors(anySet())).thenReturn(Collections.emptySet());
        }
        if (nativeRolesStore == null) {
            nativeRolesStore = mock(NativeRolesStore.class);
            doCallRealMethod().when(nativeRolesStore).accept(anySet(), anyActionListener());
            doAnswer((invocationOnMock) -> {
                @SuppressWarnings("unchecked")
                ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
                callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
                return null;
            }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        }
        if (reservedRolesStore == null) {
            reservedRolesStore = new ReservedRolesStore();
        }
        if (licenseState == null) {
            licenseState = new XPackLicenseState(() -> 0);
        }
        if (customRoleProviders == null) {
            customRoleProviders = Map.of();
        }
        return new RoleProviders(reservedRolesStore, fileRolesStore, nativeRolesStore, customRoleProviders, licenseState);
    }

    private DocumentSubsetBitsetCache buildBitsetCache() {
        return new DocumentSubsetBitsetCache(Settings.EMPTY, mock(ThreadPool.class));
    }

    private static class InMemoryRolesProvider implements BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> {
        private final Function<Set<String>, RoleRetrievalResult> roleDescriptorsFunc;

        InMemoryRolesProvider(Function<Set<String>, RoleRetrievalResult> roleDescriptorsFunc) {
            this.roleDescriptorsFunc = roleDescriptorsFunc;
        }

        @Override
        public void accept(Set<String> roles, ActionListener<RoleRetrievalResult> listener) {
            listener.onResponse(roleDescriptorsFunc.apply(roles));
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

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(
            randomFrom(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        return mock;
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> Set<T> isASet() {
        return isA(Set.class);
    }

    private RoleDescriptor roleDescriptorWithRemoteIndicesPrivileges(
        final String name,
        final RoleDescriptor.RemoteIndicesPrivileges[] rips
    ) {
        return roleDescriptorWithIndicesPrivileges(name, rips, null);
    }

    private RoleDescriptor roleDescriptorWithIndicesPrivileges(final String name, final IndicesPrivileges[] ips) {
        return roleDescriptorWithIndicesPrivileges(name, null, ips);
    }

    private RoleDescriptor roleDescriptorWithIndicesPrivileges(
        final String name,
        final RoleDescriptor.RemoteIndicesPrivileges[] rips,
        final IndicesPrivileges[] ips
    ) {
        return new RoleDescriptor(name, null, ips, null, null, null, null, null, rips, null, null, null);
    }

    private RoleDescriptor roleDescriptorWithRemoteClusterPrivileges(final String name, RemoteClusterPermissions remoteClusterPermissions) {
        return new RoleDescriptor(name, null, null, null, null, null, null, null, null, remoteClusterPermissions, null, null);
    }

    private RemoteClusterPermissions getValidRemoteClusterPermissions(String[] aliases) {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(
                RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]),
                aliases
            )
        );
    }

    private Role buildRole(final RoleDescriptor... roleDescriptors) {
        final FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback = (ActionListener<
                Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[3];
            callback.onResponse(Collections.emptyList());
            return null;
        }).when(privilegeStore).getPrivileges(isASet(), isASet(), eq(false), anyActionListener());
        CompositeRolesStore.buildRoleFromDescriptors(
            Sets.newHashSet(roleDescriptors),
            cache,
            privilegeStore,
            TestRestrictedIndices.RESTRICTED_INDICES,
            future
        );
        return future.actionGet();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private void assertHasRemoteIndicesGroupsForClusters(
        final RemoteIndicesPermission permission,
        final Set<String>... remoteClustersAliases
    ) {
        assertThat(
            permission.remoteIndicesGroups().stream().map(RemoteIndicesPermission.RemoteIndicesGroup::remoteClusterAliases).toList(),
            containsInAnyOrder(remoteClustersAliases)
        );
    }

    private void assertValidRemoteClusterPermissions(RemoteClusterPermissions permissions, String[] aliases) {
        assertValidRemoteClusterPermissionsParent(permissions, aliases);
        assertValidRemoteClusterPermissionsGroups(permissions.groups(), aliases);

    }

    private void assertValidRemoteClusterPermissionsParent(RemoteClusterPermissions permissions, String[] aliases) {
        assertTrue(permissions.hasAnyPrivileges());
        for (String alias : aliases) {
            assertTrue(permissions.hasAnyPrivileges(alias));
            assertFalse(permissions.hasAnyPrivileges(randomValueOtherThan(alias, () -> randomAlphaOfLength(5))));
            assertThat(
                permissions.collapseAndRemoveUnsupportedPrivileges(alias, TransportVersion.current()),
                arrayContaining(RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]))
            );
        }
    }

    private void assertValidRemoteClusterPermissionsGroups(List<RemoteClusterPermissionGroup> groups, String[] aliases) {
        for (RemoteClusterPermissionGroup group : groups) {
            assertThat(group.remoteClusterAliases(), arrayContaining(aliases));
            assertThat(
                group.clusterPrivileges(),
                arrayContaining(RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]))
            );
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private void assertHasRemoteIndexGroupsForClusters(
        final RemoteIndicesPermission permission,
        final Set<String> remoteClustersAliases,
        final Matcher<IndicesPermission.Group>... matchers
    ) {
        assertThat(
            permission.remoteIndicesGroups()
                .stream()
                .filter(it -> it.remoteClusterAliases().equals(remoteClustersAliases))
                .findFirst()
                .get()
                .indicesPermissionGroups(),
            containsInAnyOrder(matchers)
        );
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private void assertHasIndexGroups(final IndicesPermission permission, final Matcher<IndicesPermission.Group>... matchers) {
        assertThat(permission.groups(), arrayContainingInAnyOrder(matchers));
    }

    private static Matcher<IndicesPermission.Group> indexGroup(final String... indices) {
        return indexGroup(IndexPrivilege.READ, false, indices);
    }

    private static Matcher<IndicesPermission.Group> indexGroup(
        final IndexPrivilege privilege,
        final boolean allowRestrictedIndices,
        final String... indices
    ) {
        return indexGroup(
            privilege,
            allowRestrictedIndices,
            null,
            new FieldPermissionsDefinition.FieldGrantExcludeGroup(null, null),
            indices
        );
    }

    private static Matcher<IndicesPermission.Group> indexGroup(
        final IndexPrivilege privilege,
        final boolean allowRestrictedIndices,
        @Nullable final String query,
        final FieldPermissionsDefinition.FieldGrantExcludeGroup flsGroup,
        final String... indices
    ) {
        return indexGroup(privilege, allowRestrictedIndices, query, new FieldPermissionsDefinition(Set.of(flsGroup)), indices);
    }

    private static Matcher<IndicesPermission.Group> indexGroup(
        final IndexPrivilege privilege,
        final boolean allowRestrictedIndices,
        @Nullable final String query,
        final FieldPermissionsDefinition fieldPermissionsDefinition,
        final String... indices
    ) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object o) {
                if (false == o instanceof IndicesPermission.Group) {
                    return false;
                }
                final IndicesPermission.Group group = (IndicesPermission.Group) o;
                return equalTo(query == null ? null : Set.of(new BytesArray(query))).matches(group.getQuery())
                    && equalTo(privilege).matches(group.privilege())
                    && equalTo(allowRestrictedIndices).matches(group.allowRestrictedIndices())
                    && equalTo(new FieldPermissions(fieldPermissionsDefinition)).matches(group.getFieldPermissions())
                    && arrayContaining(indices).matches(group.indices());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                    "IndicesPermission.Group with fields{"
                        + "privilege="
                        + privilege
                        + ", allowRestrictedIndices="
                        + allowRestrictedIndices
                        + ", indices="
                        + Strings.arrayToCommaDelimitedString(indices)
                        + ", query="
                        + query
                        + ", fieldPermissionsDefinition="
                        + fieldPermissionsDefinition
                        + '}'
                );
            }
        };
    }
}
