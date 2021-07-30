/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.TestUtils.UpdatableLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequest.Empty;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ActionClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.security.authc.ApiKeyService.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.security.authc.ApiKeyServiceTests.Utils.createApiKeyAuthentication;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeRolesStoreTests extends ESTestCase {

    private static final Settings SECURITY_ENABLED_SETTINGS = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
            .build();

    private final FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
    private final String concreteSecurityIndexName = randomFrom(
        RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6, RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);

    public void testRolesWhenDlsFlsUnlicensed() throws IOException {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_DLS_FLS)).thenReturn(false);
        RoleDescriptor flsRole = new RoleDescriptor("fls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .grantedFields("*")
                        .deniedFields("foo")
                        .indices("*")
                        .privileges("read")
                        .build()
        }, null);
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);
        RoleDescriptor dlsRole = new RoleDescriptor("dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .query(matchAllBytes)
                        .build()
        }, null);
        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(matchAllBytes)
                        .build()
        }, null);
        RoleDescriptor noFlsDlsRole = new RoleDescriptor("no_fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .build()
        }, null);
        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());

        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(Settings.EMPTY, fileRolesStore, null,
            null, null, licenseState, null, null,
            null, rds -> effectiveRoleDescriptors.set(rds));

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("dls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls_dls"), roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("no_fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(noFlsDlsRole));
        effectiveRoleDescriptors.set(null);
    }

    public void testRolesWhenDlsFlsLicensed() throws IOException {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_DLS_FLS)).thenReturn(true);
        RoleDescriptor flsRole = new RoleDescriptor("fls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .grantedFields("*")
                        .deniedFields("foo")
                        .indices("*")
                        .privileges("read")
                        .build()
        }, null);
        BytesReference matchAllBytes = XContentHelper.toXContent(QueryBuilders.matchAllQuery(), XContentType.JSON, false);
        RoleDescriptor dlsRole = new RoleDescriptor("dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .query(matchAllBytes)
                        .build()
        }, null);
        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(matchAllBytes)
                        .build()
        }, null);
        RoleDescriptor noFlsDlsRole = new RoleDescriptor("no_fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .build()
        }, null);
        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(Settings.EMPTY, fileRolesStore, null,
            null, null, licenseState, null, null,
            null, rds -> effectiveRoleDescriptors.set(rds));

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(flsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(dlsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(flsDlsRole));
        effectiveRoleDescriptors.set(null);

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("no_fls_dls"), roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(noFlsDlsRole));
        effectiveRoleDescriptors.set(null);
    }

    public void testNegativeLookupsAreCached() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());

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
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback =
                (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[2];
            callback.onResponse(Collections.emptyList());
            return null;
        }).when(nativePrivilegeStore).getPrivileges(anySetOf(String.class), anySetOf(String.class), anyActionListener());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(SECURITY_ENABLED_SETTINGS,
            fileRolesStore, nativeRolesStore, reservedRolesStore, nativePrivilegeStore, null, null, null,
            null, rds -> effectiveRoleDescriptors.set(rds));
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final boolean getSuperuserRole = randomBoolean()
                && roleName.equals(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()) == false;
        final Set<String> names = getSuperuserRole ? Sets.newHashSet(roleName, ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())
                : Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(names, future);
            future.actionGet();
            if (getSuperuserRole && i == 0) {
                assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR));
                effectiveRoleDescriptors.set(null);
            } else {
                assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
            }
        }

        if (getSuperuserRole && numberOfTimesToCall > 0) {
            // the superuser role was requested so we get the role descriptors again
            verify(reservedRolesStore, times(2)).accept(anySetOf(String.class), anyActionListener());
            verify(nativePrivilegeStore).getPrivileges(isASet(), isASet(), anyActionListener());
        }
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore, nativePrivilegeStore);
    }

    public void testNegativeLookupsCacheDisabled() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final Settings settings = Settings.builder().put(SECURITY_ENABLED_SETTINGS)
            .put("xpack.security.authz.store.roles.negative_lookup_cache.max_size", 0)
            .build();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(settings, fileRolesStore, nativeRolesStore,
            reservedRolesStore, mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(settings),
            new XPackLicenseState(settings, () -> 0), cache, mock(ApiKeyService.class),
            mock(ServiceAccountService.class), documentSubsetBitsetCache,
            rds -> effectiveRoleDescriptors.set(rds));
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        assertFalse(compositeRolesStore.isValueInNegativeLookupCache(roleName));
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);
    }

    public void testNegativeLookupsAreNotCachedWithFailures() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, mock(ApiKeyService.class),
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> effectiveRoleDescriptors.set(rds));
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton(roleName), future);
        final Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        verify(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final Set<String> names = Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(names, future);
            future.actionGet();
            assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
            effectiveRoleDescriptors.set(null);
        }

        assertFalse(compositeRolesStore.isValueInNegativeLookupCache(roleName));
        verify(reservedRolesStore, times(numberOfTimesToCall + 1)).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore, times(numberOfTimesToCall + 1)).accept(anySetOf(String.class), anyActionListener());
        verify(fileRolesStore, times(numberOfTimesToCall + 1)).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore, times(numberOfTimesToCall + 1)).accept(anySetOf(String.class), anyActionListener());
        verify(nativeRolesStore, times(numberOfTimesToCall + 1)).getRoleDescriptors(isASet(), anyActionListener());
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);
    }


    public void testCustomRolesProviders() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final RoleDescriptor roleAProvider1 = new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null);
        final InMemoryRolesProvider inMemoryProvider1 = spy(new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(roleAProvider1);
            }
            return RoleRetrievalResult.success(descriptors);
        }));

        final RoleDescriptor roleBProvider2 = new RoleDescriptor("roleB", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("bar").grantedFields("*").build()
                    }, null);
        final InMemoryRolesProvider inMemoryProvider2 = spy(new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                // both role providers can resolve role A, this makes sure that if the first
                // role provider in order resolves a role, the second provider does not override it
                descriptors.add(new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("WRITE").indices("*").grantedFields("*").build()
                    }, null));
            }
            if (roles.contains("roleB")) {
                descriptors.add(roleBProvider2);
            }
            return RoleRetrievalResult.success(descriptors);
        }));

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final CompositeRolesStore compositeRolesStore =
                new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                                mock(NativePrivilegeStore.class), Arrays.asList(inMemoryProvider1, inMemoryProvider2),
                                new ThreadContext(SECURITY_ENABLED_SETTINGS), new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0),
                                cache, mock(ApiKeyService.class), mock(ServiceAccountService.class), documentSubsetBitsetCache,
                                rds -> effectiveRoleDescriptors.set(rds));

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(roleNames, future);
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
        verify(inMemoryProvider1).accept(anySetOf(String.class), anyActionListener());
        verify(inMemoryProvider2).accept(anySetOf(String.class), anyActionListener());

        final int numberOfTimesToCall = scaledRandomIntBetween(1, 8);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(Collections.singleton("unknown"), future);
            future.actionGet();
            if (i == 0) {
                assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
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
        RoleDescriptor flsRole = new RoleDescriptor("fls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .grantedFields("*")
                        .deniedFields("L1.*", "L2.*")
                        .indices("*")
                        .privileges("read")
                        .query("{ \"match\": {\"eventType.typeCode\": \"foo\"} }")
                        .build()
        }, null);
        RoleDescriptor addsL1Fields = new RoleDescriptor("dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .grantedFields("L1.*")
                        .privileges("read")
                        .query("{ \"match\": {\"eventType.typeCode\": \"foo\"} }")
                        .build()
        }, null);
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        CompositeRolesStore.buildRoleFromDescriptors(Sets.newHashSet(flsRole, addsL1Fields), cache, null, future);
        Role role = future.actionGet();

        Metadata metadata = Metadata.builder()
                .put(new IndexMetadata.Builder("test")
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build();
        Map<String, IndicesAccessControl.IndexAccessControl> acls = role.indices().authorize("indices:data/read/search",
            Collections.singleton("test"), metadata.getIndicesLookup(), cache);
        assertFalse(acls.isEmpty());
        assertTrue(acls.get("test").getFieldPermissions().grantsAccessTo("L1.foo"));
        assertFalse(acls.get("test").getFieldPermissions().grantsAccessTo("L2.foo"));
        assertTrue(acls.get("test").getFieldPermissions().grantsAccessTo("L3.foo"));
    }

    public void testMergingBasicRoles() {
        final TransportRequest request1 = mock(TransportRequest.class);
        final TransportRequest request2 = mock(TransportRequest.class);
        final TransportRequest request3 = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        ConfigurableClusterPrivilege ccp1 = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                builder.add(this, ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    req -> req == request1);
                return builder;
            }
        };
        RoleDescriptor role1 = new RoleDescriptor("r1", new String[]{"monitor"}, new IndicesPrivileges[]{
            IndicesPrivileges.builder()
                .indices("abc-*", "xyz-*")
                .privileges("read")
                .build(),
            IndicesPrivileges.builder()
                .indices("ind-1-*")
                .privileges("all")
                .build(),
        }, new RoleDescriptor.ApplicationResourcePrivileges[]{
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("app1")
                .resources("user/*")
                .privileges("read", "write")
                .build(),
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("app1")
                .resources("settings/*")
                .privileges("read")
                .build()
        }, new ConfigurableClusterPrivilege[] { ccp1 },
        new String[]{"app-user-1"}, null, null);

        ConfigurableClusterPrivilege ccp2 = new MockConfigurableClusterPrivilege() {
            @Override
            public ClusterPermission.Builder buildPermission(ClusterPermission.Builder builder) {
                builder.add(this, ((ActionClusterPrivilege) ClusterPrivilegeResolver.MANAGE_SECURITY).getAllowedActionPatterns(),
                    req -> req == request2);
                return builder;
            }
        };
        RoleDescriptor role2 = new RoleDescriptor("r2", new String[]{"manage_saml"}, new IndicesPrivileges[]{
            IndicesPrivileges.builder()
                .indices("abc-*", "ind-2-*")
                .privileges("all")
                .build()
        }, new RoleDescriptor.ApplicationResourcePrivileges[]{
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("app2a")
                .resources("*")
                .privileges("all")
                .build(),
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("app2b")
                .resources("*")
                .privileges("read")
                .build()
        }, new ConfigurableClusterPrivilege[] { ccp2 },
        new String[]{"app-user-2"}, null, null);

        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final NativePrivilegeStore privilegeStore = mock(NativePrivilegeStore.class);
        doAnswer(inv -> {
            assertEquals(3, inv.getArguments().length);
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
                = (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) inv.getArguments()[2];
            Set<ApplicationPrivilegeDescriptor> set = new HashSet<>();
            Arrays.asList("app1", "app2a", "app2b").forEach(
                app -> Arrays.asList("read", "write", "all").forEach(
                    perm -> set.add(
                        new ApplicationPrivilegeDescriptor(app, perm, Collections.emptySet(), Collections.emptyMap())
                    )));
            listener.onResponse(set);
            return null;
        }).when(privilegeStore).getPrivileges(anyCollectionOf(String.class), anyCollectionOf(String.class), anyActionListener());
        CompositeRolesStore.buildRoleFromDescriptors(Sets.newHashSet(role1, role2), cache, privilegeStore, future);
        Role role = future.actionGet();

        assertThat(role.cluster().check(ClusterStateAction.NAME, randomFrom(request1, request2, request3), authentication), equalTo(true));
        assertThat(role.cluster().check(SamlAuthenticateAction.NAME, randomFrom(request1, request2, request3), authentication),
            equalTo(true));
        assertThat(role.cluster().check(ClusterUpdateSettingsAction.NAME, randomFrom(request1, request2, request3), authentication),
            equalTo(false));

        assertThat(role.cluster().check(PutUserAction.NAME, randomFrom(request1, request2), authentication), equalTo(true));
        assertThat(role.cluster().check(PutUserAction.NAME, request3, authentication), equalTo(false));

        final Predicate<IndexAbstraction> allowedRead = role.indices().allowedIndicesMatcher(GetAction.NAME);
        assertThat(allowedRead.test(mockIndexAbstraction("abc-123")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("xyz-000")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-1-a")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-2-a")), equalTo(true));
        assertThat(allowedRead.test(mockIndexAbstraction("foo")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("abc")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("xyz")), equalTo(false));
        assertThat(allowedRead.test(mockIndexAbstraction("ind-3-a")), equalTo(false));

        final Predicate<IndexAbstraction> allowedWrite = role.indices().allowedIndicesMatcher(IndexAction.NAME);
        assertThat(allowedWrite.test(mockIndexAbstraction("abc-123")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("xyz-000")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-1-a")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-2-a")), equalTo(true));
        assertThat(allowedWrite.test(mockIndexAbstraction("foo")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("abc")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("xyz")), equalTo(false));
        assertThat(allowedWrite.test(mockIndexAbstraction("ind-3-a")), equalTo(false));

        role.application().grants(new ApplicationPrivilege("app1", "app1-read", "write"), "user/joe");
        role.application().grants(new ApplicationPrivilege("app1", "app1-read", "read"), "settings/hostname");
        role.application().grants(new ApplicationPrivilege("app2a", "app2a-all", "all"), "user/joe");
        role.application().grants(new ApplicationPrivilege("app2b", "app2b-read", "read"), "settings/hostname");
    }

    public void testCustomRolesProviderFailures() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
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
                descriptors.add(new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null));
            }
            return RoleRetrievalResult.success(descriptors);
        });

        final BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>> failingProvider =
            (roles, listener) -> listener.onFailure(new Exception("fake failure"));

        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Arrays.asList(inMemoryProvider1, failingProvider),
                new ThreadContext(SECURITY_ENABLED_SETTINGS), new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0),
                cache, mock(ApiKeyService.class), mock(ServiceAccountService.class),
                documentSubsetBitsetCache, rds -> effectiveRoleDescriptors.set(rds));

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(roleNames, future);
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
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.success(Collections.emptySet()));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();

        final RoleDescriptor roleA = new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null);
        final InMemoryRolesProvider inMemoryProvider = new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(roleA);
            }
            return RoleRetrievalResult.success(descriptors);
        });

        UpdatableLicenseState xPackLicenseState = new UpdatableLicenseState(SECURITY_ENABLED_SETTINGS);
        // these licenses don't allow custom role providers
        xPackLicenseState.update(randomFrom(OperationMode.BASIC, OperationMode.GOLD, OperationMode.STANDARD), true,
            Long.MAX_VALUE, null);
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore, mock(NativePrivilegeStore.class),
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState, cache,
            mock(ApiKeyService.class), mock(ServiceAccountService.class),
            documentSubsetBitsetCache, rds -> effectiveRoleDescriptors.set(rds));

        Set<String> roleNames = Sets.newHashSet("roleA");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        compositeRolesStore.roles(roleNames, future);
        Role role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
        effectiveRoleDescriptors.set(null);

        // no roles should've been populated, as the license doesn't permit custom role providers
        assertEquals(0, role.indices().groups().length);

        compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore, mock(NativePrivilegeStore.class),
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState, cache,
            mock(ApiKeyService.class), mock(ServiceAccountService.class),
            documentSubsetBitsetCache, rds -> effectiveRoleDescriptors.set(rds));
        // these licenses allow custom role providers
        xPackLicenseState.update(randomFrom(OperationMode.PLATINUM, OperationMode.ENTERPRISE, OperationMode.TRIAL), true,
            Long.MAX_VALUE, null);
        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        compositeRolesStore.roles(roleNames, future);
        role = future.actionGet();
        assertThat(effectiveRoleDescriptors.get(), containsInAnyOrder(roleA));
        effectiveRoleDescriptors.set(null);

        // roleA should've been populated by the custom role provider, because the license allows it
        assertEquals(1, role.indices().groups().length);

        // license expired, don't allow custom role providers
        compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore, mock(NativePrivilegeStore.class),
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState, cache,
            mock(ApiKeyService.class), mock(ServiceAccountService.class),
            documentSubsetBitsetCache, rds -> effectiveRoleDescriptors.set(rds));
        xPackLicenseState.update(randomFrom(OperationMode.PLATINUM, OperationMode.ENTERPRISE, OperationMode.TRIAL), false,
            Long.MAX_VALUE, null);
        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        compositeRolesStore.roles(roleNames, future);
        role = future.actionGet();
        assertEquals(0, role.indices().groups().length);
        assertThat(effectiveRoleDescriptors.get().isEmpty(), is(true));
    }

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return dummyIndexState(true, indexStatus);
    }

    public SecurityIndexManager.State dummyIndexState(boolean isIndexUpToDate, ClusterHealthStatus healthStatus) {
        return new SecurityIndexManager.State(
            Instant.now(), isIndexUpToDate, true, true, null, concreteSecurityIndexName, healthStatus, IndexMetadata.State.OPEN, null,
            "my_uuid");
    }

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        ReservedRolesStore reservedRolesStore = mock(ReservedRolesStore.class);
        doCallRealMethod().when(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
                Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(Settings.EMPTY),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, mock(ApiKeyService.class),
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> {}) {
            @Override
            public void invalidateAll() {
                numInvalidation.incrementAndGet();
            }
        };

        int expectedInvalidation = 0;
        // existing to no longer present
        SecurityIndexManager.State previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        SecurityIndexManager.State currentState = dummyState(null);
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // doesn't exist to exists
        previousState = dummyState(null);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green or yellow to red
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(ClusterHealthStatus.RED);
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());

        // red to non red
        previousState = dummyState(ClusterHealthStatus.RED);
        currentState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(++expectedInvalidation, numInvalidation.get());

        // green to yellow or yellow to green
        previousState = dummyState(randomFrom(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW));
        currentState = dummyState(previousState.indexHealth == ClusterHealthStatus.GREEN ?
                                  ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN);
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());
    }

    public void testCacheClearOnIndexOutOfDateChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        ReservedRolesStore reservedRolesStore = mock(ReservedRolesStore.class);
        doCallRealMethod().when(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(SECURITY_ENABLED_SETTINGS,
                fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, mock(ApiKeyService.class),
            mock(ServiceAccountService.class), documentSubsetBitsetCache, rds -> {}) {
            @Override
            public void invalidateAll() {
                numInvalidation.incrementAndGet();
            }
        };

        compositeRolesStore.onSecurityIndexStateChange(dummyIndexState(false, null), dummyIndexState(true, null));
        assertEquals(1, numInvalidation.get());

        compositeRolesStore.onSecurityIndexStateChange(dummyIndexState(true, null), dummyIndexState(false, null));
        assertEquals(2, numInvalidation.get());
    }

    public void testDefaultRoleUserWithoutRoles() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore,
            nativeRolesStore, reservedRolesStore, mock(NativePrivilegeStore.class), null, mock(ApiKeyService.class),
            mock(ServiceAccountService.class), null, null);
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        final User user = new User("no role user");
        Authentication auth = new Authentication(user, new RealmRef("name", "type", "node"), null);
        compositeRolesStore.getRoles(user, auth, rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertEquals(Role.EMPTY, roles);
    }

    public void testAnonymousUserEnabledRoleAdded() {
        Settings settings = Settings.builder()
            .put(SECURITY_ENABLED_SETTINGS)
            .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous_user_role")
            .build();
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            Set<String> names = (Set<String>) invocationOnMock.getArguments()[0];
            if (names.size() == 1 && names.contains("anonymous_user_role")) {
                RoleDescriptor rd = new RoleDescriptor("anonymous_user_role", null, null, null);
                return Collections.singleton(rd);
            }
            return Collections.emptySet();
        }).
        when(fileRolesStore).roleDescriptors(anySetOf(String.class));
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final CompositeRolesStore compositeRolesStore = buildCompositeRolesStore(settings, fileRolesStore, nativeRolesStore,
            reservedRolesStore, mock(NativePrivilegeStore.class), null, mock(ApiKeyService.class),
            mock(ServiceAccountService.class), null, null);
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        final User user = new User("no role user");
        Authentication auth = new Authentication(user, new RealmRef("name", "type", "node"), null);
        compositeRolesStore.getRoles(user, auth, rolesFuture);
        final Role roles = rolesFuture.actionGet();
        assertThat(Arrays.asList(roles.names()), hasItem("anonymous_user_role"));
    }

    public void testDoesNotUseRolesStoreForXPacAndAsyncSearchUser() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, mock(ApiKeyService.class),
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> effectiveRoleDescriptors.set(rds));
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor

        // test Xpack user short circuits to its own reserved role
        PlainActionFuture<Role> rolesFuture = new PlainActionFuture<>();
        Authentication auth = new Authentication(XPackUser.INSTANCE, new RealmRef("name", "type", "node"), null);
        compositeRolesStore.getRoles(XPackUser.INSTANCE, auth, rolesFuture);
        Role roles = rolesFuture.actionGet();
        assertThat(roles, equalTo(XPackUser.ROLE));
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verifyNoMoreInteractions(fileRolesStore, nativeRolesStore, reservedRolesStore);

        // test AyncSearch user short circuits to its own reserved role
        rolesFuture = new PlainActionFuture<>();
        auth = new Authentication(AsyncSearchUser.INSTANCE, new RealmRef("name", "type", "node"), null);
        compositeRolesStore.getRoles(AsyncSearchUser.INSTANCE, auth, rolesFuture);
        roles = rolesFuture.actionGet();
        assertThat(roles, equalTo(AsyncSearchUser.ROLE));
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verifyNoMoreInteractions(fileRolesStore, nativeRolesStore, reservedRolesStore);
    }

    public void testGetRolesForSystemUserThrowsException() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                mock(NativePrivilegeStore.class), Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, mock(ApiKeyService.class),
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> effectiveRoleDescriptors.set(rds));
        verify(fileRolesStore).addListener(anyConsumer()); // adds a listener in ctor
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> compositeRolesStore.getRoles(SystemUser.INSTANCE, null, null));
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        assertEquals("the user [_system] is the system user and we should never try to get its roles", iae.getMessage());
    }

    public void testApiKeyAuthUsesApiKeyService() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);
        ApiKeyService apiKeyService = spy(new ApiKeyService(SECURITY_ENABLED_SETTINGS, Clock.systemUTC(), mock(Client.class),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), mock(SecurityIndexManager.class), mock(ClusterService.class),
                mock(CacheInvalidatorRegistry.class), mock(ThreadPool.class)));
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener =
                (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[2];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollectionOf(String.class), anyCollectionOf(String.class), anyActionListener());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                nativePrivStore, Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, apiKeyService,
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> effectiveRoleDescriptors.set(rds));
        AuditUtil.getOrGenerateRequestId(threadContext);
        final Version version = randomFrom(Version.CURRENT, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_8_1));
        final Authentication authentication = createApiKeyAuthentication(apiKeyService, createAuthentication(),
            Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[]{"manage"}, null, null)),
            null,
            version);

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRoles(authentication.getUser(), authentication, roleFuture);
        Role role = roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));

        if (version == Version.CURRENT) {
            verify(apiKeyService, times(2)).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());
        } else {
            verify(apiKeyService).getRoleForApiKey(eq(authentication), anyActionListener());
        }
        assertThat(role.names().length, is(1));
        assertThat(role.names()[0], containsString("user_role_"));
    }

    public void testApiKeyAuthUsesApiKeyServiceWithScopedRole() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);

        ApiKeyService apiKeyService = spy(new ApiKeyService(SECURITY_ENABLED_SETTINGS, Clock.systemUTC(), mock(Client.class),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), mock(SecurityIndexManager.class), mock(ClusterService.class),
                mock(CacheInvalidatorRegistry.class), mock(ThreadPool.class)));
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener =
                (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[2];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollectionOf(String.class), anyCollectionOf(String.class), anyActionListener());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                nativePrivStore, Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0), cache, apiKeyService,
                mock(ServiceAccountService.class), documentSubsetBitsetCache,
                rds -> effectiveRoleDescriptors.set(rds));
        AuditUtil.getOrGenerateRequestId(threadContext);
        final Version version = randomFrom(Version.CURRENT, VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_8_1));
        final Authentication authentication = createApiKeyAuthentication(apiKeyService, createAuthentication(),
            Collections.singleton(new RoleDescriptor("user_role_" + randomAlphaOfLength(4), new String[]{"manage"}, null, null)),
            Collections.singletonList(new RoleDescriptor("key_role_" + randomAlphaOfLength(8), new String[]{"monitor"}, null, null)),
            version);

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRoles(authentication.getUser(), authentication, roleFuture);
        Role role = roleFuture.actionGet();
        assertThat(role.checkClusterAction("cluster:admin/foo", Empty.INSTANCE, mock(Authentication.class)), is(false));
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        if (version == Version.CURRENT) {
            verify(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), eq(false));
            verify(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), eq(true));
        } else {
            verify(apiKeyService).getRoleForApiKey(eq(authentication), anyActionListener());
        }
        assertThat(role.names().length, is(1));
        assertThat(role.names()[0], containsString("user_role_"));
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
            SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore, null, null,
            mock(ApiKeyService.class), mock(ServiceAccountService.class),
            documentSubsetBitsetCache, null);

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
            randomAlphaOfLengthBetween(4, 9), generateRandomStringArray(5, 5, false, true),
            null, null, null, null, metadata, null);

        RoleDescriptor deprecated1 = newRole.apply(MetadataUtils.getDeprecatedReservedMetadata("some reason"));
        RoleDescriptor deprecated2 = newRole.apply(MetadataUtils.getDeprecatedReservedMetadata("a different reason"));

        // Can't use getDeprecatedReservedMetadata because `Map.of` doesn't accept null values,
        // so we clone metadata with a real value and then remove that key
        final Map<String, Object> nullReasonMetadata = new HashMap<>(deprecated2.getMetadata());
        nullReasonMetadata.remove(MetadataUtils.DEPRECATED_REASON_METADATA_KEY);
        assertThat(nullReasonMetadata.keySet(), hasSize(deprecated2.getMetadata().size() -1));
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

        final CompositeRolesStore compositeRolesStore =
            buildCompositeRolesStore(SECURITY_ENABLED_SETTINGS, null, null, null, null, null,
                null, mock(ServiceAccountService.class), null, null);

        // Use a LHS so that the random-shufle-order of the list is preserved
        compositeRolesStore.logDeprecatedRoles(new LinkedHashSet<>(descriptors));

        assertWarnings(
            "The role [" + deprecated1.getName() + "] is deprecated and will be removed in a future version of Elasticsearch." +
                " some reason",
            "The role [" + deprecated2.getName() + "] is deprecated and will be removed in a future version of Elasticsearch." +
                " a different reason",
            "The role [" + deprecated3.getName() + "] is deprecated and will be removed in a future version of Elasticsearch." +
                " Please check the documentation"
        );
    }

    public void testCacheEntryIsReusedForIdenticalApiKeyRoles() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        doAnswer((invocationOnMock) -> {
            @SuppressWarnings("unchecked")
            ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
            callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(anySetOf(String.class), anyActionListener());
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());
        ThreadContext threadContext = new ThreadContext(SECURITY_ENABLED_SETTINGS);
        ApiKeyService apiKeyService = mock(ApiKeyService.class);
        NativePrivilegeStore nativePrivStore = mock(NativePrivilegeStore.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener =
                (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[2];
            listener.onResponse(Collections.emptyList());
            return Void.TYPE;
        }).when(nativePrivStore).getPrivileges(anyCollectionOf(String.class), anyCollectionOf(String.class), anyActionListener());

        final DocumentSubsetBitsetCache documentSubsetBitsetCache = buildBitsetCache();
        final AtomicReference<Collection<RoleDescriptor>> effectiveRoleDescriptors = new AtomicReference<Collection<RoleDescriptor>>();
        final CompositeRolesStore compositeRolesStore = new CompositeRolesStore(SECURITY_ENABLED_SETTINGS,
            fileRolesStore,
            nativeRolesStore,
            reservedRolesStore,
            nativePrivStore,
            Collections.emptyList(),
            new ThreadContext(SECURITY_ENABLED_SETTINGS),
            new XPackLicenseState(SECURITY_ENABLED_SETTINGS, () -> 0),
            cache,
            apiKeyService,
            mock(ServiceAccountService.class),
            documentSubsetBitsetCache,
            rds -> effectiveRoleDescriptors.set(rds));
        AuditUtil.getOrGenerateRequestId(threadContext);
        final BytesArray roleBytes = new BytesArray("{\"a role\": {\"cluster\": [\"all\"]}}");
        final BytesArray limitedByRoleBytes = new BytesArray("{\"limitedBy role\": {\"cluster\": [\"all\"]}}");
        Authentication authentication = new Authentication(new User("test api key user", "superuser"),
            new RealmRef("_es_api_key", "_es_api_key", "node"),
            null,
            Version.CURRENT,
            AuthenticationType.API_KEY,
            Map.of(API_KEY_ID_KEY,
                "key-id-1",
                API_KEY_ROLE_DESCRIPTORS_KEY,
                roleBytes,
                API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                limitedByRoleBytes));
        doCallRealMethod().when(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());

        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRoles(authentication.getUser(), authentication, roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService, times(2)).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());
        verify(apiKeyService).parseRoleDescriptors("key-id-1", roleBytes);
        verify(apiKeyService).parseRoleDescriptors("key-id-1", limitedByRoleBytes);

        // Different API key with the same roles should read from cache
        authentication = new Authentication(new User("test api key user 2", "superuser"),
            new RealmRef("_es_api_key", "_es_api_key", "node"),
            null,
            Version.CURRENT,
            AuthenticationType.API_KEY,
            Map.of(API_KEY_ID_KEY,
                "key-id-2",
                API_KEY_ROLE_DESCRIPTORS_KEY,
                roleBytes,
                API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                limitedByRoleBytes));
        doCallRealMethod().when(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());
        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRoles(authentication.getUser(), authentication, roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService, times(2)).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());
        verify(apiKeyService, never()).parseRoleDescriptors(eq("key-id-2"), any(BytesReference.class));

        // Different API key with the same limitedBy role should read from cache, new role should be built
        final BytesArray anotherRoleBytes = new BytesArray("{\"b role\": {\"cluster\": [\"manage_security\"]}}");
        authentication = new Authentication(new User("test api key user 2", "superuser"),
            new RealmRef("_es_api_key", "_es_api_key", "node"),
            null,
            Version.CURRENT,
            AuthenticationType.API_KEY,
            Map.of(API_KEY_ID_KEY,
                "key-id-3",
                API_KEY_ROLE_DESCRIPTORS_KEY,
                anotherRoleBytes,
                API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
                limitedByRoleBytes));
        doCallRealMethod().when(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), anyBoolean());
        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.getRoles(authentication.getUser(), authentication, roleFuture);
        roleFuture.actionGet();
        assertThat(effectiveRoleDescriptors.get(), is(nullValue()));
        verify(apiKeyService).getApiKeyIdAndRoleBytes(eq(authentication), eq(false));
        verify(apiKeyService).parseRoleDescriptors("key-id-3", anotherRoleBytes);
    }

    private Authentication createAuthentication() {
        final RealmRef lookedUpBy;
        final User user;
        if (randomBoolean()) {
            user = new User("_username", randomBoolean() ? new String[]{"r1"} :
                    new String[]{ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()},
                    new User("authenticated_username", new String[]{"r2"}));
            lookedUpBy = new RealmRef("lookRealm", "up", "by");
        } else {
            user = new User("_username", randomBoolean() ? new String[]{"r1"} :
                    new String[]{ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()});
            lookedUpBy = null;
        }
        return new Authentication(user, new RealmRef("authRealm", "test", "foo"), lookedUpBy,
                Version.CURRENT, randomFrom(AuthenticationType.REALM, AuthenticationType.TOKEN, AuthenticationType.INTERNAL,
                AuthenticationType.ANONYMOUS), Collections.emptyMap());
    }

    private CompositeRolesStore buildCompositeRolesStore(Settings settings,
                                                         @Nullable FileRolesStore fileRolesStore,
                                                         @Nullable NativeRolesStore nativeRolesStore,
                                                         @Nullable ReservedRolesStore reservedRolesStore,
                                                         @Nullable NativePrivilegeStore privilegeStore,
                                                         @Nullable XPackLicenseState licenseState,
                                                         @Nullable ApiKeyService apiKeyService,
                                                         @Nullable ServiceAccountService serviceAccountService,
                                                         @Nullable DocumentSubsetBitsetCache documentSubsetBitsetCache,
                                                         @Nullable Consumer<Collection<RoleDescriptor>> roleConsumer) {
        if (fileRolesStore == null) {
            fileRolesStore = mock(FileRolesStore.class);
            doCallRealMethod().when(fileRolesStore).accept(anySetOf(String.class), anyActionListener());
            when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        }
        if (nativeRolesStore == null) {
            nativeRolesStore = mock(NativeRolesStore.class);
            doCallRealMethod().when(nativeRolesStore).accept(anySetOf(String.class), anyActionListener());
            doAnswer((invocationOnMock) -> {
                @SuppressWarnings("unchecked")
                ActionListener<RoleRetrievalResult> callback = (ActionListener<RoleRetrievalResult>) invocationOnMock.getArguments()[1];
                callback.onResponse(RoleRetrievalResult.failure(new RuntimeException("intentionally failed!")));
                return null;
            }).when(nativeRolesStore).getRoleDescriptors(isASet(), anyActionListener());
        }
        if (reservedRolesStore == null) {
            reservedRolesStore = mock(ReservedRolesStore.class);
            doCallRealMethod().when(reservedRolesStore).accept(anySetOf(String.class), anyActionListener());
        }
        if (privilegeStore == null) {
            privilegeStore = mock(NativePrivilegeStore.class);
            doAnswer((invocationOnMock) -> {
                @SuppressWarnings("unchecked")
                ActionListener<Collection<ApplicationPrivilegeDescriptor>> callback =
                    (ActionListener<Collection<ApplicationPrivilegeDescriptor>>) invocationOnMock.getArguments()[2];
                callback.onResponse(Collections.emptyList());
                return null;
            }).when(privilegeStore).getPrivileges(isASet(), isASet(), anyActionListener());
        }
        if (licenseState == null) {
            licenseState = new XPackLicenseState(settings, () -> 0);
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
            roleConsumer = rds -> { };
        }
        return new CompositeRolesStore(settings, fileRolesStore, nativeRolesStore, reservedRolesStore, privilegeStore,
            Collections.emptyList(), new ThreadContext(settings), licenseState, cache, apiKeyService,
            serviceAccountService, documentSubsetBitsetCache, roleConsumer);
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
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
                IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
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
}
