/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.TestUtils.UpdatableLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeRolesStoreTests extends ESTestCase {

    private static final Settings SECURITY_ENABLED_SETTINGS = Settings.builder()
            .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
            .build();

    public void testRolesWhenDlsFlsUnlicensed() throws IOException {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(false);
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
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(Settings.EMPTY, fileRolesStore, mock(NativeRolesStore.class),
                mock(ReservedRolesStore.class), Collections.emptyList(), new ThreadContext(Settings.EMPTY), licenseState);

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls"), fieldPermissionsCache, roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("dls"), fieldPermissionsCache, roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls_dls"), fieldPermissionsCache, roleFuture);
        assertEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("no_fls_dls"), fieldPermissionsCache, roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
    }

    public void testRolesWhenDlsFlsLicensed() throws IOException {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
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
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls"))).thenReturn(Collections.singleton(flsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("dls"))).thenReturn(Collections.singleton(dlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("fls_dls"))).thenReturn(Collections.singleton(flsDlsRole));
        when(fileRolesStore.roleDescriptors(Collections.singleton("no_fls_dls"))).thenReturn(Collections.singleton(noFlsDlsRole));
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(Settings.EMPTY, fileRolesStore, mock(NativeRolesStore.class),
                mock(ReservedRolesStore.class), Collections.emptyList(), new ThreadContext(Settings.EMPTY), licenseState);

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        PlainActionFuture<Role> roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls"), fieldPermissionsCache, roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("dls"), fieldPermissionsCache, roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("fls_dls"), fieldPermissionsCache, roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());

        roleFuture = new PlainActionFuture<>();
        compositeRolesStore.roles(Collections.singleton("no_fls_dls"), fieldPermissionsCache, roleFuture);
        assertNotEquals(Role.EMPTY, roleFuture.actionGet());
    }

    public void testNegativeLookupsAreCached() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doAnswer((invocationOnMock) -> {
            ActionListener<Set<RoleDescriptor>> callback = (ActionListener<Set<RoleDescriptor>>) invocationOnMock.getArguments()[1];
            callback.onResponse(Collections.emptySet());
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final CompositeRolesStore compositeRolesStore =
                new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                        Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                        new XPackLicenseState(SECURITY_ENABLED_SETTINGS));
        verify(fileRolesStore).addListener(any(Runnable.class)); // adds a listener in ctor

        final String roleName = randomAlphaOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(Collections.singleton(roleName), fieldPermissionsCache, future);
        final Role role = future.actionGet();
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).roleDescriptors();
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final boolean getSuperuserRole = randomBoolean()
                && roleName.equals(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()) == false;
        final Set<String> names = getSuperuserRole ? Sets.newHashSet(roleName, ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())
                : Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(names, fieldPermissionsCache, future);
            future.actionGet();
        }

        if (getSuperuserRole && numberOfTimesToCall > 0) {
            // the superuser role was requested so we get the role descriptors again
            verify(reservedRolesStore, times(2)).roleDescriptors();
        }
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);

        // force a cache clear

    }

    public void testCustomRolesProviders() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doAnswer((invocationOnMock) -> {
            ActionListener<Set<RoleDescriptor>> callback = (ActionListener<Set<RoleDescriptor>>) invocationOnMock.getArguments()[1];
            callback.onResponse(Collections.emptySet());
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));
        final ReservedRolesStore reservedRolesStore = spy(new ReservedRolesStore());

        final InMemoryRolesProvider inMemoryProvider1 = spy(new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null));
            }
            return descriptors;
        }));

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
                descriptors.add(new RoleDescriptor("roleB", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("bar").grantedFields("*").build()
                    }, null));
            }
            return descriptors;
        }));

        final CompositeRolesStore compositeRolesStore =
                new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                                Arrays.asList(inMemoryProvider1, inMemoryProvider2), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                                new XPackLicenseState(SECURITY_ENABLED_SETTINGS));

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(roleNames, fieldPermissionsCache, future);
        final Role role = future.actionGet();

        // make sure custom roles providers populate roles correctly
        assertEquals(2, role.indices().groups().length);
        assertEquals(IndexPrivilege.READ, role.indices().groups()[0].privilege());
        assertThat(role.indices().groups()[0].indices()[0], anyOf(equalTo("foo"), equalTo("bar")));
        assertEquals(IndexPrivilege.READ, role.indices().groups()[1].privilege());
        assertThat(role.indices().groups()[1].indices()[0], anyOf(equalTo("foo"), equalTo("bar")));

        // make sure negative lookups are cached
        verify(inMemoryProvider1).accept(anySetOf(String.class), any(ActionListener.class));
        verify(inMemoryProvider2).accept(anySetOf(String.class), any(ActionListener.class));

        final int numberOfTimesToCall = scaledRandomIntBetween(1, 8);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(Collections.singleton("unknown"), fieldPermissionsCache, future);
            future.actionGet();
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
        Role role = CompositeRolesStore.buildRoleFromDescriptors(Sets.newHashSet(flsRole, addsL1Fields), cache);

        MetaData metaData = MetaData.builder()
                .put(new IndexMetaData.Builder("test")
                        .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
                        .numberOfShards(1).numberOfReplicas(0).build(), true)
                .build();
        Map<String, IndicesAccessControl.IndexAccessControl> acls =
                role.indices().authorize("indices:data/read/search", Collections.singleton("test"), metaData, cache);
        assertFalse(acls.isEmpty());
        assertTrue(acls.get("test").getFieldPermissions().grantsAccessTo("L1.foo"));
        assertFalse(acls.get("test").getFieldPermissions().grantsAccessTo("L2.foo"));
        assertTrue(acls.get("test").getFieldPermissions().grantsAccessTo("L3.foo"));
    }

    public void testCustomRolesProviderFailures() throws Exception {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doAnswer((invocationOnMock) -> {
            ActionListener<Set<RoleDescriptor>> callback = (ActionListener<Set<RoleDescriptor>>) invocationOnMock.getArguments()[1];
            callback.onResponse(Collections.emptySet());
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();

        final InMemoryRolesProvider inMemoryProvider1 = new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null));
            }
            return descriptors;
        });

        final BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>> failingProvider =
            (roles, listener) -> listener.onFailure(new Exception("fake failure"));

        final CompositeRolesStore compositeRolesStore =
            new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, fileRolesStore, nativeRolesStore, reservedRolesStore,
                                    Arrays.asList(inMemoryProvider1, failingProvider), new ThreadContext(SECURITY_ENABLED_SETTINGS),
                                    new XPackLicenseState(SECURITY_ENABLED_SETTINGS));

        final Set<String> roleNames = Sets.newHashSet("roleA", "roleB", "unknown");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(roleNames, fieldPermissionsCache, future);
        try {
            future.get();
            fail("provider should have thrown a failure");
        } catch (ExecutionException e) {
            assertEquals("fake failure", e.getCause().getMessage());
        }
    }

    public void testCustomRolesProvidersLicensing() {
        final FileRolesStore fileRolesStore = mock(FileRolesStore.class);
        when(fileRolesStore.roleDescriptors(anySetOf(String.class))).thenReturn(Collections.emptySet());
        final NativeRolesStore nativeRolesStore = mock(NativeRolesStore.class);
        doAnswer((invocationOnMock) -> {
            ActionListener<Set<RoleDescriptor>> callback = (ActionListener<Set<RoleDescriptor>>) invocationOnMock.getArguments()[1];
            callback.onResponse(Collections.emptySet());
            return null;
        }).when(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();

        final InMemoryRolesProvider inMemoryProvider = new InMemoryRolesProvider((roles) -> {
            Set<RoleDescriptor> descriptors = new HashSet<>();
            if (roles.contains("roleA")) {
                descriptors.add(new RoleDescriptor("roleA", null,
                    new IndicesPrivileges[] {
                        IndicesPrivileges.builder().privileges("READ").indices("foo").grantedFields("*").build()
                    }, null));
            }
            return descriptors;
        });

        UpdatableLicenseState xPackLicenseState = new UpdatableLicenseState(SECURITY_ENABLED_SETTINGS);
        // these licenses don't allow custom role providers
        xPackLicenseState.update(randomFrom(OperationMode.BASIC, OperationMode.GOLD, OperationMode.STANDARD), true);
        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore,
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState);

        Set<String> roleNames = Sets.newHashSet("roleA");
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(roleNames, fieldPermissionsCache, future);
        Role role = future.actionGet();

        // no roles should've been populated, as the license doesn't permit custom role providers
        assertEquals(0, role.indices().groups().length);

        compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore,
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState);
        // these licenses allow custom role providers
        xPackLicenseState.update(randomFrom(OperationMode.PLATINUM, OperationMode.TRIAL), true);
        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(roleNames, fieldPermissionsCache, future);
        role = future.actionGet();

        // roleA should've been populated by the custom role provider, because the license allows it
        assertEquals(1, role.indices().groups().length);

        // license expired, don't allow custom role providers
        compositeRolesStore = new CompositeRolesStore(
            Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore,
            Arrays.asList(inMemoryProvider), new ThreadContext(Settings.EMPTY), xPackLicenseState);
        xPackLicenseState.update(randomFrom(OperationMode.PLATINUM, OperationMode.TRIAL), false);
        roleNames = Sets.newHashSet("roleA");
        future = new PlainActionFuture<>();
        fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(roleNames, fieldPermissionsCache, future);
        role = future.actionGet();
        assertEquals(0, role.indices().groups().length);
    }

    private SecurityIndexManager.State dummyState(ClusterHealthStatus indexStatus) {
        return new SecurityIndexManager.State(true, true, true, true, null, indexStatus);
    }

    public void testCacheClearOnIndexHealthChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(
                Settings.EMPTY, mock(FileRolesStore.class), mock(NativeRolesStore.class), mock(ReservedRolesStore.class),
                Collections.emptyList(), new ThreadContext(Settings.EMPTY), new XPackLicenseState(SECURITY_ENABLED_SETTINGS)) {
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
        currentState = dummyState(previousState.indexStatus == ClusterHealthStatus.GREEN ?
                                  ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN);
        compositeRolesStore.onSecurityIndexStateChange(previousState, currentState);
        assertEquals(expectedInvalidation, numInvalidation.get());
    }

    public void testCacheClearOnIndexOutOfDateChange() {
        final AtomicInteger numInvalidation = new AtomicInteger(0);

        CompositeRolesStore compositeRolesStore = new CompositeRolesStore(SECURITY_ENABLED_SETTINGS, mock(FileRolesStore.class),
                mock(NativeRolesStore.class), mock(ReservedRolesStore.class),
                Collections.emptyList(), new ThreadContext(SECURITY_ENABLED_SETTINGS), new XPackLicenseState(SECURITY_ENABLED_SETTINGS)) {
            @Override
            public void invalidateAll() {
                numInvalidation.incrementAndGet();
            }
        };

        compositeRolesStore.onSecurityIndexStateChange(
            new SecurityIndexManager.State(true, false, true, true, null, null),
            new SecurityIndexManager.State(true, true, true, true, null, null));
        assertEquals(1, numInvalidation.get());

        compositeRolesStore.onSecurityIndexStateChange(
            new SecurityIndexManager.State(true, true, true, true, null, null),
            new SecurityIndexManager.State(true, false, true, true, null, null));
        assertEquals(2, numInvalidation.get());
    }

    private static class InMemoryRolesProvider implements BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>> {
        private final Function<Set<String>, Set<RoleDescriptor>> roleDescriptorsFunc;

        InMemoryRolesProvider(Function<Set<String>, Set<RoleDescriptor>> roleDescriptorsFunc) {
            this.roleDescriptorsFunc = roleDescriptorsFunc;
        }

        @Override
        public void accept(Set<String> roles, ActionListener<Set<RoleDescriptor>> listener) {
            listener.onResponse(roleDescriptorsFunc.apply(roles));
        }
    }
}
