/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.mock.orig.Mockito.times;
import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
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

    public void testRolesWhenDlsFlsUnlicensed() {
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
        RoleDescriptor dlsRole = new RoleDescriptor("dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .query(QueryBuilders.matchAllQuery().buildAsBytes())
                        .build()
        }, null);
        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(QueryBuilders.matchAllQuery().buildAsBytes())
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
                mock(ReservedRolesStore.class), licenseState);

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

    public void testRolesWhenDlsFlsLicensed() {
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
        RoleDescriptor dlsRole = new RoleDescriptor("dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .query(QueryBuilders.matchAllQuery().buildAsBytes())
                        .build()
        }, null);
        RoleDescriptor flsDlsRole = new RoleDescriptor("fls_dls", null, new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("read")
                        .grantedFields("*")
                        .deniedFields("foo")
                        .query(QueryBuilders.matchAllQuery().buildAsBytes())
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
                mock(ReservedRolesStore.class), licenseState);

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
                new CompositeRolesStore(Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore, new XPackLicenseState());
        verify(fileRolesStore).addListener(any(Runnable.class)); // adds a listener in ctor

        final String roleName = randomAsciiOfLengthBetween(1, 10);
        PlainActionFuture<Role> future = new PlainActionFuture<>();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        compositeRolesStore.roles(Collections.singleton(roleName), fieldPermissionsCache, future);
        final Role role = future.actionGet();
        assertEquals(Role.EMPTY, role);
        verify(reservedRolesStore).roleDescriptors();
        verify(fileRolesStore).roleDescriptors(eq(Collections.singleton(roleName)));
        verify(nativeRolesStore).getRoleDescriptors(isA(String[].class), any(ActionListener.class));

        final int numberOfTimesToCall = scaledRandomIntBetween(0, 32);
        final boolean getSuperuserRole = randomBoolean() && roleName.equals(ReservedRolesStore.SUPERUSER_ROLE.name()) == false;
        final Set<String> names = getSuperuserRole ? Sets.newHashSet(roleName, ReservedRolesStore.SUPERUSER_ROLE.name()) :
                Collections.singleton(roleName);
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
    }
}
