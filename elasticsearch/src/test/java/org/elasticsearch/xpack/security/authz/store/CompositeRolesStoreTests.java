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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.permission.Role;

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
                new CompositeRolesStore(Settings.EMPTY, fileRolesStore, nativeRolesStore, reservedRolesStore);
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
        final boolean getSuperuserRole = randomBoolean();
        final Set<String> names = getSuperuserRole ? Sets.newHashSet(roleName, ReservedRolesStore.SUPERUSER_ROLE.name()) :
                Collections.singleton(roleName);
        for (int i = 0; i < numberOfTimesToCall; i++) {
            future = new PlainActionFuture<>();
            compositeRolesStore.roles(names, fieldPermissionsCache, future);
            future.actionGet();
        }

        if (getSuperuserRole) {
            // the superuser role was requested so we get the role descriptors again
            verify(reservedRolesStore, times(2)).roleDescriptors();
        }
        verifyNoMoreInteractions(fileRolesStore, reservedRolesStore, nativeRolesStore);
    }
}
