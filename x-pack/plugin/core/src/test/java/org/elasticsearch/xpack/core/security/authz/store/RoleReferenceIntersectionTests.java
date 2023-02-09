/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.LimitedRole;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RoleReferenceIntersectionTests extends ESTestCase {

    public void testBuildRoleForSingleRoleReference() {
        final RoleReference roleReference = mock(RoleReference.class);
        final Role role = mock(Role.class);
        final RoleReferenceIntersection roleReferenceIntersection = new RoleReferenceIntersection(roleReference);
        final BiConsumer<RoleReference, ActionListener<Role>> singleRoleBuilder = (r, l) -> {
            assertThat(r, is(roleReference));
            l.onResponse(role);
        };

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        roleReferenceIntersection.buildRole(singleRoleBuilder, future);
        assertThat(future.actionGet(), is(role));
    }

    public void testBuildRoleForListOfRoleReferences() {
        final int size = randomIntBetween(2, 3);
        final List<RoleReference> roleReferences = new ArrayList<>(size);
        final List<Role> roles = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            final RoleReference roleReference = mock(RoleReference.class);
            when(roleReference.id()).thenReturn(new RoleKey(Set.of(), String.valueOf(i)));
            roleReferences.add(roleReference);

            final Role role = mock(Role.class);
            when(role.limitedBy(any())).thenCallRealMethod();
            roles.add(role);
        }

        final RoleReferenceIntersection roleReferenceIntersection = new RoleReferenceIntersection(roleReferences);
        final BiConsumer<RoleReference, ActionListener<Role>> singleRoleBuilder = (rf, l) -> {
            l.onResponse(roles.get(Integer.parseInt(rf.id().getSource())));
        };

        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        roleReferenceIntersection.buildRole(singleRoleBuilder, future);

        final Role role = future.actionGet();
        assertThat(role, instanceOf(LimitedRole.class));

        verify(roles.get(0)).limitedBy(roles.get(1));

        if (size == 2) {
            assertThat(role, equalTo(new LimitedRole(roles.get(0), roles.get(1))));
        } else {
            assertThat(role, equalTo(new LimitedRole(new LimitedRole(roles.get(0), roles.get(1)), roles.get(2))));
        }
    }
}
