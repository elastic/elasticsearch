/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ScopedRoleTests extends ESTestCase {

    public void testRoleConstructorWithScopedRole() {
        Role fromRole = Role.builder("a-role").build();
        Role scopedByRole = Role.builder("scoped-role").build();
        Role role = ScopedRole.createScopedRole(fromRole, scopedByRole);
        assertNotNull(role);
    }

    public void testCheckClusterAction() {
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton(ClusterPrivilegeName.MANAGE_SECURITY), Collections.emptyList())
                .build();
        assertThat(fromRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
        {
            Role scopedByRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.ALL), Collections.emptyList()).build();
            assertThat(scopedByRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(scopedByRole.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(true));
            Role role = ScopedRole.createScopedRole(fromRole, scopedByRole);
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(role.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(false));
        }
        {
            Role scopedRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.MONITOR), Collections.emptyList()).build();
            assertThat(scopedRole.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(true));
            Role role = ScopedRole.createScopedRole(fromRole, scopedRole);
            assertThat(role.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(false));
        }
    }

    public void testCheckIndicesAction() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1").build();
        assertThat(fromRole.checkIndicesAction(SearchAction.NAME), is(true));
        assertThat(fromRole.checkIndicesAction(CreateIndexAction.NAME), is(false));

        {
            Role scopedByRole = Role.builder("scoped-role").add(IndexPrivilege.ALL, "ind-1").build();
            assertThat(scopedByRole.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(scopedByRole.checkIndicesAction(CreateIndexAction.NAME), is(true));
            Role role = ScopedRole.createScopedRole(fromRole, scopedByRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
        {
            Role scopedRole = Role.builder("scoped-role").add(IndexPrivilege.NONE, "ind-1").build();
            assertThat(scopedRole.checkIndicesAction(SearchAction.NAME), is(false));
            Role role = ScopedRole.createScopedRole(fromRole, scopedRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(false));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
    }
}
