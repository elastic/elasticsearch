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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LimitedRoleTests extends ESTestCase {

    public void testRoleConstructorWithLimitedRole() {
        Role fromRole = Role.builder("a-role").build();
        Role limitedByRole = Role.builder("limited-role").build();
        Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
        assertNotNull(role);

        NullPointerException npe = expectThrows(NullPointerException.class, () -> LimitedRole.createLimitedRole(fromRole, null));
        assertThat(npe.getMessage(), containsString("limited by role is required to create limited role"));
    }

    public void testCheckClusterAction() {
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton(ClusterPrivilegeName.MANAGE_SECURITY), Collections.emptyList())
                .build();
        assertThat(fromRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.ALL), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(limitedByRole.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(role.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.MONITOR), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(false));
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(false));
        }
    }

    public void testCheckIndicesAction() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1").build();
        assertThat(fromRole.checkIndicesAction(SearchAction.NAME), is(true));
        assertThat(fromRole.checkIndicesAction(CreateIndexAction.NAME), is(false));

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.ALL, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(limitedByRole.checkIndicesAction(CreateIndexAction.NAME), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.NONE, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(false));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(false));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
    }

    public void testAllowedIndicesMatcher() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1*").build();
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(false));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(false));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-*").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));
        }
    }
}
