/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class UserManagedServiceAccountPrivilegeTests extends ESTestCase {

    private static final List<String> USER_MANAGED_ACTIONS = List.of(
        PutUserManagedServiceAccountAction.NAME,
        DeleteUserManagedServiceAccountAction.NAME,
        CreateUserManagedServiceAccountTokenAction.NAME,
        DeleteUserManagedServiceAccountTokenAction.NAME
    );

    public void testManageSecurityGrantsEveryUserManagedAction() {
        for (String action : USER_MANAGED_ACTIONS) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.MANAGE_SECURITY, action), is(true));
        }
    }

    public void testManageServiceAccountGrantsNoUserManagedAction() {
        for (String action : USER_MANAGED_ACTIONS) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT, action), is(false));
        }
    }

    public void testManageServiceAccountStillGrantsTheBuiltInActions() {
        for (String action : List.of(
            CreateServiceAccountTokenAction.NAME,
            DeleteServiceAccountTokenAction.NAME,
            GetServiceAccountAction.NAME
        )) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT, action), is(true));
        }
    }

    public void testReadSecurityReadsAccountsButWritesNone() {
        assertThat(checkPrivilege(ClusterPrivilegeResolver.READ_SECURITY, GetServiceAccountAction.NAME), is(true));
        for (String action : USER_MANAGED_ACTIONS) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.READ_SECURITY, action), is(false));
        }
    }

    public void testManageGrantsNoUserManagedAction() {
        for (String action : USER_MANAGED_ACTIONS) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.MANAGE, action), is(false));
        }
    }

    public void testAllGrantsEveryUserManagedAction() {
        for (String action : USER_MANAGED_ACTIONS) {
            assertThat(action, checkPrivilege(ClusterPrivilegeResolver.ALL, action), is(true));
        }
    }

    private static boolean checkPrivilege(NamedClusterPrivilege privilege, String action) {
        return privilege.permission().check(action, mock(TransportRequest.class), AuthenticationTestHelper.builder().build());
    }
}
