/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ManagedServiceAccountPrivilegeTests extends ESTestCase {

    public void testManagedActionsAreOutsideManageServiceAccountWildcard() {
        final var authentication = AuthenticationTestHelper.builder().build();
        final TransportRequest request = mock(TransportRequest.class);
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(CreateServiceAccountTokenAction.NAME, request, authentication),
            is(true)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(DeleteServiceAccountTokenAction.NAME, request, authentication),
            is(true)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(PutManagedServiceAccountAction.NAME, request, authentication),
            is(false)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(DeleteManagedServiceAccountAction.NAME, request, authentication),
            is(false)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(CreateManagedServiceAccountTokenAction.NAME, request, authentication),
            is(false)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SERVICE_ACCOUNT.permission()
                .check(DeleteManagedServiceAccountTokenAction.NAME, request, authentication),
            is(false)
        );
    }

    public void testManagedActionsAreGrantedByManageSecurity() {
        final var authentication = AuthenticationTestHelper.builder().build();
        final TransportRequest request = mock(TransportRequest.class);
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission().check(PutManagedServiceAccountAction.NAME, request, authentication),
            is(true)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission().check(DeleteManagedServiceAccountAction.NAME, request, authentication),
            is(true)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission()
                .check(CreateManagedServiceAccountTokenAction.NAME, request, authentication),
            is(true)
        );
        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission()
                .check(DeleteManagedServiceAccountTokenAction.NAME, request, authentication),
            is(true)
        );
    }
}
