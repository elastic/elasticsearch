/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test for the {@link NativeRolesStore#getRoleDescriptors} method.
 */
public class GeRoleDescriptorsTests extends NativeRealmIntegTestCase {

    private static Set<String> customRoles;

    @BeforeClass
    public static void init() throws Exception {
        new ReservedRolesStore();

        final int numOfRoles = randomIntBetween(5, 10);
        customRoles = new HashSet<>(numOfRoles);
        for (int i = 0; i < numOfRoles; i++) {
            customRoles.add("custom_role_" + randomAlphaOfLength(10) + "_" + i);
        }
    }

    @Before
    public void setup() throws IOException {
        final TestSecurityClient securityClient = new TestSecurityClient(getRestClient(), SECURITY_REQUEST_OPTIONS);
        for (String role : customRoles) {
            final RoleDescriptor descriptor = new RoleDescriptor(
                role,
                new String[0],
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("ALL")
                        .allowRestrictedIndices(randomBoolean())
                        .build() },
                new String[0]
            );
            securityClient.putRole(descriptor);
            logger.info("--> created role [{}]", role);
        }

        ensureGreen(SecuritySystemIndices.SECURITY_MAIN_ALIAS);
    }

    public void testGetCustomRoles() {
        for (NativeRolesStore rolesStore : internalCluster().getInstances(NativeRolesStore.class)) {
            PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
            rolesStore.getRoleDescriptors(customRoles, future);
            RoleRetrievalResult result = future.actionGet();
            assertThat(result, notNullValue());
            assertTrue(result.isSuccess());
            assertThat(result.getDescriptors().stream().map(RoleDescriptor::getName).toList(), containsInAnyOrder(customRoles.toArray()));
        }
    }

    public void testGetReservedRoles() {
        for (NativeRolesStore rolesStore : internalCluster().getInstances(NativeRolesStore.class)) {
            PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
            Set<String> reservedRoles = randomUnique(() -> randomFrom(ReservedRolesStore.names()), randomIntBetween(1, 5));
            AssertionError error = expectThrows(AssertionError.class, () -> rolesStore.getRoleDescriptors(reservedRoles, future));
            assertThat(error.getMessage(), containsString("native roles store should not be called with reserved role names"));
        }
    }

    public void testGetAllRoles() {
        for (NativeRolesStore rolesStore : internalCluster().getInstances(NativeRolesStore.class)) {
            PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
            rolesStore.getRoleDescriptors(randomBoolean() ? null : Set.of(), future);
            RoleRetrievalResult result = future.actionGet();
            assertThat(result, notNullValue());
            assertTrue(result.isSuccess());
            assertThat(result.getDescriptors().stream().map(RoleDescriptor::getName).toList(), containsInAnyOrder(customRoles.toArray()));
        }
    }
}
