/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test for the clear roles API
 */
public class ClearRolesCacheTests extends NativeRealmIntegTestCase {

    private static String[] roles;

    @BeforeClass
    public static void init() throws Exception {
        roles = new String[randomIntBetween(5, 10)];
        for (int i = 0; i < roles.length; i++) {
            roles[i] = randomAlphaOfLength(6) + "_" + i;
        }
    }

    @Before
    public void setupForTests() throws IOException {
        final TestSecurityClient securityClient = new TestSecurityClient(getRestClient(), SECURITY_REQUEST_OPTIONS);
        // create roles
        for (String role : roles) {
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
            logger.debug("--> created role [{}]", role);
        }

        ensureGreen(SecuritySystemIndices.SECURITY_MAIN_ALIAS);

        final Set<String> rolesSet = new HashSet<>(Arrays.asList(roles));
        // warm up the caches on every node
        for (NativeRolesStore rolesStore : internalCluster().getInstances(NativeRolesStore.class)) {
            PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
            rolesStore.getRoleDescriptors(rolesSet, future);
            assertThat(future.actionGet(), notNullValue());
            assertTrue(future.actionGet().isSuccess());
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testModifyingViaApiClearsCache() throws Exception {
        final TestSecurityClient securityClient = new TestSecurityClient(getRestClient(), SECURITY_REQUEST_OPTIONS);
        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        for (String roleName : toModify) {
            RoleDescriptor descriptor = new RoleDescriptor(
                roleName,
                new String[0],
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder()
                        .indices("*")
                        .privileges("ALL")
                        .allowRestrictedIndices(randomBoolean())
                        .build() },
                new String[] { roleName }
            );
            final DocWriteResponse.Result result = securityClient.putRole(descriptor);
            assertThat(result, is(DocWriteResponse.Result.UPDATED));
            logger.debug("--> updated role [{}] with run_as", roleName);
        }

        assertRolesAreCorrect(securityClient, toModify);
    }

    public void testDeletingViaApiClearsCache() throws Exception {
        final TestSecurityClient securityClient = new TestSecurityClient(getRestClient(), SECURITY_REQUEST_OPTIONS);
        final int rolesToDelete = randomIntBetween(1, roles.length - 1);
        List<String> toDelete = randomSubsetOf(rolesToDelete, roles);
        for (String role : toDelete) {
            assertTrue(securityClient.deleteRole(role));
        }

        final Map<String, RoleDescriptor> roles = securityClient.getRoleDescriptors(ClearRolesCacheTests.roles);
        assertThat(roles, aMapWithSize(ClearRolesCacheTests.roles.length - rolesToDelete));
    }

    private void assertRolesAreCorrect(TestSecurityClient client, List<String> toModify) throws IOException {
        for (String roleName : roles) {
            logger.debug("--> getting role [{}]", roleName);
            RoleDescriptor role = client.getRoleDescriptor(roleName);
            assertThat(role, notNullValue());
            final String[] runAs = role.getRunAs();
            if (toModify.contains(roleName)) {
                assertThat("role [" + roleName + "] should be modified and have run as", runAs, not(emptyArray()));
                assertThat(runAs, hasItemInArray(roleName));
            } else {
                assertThat("role [" + role + "] should be cached and not have run as set but does!", runAs, emptyArray());
            }
        }
    }
}
