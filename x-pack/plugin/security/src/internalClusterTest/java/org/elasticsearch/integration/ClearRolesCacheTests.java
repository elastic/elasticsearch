/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteRoleResponse;
import org.elasticsearch.client.security.GetRolesRequest;
import org.elasticsearch.client.security.GetRolesResponse;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutRoleResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.hamcrest.Matchers.is;
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
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        // create roles
        for (String role : roles) {
            restClient.security().putRole(new PutRoleRequest(
                Role.builder().name(role)
                    .clusterPrivileges("none")
                    .indicesPrivileges(
                        IndicesPrivileges.builder().indices("*").privileges("ALL").allowRestrictedIndices(randomBoolean()).build())
                    .build(), RefreshPolicy.IMMEDIATE),
                SECURITY_REQUEST_OPTIONS);
            logger.debug("--> created role [{}]", role);
        }

        ensureGreen(RestrictedIndicesNames.SECURITY_MAIN_ALIAS);

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
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        for (String role : toModify) {
            PutRoleResponse response = restClient.security().putRole(new PutRoleRequest(Role.builder().name(role)
                    .clusterPrivileges("none")
                    .indicesPrivileges(
                        IndicesPrivileges.builder().indices("*").privileges("ALL").allowRestrictedIndices(randomBoolean()).build())
                    .runAsPrivilege(role)
                    .build(), randomBoolean() ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE), SECURITY_REQUEST_OPTIONS);
            assertThat(response.isCreated(), is(false));
            logger.debug("--> updated role [{}] with run_as", role);
        }

        assertRolesAreCorrect(restClient, toModify);
    }

    public void testDeletingViaApiClearsCache() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final int rolesToDelete = randomIntBetween(1, roles.length - 1);
        List<String> toDelete = randomSubsetOf(rolesToDelete, roles);
        for (String role : toDelete) {
            DeleteRoleResponse response = restClient.security()
                .deleteRole(new DeleteRoleRequest(role, RefreshPolicy.IMMEDIATE), SECURITY_REQUEST_OPTIONS);
            assertTrue(response.isFound());
        }

        GetRolesResponse roleResponse = restClient.security().getRoles(new GetRolesRequest(roles), SECURITY_REQUEST_OPTIONS);
        assertFalse(roleResponse.getRoles().isEmpty());
        assertThat(roleResponse.getRoles().size(), is(roles.length - rolesToDelete));
    }

    private void assertRolesAreCorrect(RestHighLevelClient restClient, List<String> toModify) throws IOException {
        for (String role : roles) {
            logger.debug("--> getting role [{}]", role);
            GetRolesResponse roleResponse = restClient.security().getRoles(new GetRolesRequest(role), SECURITY_REQUEST_OPTIONS);
            assertThat(roleResponse.getRoles().isEmpty(), is(false));
            final Set<String> runAs = roleResponse.getRoles().get(0).getRunAsPrivilege();
            if (toModify.contains(role)) {
                assertThat("role [" + role + "] should be modified and have run as", runAs == null || runAs.size() == 0, is(false));
                assertThat(runAs.contains(role), is(true));
            } else {
                assertThat("role [" + role + "] should be cached and not have run as set but does!", runAs == null || runAs.size() == 0,
                        is(true));
            }
        }
    }
}
