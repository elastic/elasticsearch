/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
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
    public void setupForTests() {
        SecurityClient c = securityClient();
        // create roles
        for (String role : roles) {
            c.preparePutRole(role)
                    .cluster("none")
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null, null)
                    .get();
            logger.debug("--> created role [{}]", role);
        }

        ensureGreen(SecurityIndexManager.SECURITY_INDEX_NAME);

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
        Client client = internalCluster().transportClient();
        SecurityClient securityClient = securityClient(client);

        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        for (String role : toModify) {
            PutRoleResponse response = securityClient.preparePutRole(role)
                    .cluster("none")
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null, null)
                    .runAs(role)
                    .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
                    .get();
            assertThat(response.isCreated(), is(false));
            logger.debug("--> updated role [{}] with run_as", role);
        }

        assertRolesAreCorrect(securityClient, toModify);
    }

    public void testDeletingViaApiClearsCache() throws Exception {
        final int rolesToDelete = randomIntBetween(1, roles.length - 1);
        List<String> toDelete = randomSubsetOf(rolesToDelete, roles);
        for (String role : toDelete) {
            DeleteRoleResponse response = securityClient().prepareDeleteRole(role).get();
            assertTrue(response.found());
        }

        GetRolesResponse roleResponse = securityClient().prepareGetRoles().names(roles).get();
        assertTrue(roleResponse.hasRoles());
        assertThat(roleResponse.roles().length, is(roles.length - rolesToDelete));
    }

    private void assertRolesAreCorrect(SecurityClient securityClient, List<String> toModify) {
        for (String role : roles) {
            logger.debug("--> getting role [{}]", role);
            GetRolesResponse roleResponse = securityClient.prepareGetRoles().names(role).get();
            assertThat(roleResponse.hasRoles(), is(true));
            final String[] runAs = roleResponse.roles()[0].getRunAs();
            if (toModify.contains(role)) {
                assertThat("role [" + role + "] should be modified and have run as", runAs == null || runAs.length == 0, is(false));
                assertThat(Arrays.asList(runAs).contains(role), is(true));
            } else {
                assertThat("role [" + role + "] should be cached and not have run as set but does!", runAs == null || runAs.length == 0,
                        is(true));
            }
        }
    }
}
