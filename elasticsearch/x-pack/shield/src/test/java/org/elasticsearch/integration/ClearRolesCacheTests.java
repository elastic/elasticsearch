/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.action.role.AddRoleResponse;
import org.elasticsearch.shield.action.role.GetRolesResponse;
import org.elasticsearch.shield.ShieldTemplateService;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.shield.client.ShieldClient;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test for the Shield clear roles API that changes the polling aspect of shield to only run once an hour in order to
 * test the cache clearing APIs.
 */
@TestLogging("shield.authc.esnative:TRACE,shield.authz.esnative:TRACE,integration:DEBUG")
public class ClearRolesCacheTests extends ShieldIntegTestCase {

    private static String[] roles;

    @BeforeClass
    public static void init() throws Exception {
        roles = new String[randomIntBetween(5, 10)];
        for (int i = 0; i < roles.length; i++) {
            roles[i] = randomAsciiOfLength(6) + "_" + i;
        }
    }

    @Before
    public void setupForTest() throws Exception {
        // Clear the realm cache for all realms since we use a SUITE scoped cluster
        ShieldClient client = new ShieldClient(internalCluster().transportClient());
        client.prepareClearRealmCache().get();

        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeUsersStore.State.STARTED));
                }
            });
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeRolesStore.State.STARTED));
                }
            });
        }

        ShieldClient c = new ShieldClient(client());
        // create roles
        for (String role : roles) {
            c.prepareAddRole().name(role)
                    .cluster("none")
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null)
                    .get();
            logger.debug("--> created role [{}]", role);
        }

        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);

        // warm up the caches on every node
        for (ESNativeRolesStore rolesStore : internalCluster().getInstances(ESNativeRolesStore.class)) {
            for (String role : roles) {
                assertThat(rolesStore.role(role), notNullValue());
            }
        }
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("shield.authc.native.reload.interval", TimeValue.timeValueSeconds(2L))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    public void testModifyingViaApiClearsCache() throws Exception {
        Client client = internalCluster().transportClient();
        ShieldClient shieldClient = new ShieldClient(client);

        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        for (String role : toModify) {
            AddRoleResponse response = shieldClient.prepareAddRole().name(role)
                    .cluster("none")
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null)
                    .runAs(role)
                    .get();
            assertThat(response.isCreated(), is(false));
            logger.debug("--> updated role [{}] with run_as", role);
        }

        assertRolesAreCorrect(shieldClient, toModify);
    }

    public void testModifyingDocumentsDirectly() throws Exception {
        Client client = internalCluster().transportClient();

        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        for (String role : toModify) {
            UpdateResponse response = client.prepareUpdate().setId(role).setIndex(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME)
                    .setType(ESNativeRolesStore.INDEX_ROLE_TYPE)
                    .setDoc("run_as", new String[] { role })
                    .get();
            assertThat(response.isCreated(), is(false));
            logger.debug("--> updated role [{}] with run_as", role);
        }

        // in this test, the poller runs too frequently to check the cache still has roles without run as
        // clear the cache and we should definitely see the latest values!
        ShieldClient shieldClient = new ShieldClient(client);
        final boolean useHttp = randomBoolean();
        final boolean clearAll = randomBoolean();
        logger.debug("--> starting to clear roles. using http [{}] clearing all [{}]", useHttp, clearAll);
        String[] rolesToClear = clearAll ? (randomBoolean() ? roles : null) : toModify.toArray(Strings.EMPTY_ARRAY);
        if (useHttp) {
            String path;
            if (rolesToClear == null) {
                path = "/_shield/role/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache";
            } else {
                path = "/_shield/role/" + Strings.arrayToCommaDelimitedString(rolesToClear) + "/_clear_cache";
            }
            HttpResponse response = httpClient().path(path).method("POST")
                    .addHeader("Authorization",
                            UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                                    new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray())))
                    .execute();
            assertThat(response.getStatusCode(), is(RestStatus.OK.getStatus()));
        } else {
            shieldClient.prepareClearRolesCache().roles(rolesToClear).get();
        }

        assertRolesAreCorrect(shieldClient, toModify);
    }

    public void testDeletingRoleDocumentDirectly() throws Exception {
        Client client = internalCluster().transportClient();
        ShieldClient shieldClient = new ShieldClient(client);

        final String role = randomFrom(roles);
        List<RoleDescriptor> foundRoles = shieldClient.prepareGetRoles().roles(role).get().roles();
        assertThat(foundRoles.size(), is(1));
        logger.debug("--> deleting role [{}]", role);
        DeleteResponse response = client.prepareDelete(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME,
                ESNativeRolesStore.INDEX_ROLE_TYPE, role).get();
        assertThat(response.isFound(), is(true));

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(shieldClient.prepareGetRoles().roles(role).get().roles().isEmpty(), is(true));
            }
        });
    }

    private void assertRolesAreCorrect(ShieldClient shieldClient, List<String> toModify) {
        for (String role : roles) {
            logger.debug("--> getting role [{}]", role);
            GetRolesResponse roleResponse = shieldClient.prepareGetRoles().roles(role).get();
            assertThat(roleResponse.isExists(), is(true));
            final String[] runAs = roleResponse.roles().get(0).getRunAs();
            if (toModify.contains(role)) {
                assertThat("role [" + role + "] should be modified and have run as", runAs == null || runAs.length == 0, is(false));
                assertThat(Arrays.asList(runAs).contains(role), is(true));
            } else {
                assertThat("role [" + role + "] should be cached and not have run as set but does!", runAs == null || runAs.length == 0,
                        is(true));
            }
        }
    }

    @After
    public void stopESNativeStores() throws Exception {
        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeUsersStore.State.STOPPED, ESNativeUsersStore.State.FAILED));
                }
            });
            store.reset();
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeRolesStore.State.STOPPED, ESNativeRolesStore.State.FAILED));
                }
            });
            store.reset();
        }
    }
}
