/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;


/**
 * Test for the clear roles API that changes the polling aspect of security to only run once an hour in order to
 * test the cache clearing APIs.
 */
public class ClearRolesCacheTests extends NativeRealmIntegTestCase {

    private static String[] roles;

    @BeforeClass
    public static void init() throws Exception {
        roles = new String[randomIntBetween(5, 10)];
        for (int i = 0; i < roles.length; i++) {
            roles[i] = randomAsciiOfLength(6) + "_" + i;
        }
    }

    @Before
    public void setupForTests() {
        SecurityClient c = securityClient();
        // create roles
        for (String role : roles) {
            c.preparePutRole(role)
                    .cluster("none")
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null)
                    .get();
            logger.debug("--> created role [{}]", role);
        }

        ensureGreen(SecurityTemplateService.SECURITY_INDEX_NAME);

        // warm up the caches on every node
        for (NativeRolesStore rolesStore : internalCluster().getInstances(NativeRolesStore.class)) {
            for (String role : roles) {
                assertThat(rolesStore.role(role), notNullValue());
            }
        }
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        TimeValue pollerInterval = TimeValue.timeValueMillis((long) randomIntBetween(2, 2000));
        logger.debug("using poller interval [{}]", pollerInterval);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NativeRolesStore.POLL_INTERVAL_SETTING.getKey(), pollerInterval.getStringRep())
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
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
                    .addIndices(new String[] { "*" }, new String[] { "ALL" }, null, null)
                    .runAs(role)
                    .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
                    .get();
            assertThat(response.isCreated(), is(false));
            logger.debug("--> updated role [{}] with run_as", role);
        }

        assertRolesAreCorrect(securityClient, toModify);
    }

    public void testModifyingDocumentsDirectly() throws Exception {
        int modifiedRolesCount = randomIntBetween(1, roles.length);
        List<String> toModify = randomSubsetOf(modifiedRolesCount, roles);
        logger.debug("--> modifying roles {} to have run_as", toModify);
        final boolean refresh = randomBoolean();
        for (String role : toModify) {
            UpdateResponse response = internalClient().prepareUpdate().setId(role).setIndex(SecurityTemplateService.SECURITY_INDEX_NAME)
                    .setType(NativeRolesStore.ROLE_DOC_TYPE)
                    .setDoc("run_as", new String[] { role })
                    .setRefreshPolicy(refresh ? IMMEDIATE : NONE)
                    .get();
            assertEquals(DocWriteResponse.Operation.INDEX, response.getOperation());
            logger.debug("--> updated role [{}] with run_as", role);
        }

        // in this test, the poller runs too frequently to check the cache still has roles without run as
        // clear the cache and we should definitely see the latest values!
        SecurityClient securityClient = securityClient(internalCluster().transportClient());
        final boolean useHttp = randomBoolean();
        final boolean clearAll = randomBoolean();
        logger.debug("--> starting to clear roles. using http [{}] clearing all [{}]", useHttp, clearAll);
        String[] rolesToClear = clearAll ? (randomBoolean() ? roles : null) : toModify.toArray(new String[toModify.size()]);
        if (useHttp) {
            String path;
            if (rolesToClear == null) {
                path = "/_xpack/security/role/" + (randomBoolean() ? "*" : "_all") + "/_clear_cache";
            } else {
                path = "/_xpack/security/role/" + Strings.arrayToCommaDelimitedString(rolesToClear) + "/_clear_cache";
            }
            Response response = getRestClient().performRequest("POST", path,
                    new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.DEFAULT_USER_NAME,
                                    new SecuredString(SecuritySettingsSource.DEFAULT_PASSWORD.toCharArray()))));
            assertThat(response.getStatusLine().getStatusCode(), is(RestStatus.OK.getStatus()));
        } else {
            securityClient.prepareClearRolesCache().names(rolesToClear).get();
        }

        assertRolesAreCorrect(securityClient, toModify);
    }

    public void testDeletingRoleDocumentDirectly() throws Exception {
        SecurityClient securityClient = securityClient(internalCluster().transportClient());

        final String role = randomFrom(roles);
        RoleDescriptor[] foundRoles = securityClient.prepareGetRoles().names(role).get().roles();
        assertThat(foundRoles.length, is(1));
        logger.debug("--> deleting role [{}]", role);
        final boolean refresh = randomBoolean();
        DeleteResponse response = internalClient()
                .prepareDelete(SecurityTemplateService.SECURITY_INDEX_NAME, NativeRolesStore.ROLE_DOC_TYPE, role)
                .setRefreshPolicy(refresh ? IMMEDIATE : NONE)
                .get();
        assertEquals(DocWriteResponse.Operation.DELETE, response.getOperation());

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(securityClient.prepareGetRoles().names(role).get().roles(), arrayWithSize(0));
            }
        });
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
