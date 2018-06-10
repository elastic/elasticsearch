/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.role;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.INDEX;
import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.ROLE_A;
import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.ROLE_B;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for custom roles providers.
 */
public class CustomRolesProviderIT extends ESIntegTestCase {

    private static final String TEST_USER = "test_user";
    private static final String TEST_PWD = "change_me";

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                    .put(ThreadContext.PREFIX + "." + CustomRealm.USER_HEADER, CustomRealm.KNOWN_USER)
                    .put(ThreadContext.PREFIX + "." + CustomRealm.PW_HEADER, CustomRealm.KNOWN_PW.toString())
                    .put(NetworkModule.TRANSPORT_TYPE_KEY, "security4")
                    .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(XPackClientPlugin.class);
    }

    public void setupTestUser(String role) {
        SecurityClient securityClient = new SecurityClient(client());
        securityClient.preparePutUser(TEST_USER, TEST_PWD.toCharArray(), "bcrypt", role).get();
    }

    public void testAuthorizedCustomRoleSucceeds() throws Exception {
        setupTestUser(ROLE_B);
        // roleB has all permissions on index "foo", so creating "foo" should succeed
        Response response = getRestClient().performRequest("PUT", "/" + INDEX, authHeader());
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testFirstResolvedRoleTakesPrecedence() throws Exception {
        // the first custom roles provider has set ROLE_A to only have read permission on the index,
        // the second custom roles provider has set ROLE_A to have all permissions, but since
        // the first custom role provider appears first in order, it should take precedence and deny
        // permission to create the index
        setupTestUser(ROLE_A);
        // roleB has all permissions on index "foo", so creating "foo" should succeed
        try {
            getRestClient().performRequest("PUT", "/" + INDEX, authHeader());
            fail(ROLE_A + " should not be authorized to create index " + INDEX);
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testUnresolvedRoleDoesntSucceed() throws Exception {
        setupTestUser("unknown");
        // roleB has all permissions on index "foo", so creating "foo" should succeed
        try {
            getRestClient().performRequest("PUT", "/" + INDEX, authHeader());
            fail(ROLE_A + " should not be authorized to create index " + INDEX);
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    private BasicHeader authHeader() {
        return new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                               basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PWD.toCharArray())));
    }
}
