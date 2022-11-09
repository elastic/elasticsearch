/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.role;

import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.example.realm.CustomRealmIT;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.INDEX;
import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.ROLE_A;
import static org.elasticsearch.example.role.CustomInMemoryRolesProvider.ROLE_B;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for custom roles providers.
 */
public class CustomRolesProviderIT extends ESRestTestCase {
    private static final String TEST_USER = "test_user";
    private static final String TEST_PWD = "test-user-password";

    private static final RequestOptions AUTH_OPTIONS;
    static {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader(
            UsernamePasswordToken.BASIC_AUTH_HEADER,
            UsernamePasswordToken.basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PWD.toCharArray()))
        );
        AUTH_OPTIONS = options.build();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + "." + CustomRealm.USER_HEADER, CustomRealmIT.USERNAME)
            .put(ThreadContext.PREFIX + "." + CustomRealm.PW_HEADER, CustomRealmIT.PASSWORD)
            .build();
    }

    public void setupTestUser(String role) throws IOException {
        final String endpoint = "/_security/user/" + TEST_USER;
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        final String body = formatted("""
            {
                "username": "%s",
                "password": "%s",
                "roles": [ "%s" ]
            }
            """, TEST_USER, TEST_PWD, role);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));
        request.setOptions(RequestOptions.DEFAULT);
        adminClient().performRequest(request);
    }

    public void testAuthorizedCustomRoleSucceeds() throws Exception {
        setupTestUser(ROLE_B);
        // roleB has all permissions on index "foo", so creating "foo" should succeed
        Request request = new Request("PUT", "/" + INDEX);
        request.setOptions(AUTH_OPTIONS);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testFirstResolvedRoleTakesPrecedence() throws Exception {
        // the first custom roles provider has set ROLE_A to only have read permission on the index,
        // the second custom roles provider has set ROLE_A to have all permissions, but since
        // the first custom role provider appears first in order, it should take precedence and deny
        // permission to create the index
        setupTestUser(ROLE_A);
        Request request = new Request("PUT", "/" + INDEX);
        request.setOptions(AUTH_OPTIONS);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
    }

    public void testUnresolvedRoleDoesntSucceed() throws Exception {
        setupTestUser("unknown");
        Request request = new Request("PUT", "/" + INDEX);
        request.setOptions(AUTH_OPTIONS);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
    }

}
