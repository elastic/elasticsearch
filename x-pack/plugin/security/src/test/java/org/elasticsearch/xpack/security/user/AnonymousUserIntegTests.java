/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AnonymousUserIntegTests extends SecurityIntegTestCase {
    private boolean authorizationExceptionsEnabled = randomBoolean();

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous")
                .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), authorizationExceptionsEnabled)
                .build();
    }

    @Override
    public String configRoles() {
        return super.configRoles() + "\n" +
                "anonymous:\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ READ ]\n";
    }

    public void testAnonymousViaHttp() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_nodes"));
            fail("request should have failed");
        } catch(ResponseException e) {
            int statusCode = e.getResponse().getStatusLine().getStatusCode();
            Response response = e.getResponse();
            if (authorizationExceptionsEnabled) {
                assertThat(statusCode, is(403));
                assertThat(response.getHeader("WWW-Authenticate"), nullValue());
                assertThat(EntityUtils.toString(response.getEntity()), containsString("security_exception"));
            } else {
                assertThat(statusCode, is(401));
                assertThat(response.getHeader("WWW-Authenticate"), notNullValue());
                assertThat(response.getHeader("WWW-Authenticate"), containsString("Basic"));
                assertThat(EntityUtils.toString(response.getEntity()), containsString("security_exception"));
            }
        }
    }
}
