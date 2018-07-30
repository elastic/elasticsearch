/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RestAuthenticateActionTests extends SecurityIntegTestCase {

    private static boolean anonymousEnabled;

    @BeforeClass
    public static void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        if (anonymousEnabled) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), "anon")
                   .putList(AnonymousUser.ROLES_SETTING.getKey(), SecuritySettingsSource.TEST_ROLE, "foo")
                   .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false);
        }
        return builder.build();
    }

    public void testAuthenticateApi() throws Exception {
        Response response = getRestClient().performRequest("GET", "/_xpack/security/_authenticate",
                new BasicHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("username").toString(), equalTo(SecuritySettingsSource.TEST_USER_NAME));
        List<String> roles = objectPath.evaluate("roles");
        assertThat(roles.size(), is(1));
        assertThat(roles, contains(SecuritySettingsSource.TEST_ROLE));
    }

    public void testAuthenticateApiWithoutAuthentication() throws Exception {
        try {
            Response response = getRestClient().performRequest("GET", "/_xpack/security/_authenticate");
            if (anonymousEnabled) {
                assertThat(response.getStatusLine().getStatusCode(), is(200));
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                assertThat(objectPath.evaluate("username").toString(), equalTo("anon"));
                @SuppressWarnings("unchecked")
                List<String> roles = (List<String>) objectPath.evaluate("roles");
                assertThat(roles.size(), is(2));
                assertThat(roles, contains(SecuritySettingsSource.TEST_ROLE, "foo"));
            } else {
                fail("request should have failed");
            }
        } catch(ResponseException e) {
            if (anonymousEnabled) {
                fail("request should have succeeded");
            } else {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
            }
        }
    }
}
