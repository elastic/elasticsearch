/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.BeforeClass;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_TO_REALM_ASSOC_SETTING;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RestAuthenticateActionTests extends SecurityIntegTestCase {

    private static boolean anonymousEnabled;
    private static boolean operatorUser;
    private static boolean operatorPrivilegesEnabled;
    private static String domainName;

    @BeforeClass
    public static void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
    }

    @BeforeClass
    public static void maybeSetDomain() {
        domainName = randomFrom(randomAlphaOfLengthBetween(3, 5), null);
    }

    @BeforeClass
    public static void maybeSetOperator() {
        operatorUser = randomBoolean();
        operatorPrivilegesEnabled = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected String configOperatorUsers() {
        return super.configOperatorUsers()
            + "operator:\n"
            + "  - usernames: ['"
            + (operatorUser ? SecuritySettingsSource.TEST_USER_NAME : "_another_user")
            + "']\n";
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        if (anonymousEnabled) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), "anon")
                .putList(AnonymousUser.ROLES_SETTING.getKey(), SecuritySettingsSource.TEST_ROLE, "foo")
                .put(AuthorizationService.ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.getKey(), false);
        }
        if (domainName != null) {
            builder.put(DOMAIN_TO_REALM_ASSOC_SETTING.getConcreteSettingForNamespace(domainName).getKey(), "file");
        }
        builder.put("xpack.security.operator_privileges.enabled", operatorPrivilegesEnabled);
        return builder.build();
    }

    public void testAuthenticateApi() throws Exception {
        Request request = new Request("GET", "/_security/_authenticate");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        Response a = getRestClient().performRequest(request);
        ObjectPath objectPath = ObjectPath.createFromResponse(a);
        assertThat(objectPath.evaluate("username").toString(), equalTo(SecuritySettingsSource.TEST_USER_NAME));
        assertThat(objectPath.evaluate("authentication_realm.name").toString(), equalTo("file"));
        assertThat(objectPath.evaluate("authentication_realm.type").toString(), equalTo("file"));
        if (domainName != null) {
            assertThat(objectPath.evaluate("authentication_realm.domain").toString(), equalTo(domainName));
        } else {
            assertThat(objectPath.evaluate("lookup_realm.domain"), nullValue());
        }
        assertThat(objectPath.evaluate("lookup_realm.name").toString(), equalTo("file"));
        assertThat(objectPath.evaluate("lookup_realm.type").toString(), equalTo("file"));
        if (domainName != null) {
            assertThat(objectPath.evaluate("lookup_realm.domain").toString(), equalTo(domainName));
        } else {
            assertThat(objectPath.evaluate("lookup_realm.domain"), nullValue());
        }
        assertThat(objectPath.evaluate("authentication_type").toString(), equalTo("realm"));
        List<String> roles = objectPath.evaluate("roles");
        if (anonymousEnabled) {
            assertThat(roles.size(), is(3));
            assertThat(roles, contains(SecuritySettingsSource.TEST_ROLE, SecuritySettingsSource.TEST_ROLE, "foo"));
        } else {
            assertThat(roles.size(), is(1));
            assertThat(roles, contains(SecuritySettingsSource.TEST_ROLE));
        }
        if (operatorUser && operatorPrivilegesEnabled) {
            assertThat(objectPath.evaluate("operator"), equalTo(true));
        } else {
            assertThat(objectPath.evaluate("operator"), equalTo(null));
        }
    }

    public void testAuthenticateApiWithoutAuthentication() throws Exception {
        try {
            Response response = getRestClient().performRequest(new Request("GET", "/_security/_authenticate"));
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
        } catch (ResponseException e) {
            if (anonymousEnabled) {
                fail("request should have succeeded");
            } else {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
            }
        }
    }
}
