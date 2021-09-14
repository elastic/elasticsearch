/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.BeforeClass;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.is;

public class RunAsIntegTests extends SecurityIntegTestCase {

    private static final String RUN_AS_USER = "run_as_user";
    private static final String CLIENT_USER = "transport_user";
    private static final String ROLES =
            "run_as_role:\n" +
            "  run_as: [ '" + SecuritySettingsSource.TEST_USER_NAME + "', 'idontexist' ]\n";

    // indicates whether the RUN_AS_USER that is being authenticated is also a superuser
    private static boolean runAsHasSuperUserRole;

    @BeforeClass
    public static void configureRunAsHasSuperUserRole() {
        runAsHasSuperUserRole = randomBoolean();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public String configRoles() {
        return ROLES + super.configRoles();
    }

    @Override
    public String configUsers() {
        return super.configUsers()
                + RUN_AS_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n"
                + CLIENT_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    public String configUsersRoles() {
        String roles = super.configUsersRoles()
                + "run_as_role:" + RUN_AS_USER + "\n"
                + "transport_client:" + CLIENT_USER;
        if (runAsHasSuperUserRole) {
            roles = roles + "\n"
                    + "superuser:" + RUN_AS_USER;
        }
        return roles;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return false;
    }

    public void testUserImpersonationUsingHttp() throws Exception {
        // use the http user and try to run as
        try {
            Request request = new Request("GET", "/_nodes");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(CLIENT_USER, TEST_PASSWORD_SECURE_STRING));
            options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, SecuritySettingsSource.TEST_USER_NAME);
            request.setOptions(options);
            getRestClient().performRequest(request);
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        if (runAsHasSuperUserRole == false) {
            try {
                //the run as user shouldn't have access to the nodes api
                Request request = new Request("GET", "/_nodes");
                RequestOptions.Builder options = request.getOptions().toBuilder();
                options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
                request.setOptions(options);
                getRestClient().performRequest(request);
                fail("request should have failed");
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
            }
        }

        // but when running as a different user it should work
        getRestClient().performRequest(requestForUserRunAsUser(SecuritySettingsSource.TEST_USER_NAME));
    }

    public void testEmptyHeaderUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser(""));
            fail("request should have failed");
        } catch(ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        }
    }

    public void testNonExistentRunAsUserUsingHttp() throws Exception {
        try {
            getRestClient().performRequest(requestForUserRunAsUser("idontexist"));
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    private static Request requestForUserRunAsUser(String user) {
        Request request = new Request("GET", "/_nodes");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(RUN_AS_USER, TEST_PASSWORD_SECURE_STRING));
        options.addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, user);
        request.setOptions(options);
        return request;
    }
}
