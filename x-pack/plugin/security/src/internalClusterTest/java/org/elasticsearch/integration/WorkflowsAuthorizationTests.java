/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.rest.ESRestTestCase.assertOK;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class WorkflowsAuthorizationTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockTransportService() {
        return false; // security has its own transport service
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    private static final String ROLES = """
        role_a:
          cluster: [ all ]
          indices:
            - names: '*'
              privileges: [ all ]
        role_b:
          cluster: [ all ]
          indices:
            - names: '*'
              privileges: [ all ]
          workflows: ['search_application']
        """;

    private static final String USERS_ROLES = """
        role_a:user_a
        role_b:user_b
        """;

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() + "user_a:" + usersPasswdHashed + "\n" + "user_b:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    public void testWorkflowAuthorization() throws Exception {
        // Note: Au
        Request request = new Request("GET", "/_security/_authenticate");
        Response response = performRequestWithUser("user_a", request);
        assertOK(response);
        System.out.println("==========" + responseAsMap(response));

    }

    protected static Map<String, Object> responseAsMap(Response response) throws IOException {
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(
            entityContentType.xContent(),
            response.getEntity().getContent(),
            false
        );
        assertNotNull(responseEntity);
        return responseEntity;
    }

    private Response performRequestWithUser(String username, Request request) throws IOException {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(username, new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
        );
        request.setOptions(options);
        return getRestClient().performRequest(request);
    }
}
