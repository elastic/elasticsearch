/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class SuperuserIT extends SecurityInBasicRestTestCase {

    private static final String SYSTEM_WRITE = "system_write";
    private static final String TEST_SUPERUSER = "test_superuser";
    private static final String TEST_SUPERUSER_WITH_SYSTEM_WRITE = "test_superuser_with_system_write";
    private static final SecureString TEST_PASSWORD = new SecureString("test_password".toCharArray());

    @Before
    public void setupSecurity() throws Exception {
        createSystemWriteRole();
        createUser(TEST_SUPERUSER, new String[] { "superuser" });
        createUser(TEST_SUPERUSER_WITH_SYSTEM_WRITE, new String[] { "superuser", SYSTEM_WRITE });
    }

    private void createUser(String username, String[] roles) throws IOException {
        final Request request = new Request("POST", "/_security/user/" + username);
        Map<String, Object> body = Map.ofEntries(Map.entry("roles", roles), Map.entry("password", TEST_PASSWORD.toString()));
        request.setJsonEntity(XContentTestUtils.convertToXContent(body, XContentType.JSON).utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    private void createSystemWriteRole() throws IOException {
        final Request addRole = new Request("POST", "/_security/role/" + SYSTEM_WRITE);
        addRole.setJsonEntity("""
            {
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": ["all"],
                  "allow_restricted_indices" : true
                }
              ]
            }""");
        Response response = adminClient().performRequest(addRole);
        assertOK(response);
    }

    /**
     * Verifies that users with the "superuser" role can read from the system indices (specifically the security index)
     */
    public void testSuperuserReadSystemIndex() throws Exception {
        final Request request = new Request("GET", "/.security/_doc/role-" + SYSTEM_WRITE);
        setAuthentication(request, TEST_SUPERUSER, TEST_PASSWORD);
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );

        Response response = client().performRequest(request);
        assertOK(response);

    }

    /**
     * Verifies that users with the "superuser" role cannot write to system indices (specifically the security index)
     */
    public void testSuperuserCannotWriteToSystemIndex() throws Exception {
        final Request request = new Request("PUT", "/.security/_doc/fake-" + randomAlphaOfLength(8));
        request.setJsonEntity("{ \"enabled\": false }");
        setAuthentication(request, TEST_SUPERUSER, TEST_PASSWORD);

        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(403));
    }

    /**
     * Verifies that users with the "superuser" role can get cluster health
     */
    public void testSuperuserGetClusterHealth() throws Exception {
        final Request request = new Request("GET", "/_cluster/health");
        setAuthentication(request, TEST_SUPERUSER, TEST_PASSWORD);

        Response response = client().performRequest(request);
        assertOK(response);
    }

    /**
     * Verifies that users with the "superuser" role can write to system indices (specifically the security index)
     * _if_ another role grants that access.
     * This tests that users with superuser can also have another role (and that role is effective).
     */
    public void testSuperuserWithAdditionalRoleCanWriteToSystemIndex() throws Exception {
        final Request request = new Request("PUT", "/.security/_doc/fake-" + randomAlphaOfLength(8));
        request.setJsonEntity("{ \"enabled\": false }");
        setAuthentication(request, TEST_SUPERUSER_WITH_SYSTEM_WRITE, TEST_PASSWORD);
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );

        Response response = client().performRequest(request);
        assertOK(response);
    }

    /**
     * Verifies that users with the "superuser" role can get cluster health, even if they have another role.
     * This tests that users with superuser and another role retain the privileges of the superuser role.
     */
    public void testSuperuserWithAdditionalRoleCanGetClusterHealth() throws Exception {
        final Request request = new Request("GET", "/_cluster/health");
        setAuthentication(request, TEST_SUPERUSER_WITH_SYSTEM_WRITE, TEST_PASSWORD);

        Response response = client().performRequest(request);
        assertOK(response);
    }

    private void setAuthentication(Request request, String username, SecureString password) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(HttpHeaders.AUTHORIZATION, UsernamePasswordToken.basicAuthHeaderValue(username, password));
        request.setOptions(options);
    }

    private void expectWarnings(Request request, String... expectedWarnings) {
        final Set<String> expected = Set.of(expectedWarnings);
        RequestOptions options = request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expected) == false;
        }).build();
        request.setOptions(options);
    }
}
