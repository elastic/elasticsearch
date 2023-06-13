/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.apikey;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyWorkflowsRestrictionRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String MANAGE_API_KEY_USER = "manage_api_key_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    public static final String MANAGE_API_KEY_ROLE = "manage_api_key_role";

    @Before
    public void setup() throws IOException {
        createUser(MANAGE_API_KEY_USER, PASSWORD, List.of(MANAGE_API_KEY_ROLE));

        final Request putRoleRequest = new Request("PUT", "/_security/role/" + MANAGE_API_KEY_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": ["manage_api_key"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["read"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        final Request indexDocRequestA = new Request("POST", "/index-a/_doc?refresh=true");
        indexDocRequestA.setJsonEntity("{\"foo\": \"bar\"}");
        assertOK(adminClient().performRequest(indexDocRequestA));

        final Request indexDocRequestB = new Request("POST", "/index-b/_doc?refresh=true");
        indexDocRequestB.setJsonEntity("{\"baz\": \"qux\"}");
        assertOK(adminClient().performRequest(indexDocRequestB));
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(MANAGE_API_KEY_USER);
        deleteRole(MANAGE_API_KEY_ROLE);
        invalidateApiKeysForUser(MANAGE_API_KEY_USER);
        deleteIndex(adminClient(), "index-a");
        deleteIndex(adminClient(), "index-b");
    }

    public void testWorkflowsRestrictionDeniesAccess() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
                "name": "restricted_api_key",
                "role_descriptors":{
                    "r1": {
                        "indices": [
                          {
                            "names": ["index-a"],
                            "privileges": ["read"]
                          }
                        ],
                        "restriction": {
                            "workflows": ["search_application_query"]
                        }
                    }
                }
            }""");
        ObjectPath createApiKeyResponse = assertOKAndCreateObjectPath(performRequestWithUser(createApiKeyRequest, MANAGE_API_KEY_USER));
        String apiKeyEncoded = createApiKeyResponse.evaluate("encoded");
        assertThat(apiKeyEncoded, notNullValue());
        String apiKeyId = createApiKeyResponse.evaluate("id");
        assertThat(apiKeyId, notNullValue());

        final Request searchRequest = new Request("GET", "/index-a/_search");
        ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(searchRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Access is restricted only to [search_application_query] workflow(s)."));

        // Check that "same user permissions" are denied.
        final Request getApiKeyRequest = new Request("GET", "/_security/api_key");
        getApiKeyRequest.addParameter("id", apiKeyId);
        e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(getApiKeyRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Access is restricted only to [search_application_query] workflow(s)."));

        final Request hasPrivilegeRequest = new Request("POST", "/_security/user/_has_privileges");
        hasPrivilegeRequest.setJsonEntity("""
            {
              "index" : [
                {
                  "names": [ "index-a" ],
                  "privileges": [ "read" ]
                }
              ]
            }
            """);
        e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(hasPrivilegeRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Access is restricted only to [search_application_query] workflow(s)."));

        final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
        e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(authenticateRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("Access is restricted only to [search_application_query] workflow(s)."));
    }

    private Response performRequestWithUser(Request request, String username) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(username, PASSWORD))
        );
        return client().performRequest(request);
    }

    protected Response performRequestWithApiKey(final Request request, final String encoded) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
    }

}
