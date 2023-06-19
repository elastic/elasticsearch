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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyWorkflowsRestrictionRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String WORKFLOW_API_KEY_USER = "workflow_api_key_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    private static final String WORKFLOW_API_KEY_ROLE = "workflow_api_key_role";

    @Before
    public void setup() throws IOException {
        createUser(WORKFLOW_API_KEY_USER, PASSWORD, List.of(WORKFLOW_API_KEY_ROLE));

        final Request putRoleRequest = new Request("PUT", "/_security/role/" + WORKFLOW_API_KEY_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": ["manage_api_key"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        final Request indexDocRequestA = new Request("POST", "/index-a/_doc/doc1?refresh=true");
        indexDocRequestA.setJsonEntity("{\"foo\": \"bar\"}");
        assertOK(adminClient().performRequest(indexDocRequestA));

        final Request indexDocRequestB = new Request("POST", "/index-b/_doc/doc2?refresh=true");
        indexDocRequestB.setJsonEntity("{\"baz\": \"qux\"}");
        assertOK(adminClient().performRequest(indexDocRequestB));
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(WORKFLOW_API_KEY_USER);
        deleteRole(WORKFLOW_API_KEY_ROLE);
        invalidateApiKeysForUser(WORKFLOW_API_KEY_USER);
        deleteIndex(adminClient(), "index-a");
        deleteIndex(adminClient(), "index-b");
    }

    public void testWorkflowsRestrictionAllowsAccess() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
                "name": "restricted_api_key",
                "role_descriptors":{
                    "r1": {
                        "indices": [
                          {
                            "names": ["my-app-a"],
                            "privileges": ["read"]
                          }
                        ],
                        "restriction": {
                            "workflows": ["search_application_query"]
                        }
                    }
                }
            }""");
        ObjectPath createApiKeyResponse = assertOKAndCreateObjectPath(performRequestWithUser(createApiKeyRequest, WORKFLOW_API_KEY_USER));
        String apiKeyEncoded = createApiKeyResponse.evaluate("encoded");
        assertThat(apiKeyEncoded, notNullValue());
        String apiKeyId = createApiKeyResponse.evaluate("id");
        assertThat(apiKeyId, notNullValue());

        final Request createSearchApplicationARequest = new Request("PUT", "_application/search_application/my-app-a?create");
        createSearchApplicationARequest.setJsonEntity("""
            {
              "indices": [ "index-a" ],
              "template": {
                "script": {
                  "source": {
                    "query": {
                      "term": {
                        "{{field_name}}": "{{field_value}}"
                      }
                    }
                  },
                  "params": {
                    "field_name": "foo",
                    "field_value": "bar"
                  }
                }
              }
            }
            """);
        assertOK(adminClient().performRequest(createSearchApplicationARequest));
        ObjectPath queryResponseA = assertOKAndCreateObjectPath(
            performRequestWithApiKey(new Request("GET", "_application/search_application/my-app-a/_search"), apiKeyEncoded)
        );
        assertThat(queryResponseA.evaluate("hits.total.value"), equalTo(1));
        assertThat(queryResponseA.evaluate("hits.hits.0._id"), equalTo("doc1"));

        // Check that access is rejected by workflow restriction after successful search application query call.
        // This test additionally proves that the permission check works correctly even after the API key's role is cached.
        final Request searchRequest = new Request("GET", "/my-app-a/_search");
        var e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(searchRequest, apiKeyEncoded));
        assertThat(e.getMessage(), containsString("access restricted by workflow"));

        // Check that access is rejected due to missing index privilege and not due to the workflow restriction.
        final Request createSearchApplicationBRequest = new Request("PUT", "_application/search_application/my-app-b?create");
        createSearchApplicationBRequest.setJsonEntity("""
            {
              "indices": [ "index-b" ],
              "template": {
                "script": {
                  "source": {
                    "query": {
                      "term": {
                        "{{field_name}}": "{{field_value}}"
                      }
                    }
                  },
                  "params": {
                    "field_name": "baz",
                    "field_value": "qux"
                  }
                }
              }
            }
            """);
        assertOK(adminClient().performRequest(createSearchApplicationBRequest));

        e = expectThrows(
            ResponseException.class,
            () -> performRequestWithApiKey(new Request("GET", "_application/search_application/my-app-b/_search"), apiKeyEncoded)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "action [indices:data/read/xpack/application/search_application/search] is unauthorized for API key id ["
                    + apiKeyId
                    + "] of user ["
                    + WORKFLOW_API_KEY_USER
                    + "] on indices [my-app-b], this action is granted by the index privileges [read,all]"
            )
        );
        assertThat(e.getMessage(), not(containsString("access restricted by workflow")));

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
                            "names": ["*"],
                            "privileges": ["read"]
                          }
                        ],
                        "restriction": {
                            "workflows": ["search_application_query"]
                        }
                    }
                }
            }""");
        ObjectPath createApiKeyResponse = assertOKAndCreateObjectPath(performRequestWithUser(createApiKeyRequest, WORKFLOW_API_KEY_USER));
        String apiKeyEncoded = createApiKeyResponse.evaluate("encoded");
        assertThat(apiKeyEncoded, notNullValue());
        String apiKeyId = createApiKeyResponse.evaluate("id");
        assertThat(apiKeyId, notNullValue());

        final Request searchRequest = new Request("GET", "/index-*/_search");
        ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(searchRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [indices:data/read/search] is unauthorized for API key "));
        assertThat(e.getMessage(), containsString("access restricted by workflow"));

        // Check that "same user permissions" are denied.
        final Request getApiKeyRequest = new Request("GET", "/_security/api_key");
        getApiKeyRequest.addParameter("id", apiKeyId);
        e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(getApiKeyRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [cluster:admin/xpack/security/api_key/get] is unauthorized for API key "));
        assertThat(e.getMessage(), containsString("access restricted by workflow"));

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
        assertThat(
            e.getMessage(),
            containsString("action [cluster:admin/xpack/security/user/has_privileges] is unauthorized for API key ")
        );
        assertThat(e.getMessage(), containsString("access restricted by workflow"));

        final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
        e = expectThrows(ResponseException.class, () -> performRequestWithApiKey(authenticateRequest, apiKeyEncoded));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [cluster:admin/xpack/security/user/authenticate] is unauthorized for API key "));
        assertThat(e.getMessage(), containsString("access restricted by workflow"));
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
