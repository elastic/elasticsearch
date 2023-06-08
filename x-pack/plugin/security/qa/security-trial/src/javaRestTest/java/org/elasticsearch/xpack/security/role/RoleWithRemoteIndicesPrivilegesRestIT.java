/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RoleWithRemoteIndicesPrivilegesRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    @Before
    public void setup() throws IOException {
        createUser(REMOTE_SEARCH_USER, PASSWORD, List.of(REMOTE_SEARCH_ROLE));
        createIndex(adminClient(), "index-a", null, null, null);
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(REMOTE_SEARCH_USER);
        deleteIndex(adminClient(), "index-a");
    }

    public void testRemoteIndexPrivileges() throws IOException {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"],
                  "query": "{\\"match\\":{\\"field\\":\\"a\\"}}",
                  "field_security" : {
                    "grant": ["field"]
                  }
                }
              ]
            }""");
        final Response putRoleResponse1 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse1);

        final Response getRoleResponse = adminClient().performRequest(new Request("GET", "/_security/role/" + REMOTE_SEARCH_ROLE));
        assertOK(getRoleResponse);
        expectRoleDescriptorInResponse(
            getRoleResponse,
            new RoleDescriptor(
                REMOTE_SEARCH_ROLE,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-a", "*")
                        .indices("index-a", "*")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .privileges("read")
                        .grantedFields("field")
                        .build() },
                null
            )
        );

        // Remote privilege does not authorize local search
        var searchRequest = new Request("GET", "/index-a/_search");
        searchRequest.setOptions(
            searchRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
        );
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [" + SearchAction.NAME + "] is unauthorized for user"));

        // Add local privileges and check local authorization works
        putRoleRequest = new Request("PUT", "_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"]
                }
              ],
              "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"],
                  "query": "{\\"match\\":{\\"field\\":\\"a\\"}}",
                  "field_security" : {
                    "grant": ["field"]
                  }
                }
              ]
            }""");
        final Response putRoleResponse2 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse2);
        final Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);

        // Check local cluster privilege also works
        var clusterActionRequest = new Request("GET", "/_cluster/health");
        clusterActionRequest.setOptions(
            clusterActionRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
        );
        assertOK(client().performRequest(clusterActionRequest));

        final Response getRoleResponse2 = adminClient().performRequest(new Request("GET", "_security/role/" + REMOTE_SEARCH_ROLE));
        assertOK(getRoleResponse2);
        expectRoleDescriptorInResponse(
            getRoleResponse2,
            new RoleDescriptor(
                REMOTE_SEARCH_ROLE,
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("index-a").privileges("all").build() },
                null,
                null,
                null,
                null,
                null,
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    RoleDescriptor.RemoteIndicesPrivileges.builder("remote-a", "*")
                        .indices("index-a", "*")
                        .privileges("read")
                        .query("{\"match\":{\"field\":\"a\"}}")
                        .grantedFields("field")
                        .build() },
                null
            )
        );
    }

    public void testGetUserPrivileges() throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"],
                  "query": "{\\"match\\":{\\"field\\":\\"a\\"}}",
                  "field_security": {
                    "grant": ["field"]
                  }
                }
              ]
            }""");
        final Response putRoleResponse1 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse1);

        final Response getUserPrivilegesResponse1 = executeAsRemoteSearchUser(new Request("GET", "/_security/user/_privileges"));
        assertOK(getUserPrivilegesResponse1);
        assertThat(responseAsMap(getUserPrivilegesResponse1), equalTo(mapFromJson("""
            {
              "cluster": [],
              "global": [],
              "indices": [],
              "applications": [],
              "run_as": [],
              "remote_indices": [
                {
                  "names": ["*", "index-a"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false,
                  "clusters": ["remote-a", "*"],
                  "query": ["{\\"match\\":{\\"field\\":\\"a\\"}}"],
                  "field_security": [{"grant": ["field"]}]
                }
              ]
            }""")));

        final var putRoleRequest2 = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest2.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"]
                }
              ],
              "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"]
                }
              ]
            }""");
        final Response putRoleResponse2 = adminClient().performRequest(putRoleRequest2);
        assertOK(putRoleResponse2);

        final Response getUserPrivilegesResponse2 = executeAsRemoteSearchUser(new Request("GET", "/_security/user/_privileges"));
        assertOK(getUserPrivilegesResponse2);
        assertThat(responseAsMap(getUserPrivilegesResponse2), equalTo(mapFromJson("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"],
                  "allow_restricted_indices": false
                }
              ],
              "applications": [],
              "run_as": [],
              "remote_indices": [
                {
                  "names": ["*", "index-a"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false,
                  "clusters": ["remote-a", "*"]
                }
              ]
            }""")));
    }

    public void testGetUserPrivilegesWithMultipleFlsDlsDefinitionsPreservesGroupPerIndexPrivilege() throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "remote_indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"],
                  "query": "{\\"match\\":{\\"field-a\\":\\"a\\"}}",
                  "field_security": {
                    "grant": ["field-a"]
                  }
                },
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"],
                  "clusters": ["remote-a", "*"],
                  "query": "{\\"match\\":{\\"field-b\\":\\"b\\"}}",
                  "field_security": {
                    "grant": ["field-b"]
                  }
                }
              ]
            }""");
        final Response putRoleResponse1 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse1);

        final Response getUserPrivilegesResponse1 = executeAsRemoteSearchUser(new Request("GET", "/_security/user/_privileges"));
        assertOK(getUserPrivilegesResponse1);
        final Map<String, Object> actual = responseAsMap(getUserPrivilegesResponse1);
        // The order of remote indices is not deterministic, so manually extract remote indices from map response to compare ignoring order
        @SuppressWarnings("unchecked")
        final List<Object> rawRemoteIndices = (List<Object>) actual.get("remote_indices");
        assertThat(rawRemoteIndices, notNullValue());
        assertThat(rawRemoteIndices, containsInAnyOrder(mapFromJson("""
            {
              "names": ["*", "index-a"],
              "privileges": ["read"],
              "allow_restricted_indices": false,
              "clusters": ["remote-a", "*"],
              "query": ["{\\"match\\":{\\"field-a\\":\\"a\\"}}"],
              "field_security": [{"grant": ["field-a"]}]
            }"""), mapFromJson("""
            {
              "names": ["*", "index-a"],
              "privileges": ["read"],
              "allow_restricted_indices": false,
              "clusters": ["remote-a", "*"],
              "query": ["{\\"match\\":{\\"field-b\\":\\"b\\"}}"],
              "field_security": [{"grant": ["field-b"]}]
            }""")));
    }

    private static Map<String, Object> mapFromJson(String json) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    private Response executeAsRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
        );
        return client().performRequest(request);
    }

    private void expectRoleDescriptorInResponse(final Response getRoleResponse, final RoleDescriptor expectedRoleDescriptor)
        throws IOException {
        final Map<String, RoleDescriptor> actual = responseAsParser(getRoleResponse).map(
            HashMap::new,
            p -> RoleDescriptor.parse(expectedRoleDescriptor.getName(), p, false)
        );
        assertThat(actual, equalTo(Map.of(expectedRoleDescriptor.getName(), expectedRoleDescriptor)));
    }
}
