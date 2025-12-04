/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

/**
 * REST test to reproduce a bug where a user can submit an async cross-cluster search
 * but cannot check its status due to missing monitor permission.
 *
 * The issue is in RBACEngine.authorizeClusterAction() which checks:
 *   role.checkIndicesAction(SubmitAsyncSearchAction.NAME)
 * This check may fail for cross-cluster searches because the permission check context
 * might be different when checking status vs when submitting the search.
 */
public class CrossClusterAsyncSearchAuthRestIT extends ESRestTestCase {

    private static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";
    private static final String TEST_USER = "test_search_user";
    private static final String TEST_ROLE = "test_search_role";
    private static final String TEST_PASSWORD = "test-password";
    private static final String OTHER_USER = "other_user";
    private static final String OTHER_ROLE = "other_role";
    private static final String OTHER_PASSWORD = "other-password";
    private static final String LOCAL_INDEX = "local_index";
    private static final String REMOTE_INDEX = "remote_index";
    private static final String ADMIN_USER = "admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    // Remote cluster (fulfilling cluster)
    private static final ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote-cluster")
        .module("analysis-common")
        .module("x-pack-async-search")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user(ADMIN_USER, ADMIN_PASSWORD)
        .build();

    // Local cluster (querying cluster) - connects to remote cluster
    private static final ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local-cluster")
        .module("analysis-common")
        .module("x-pack-async-search")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("node.roles", "[data,ingest,master,remote_cluster_client]")
        .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds", () -> "\"" + remoteCluster.getTransportEndpoint(0) + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".skip_unavailable", "false")
        .user(ADMIN_USER, ADMIN_PASSWORD)
        .build();

    @ClassRule
    // Use RuleChain to ensure remote cluster starts before local cluster
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, new SecureString(ADMIN_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void setupSecurity() throws Exception {
        Request putRoleRequest = new Request("PUT", "/_security/role/" + TEST_ROLE);
        putRoleRequest.setJsonEntity(Strings.format("""
            {
              "remote_indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"],
                  "clusters": ["%s"]
                }
              ]
            }""", REMOTE_CLUSTER_ALIAS));
        assertOK(adminClient().performRequest(putRoleRequest));

        // Create user on local cluster
        Request putUserRequest = new Request("PUT", "/_security/user/" + TEST_USER);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"],
              "email": "%s@test.example.com",
              "full_name": "Test Search User"
            }""", TEST_PASSWORD, TEST_ROLE, TEST_USER));
        assertOK(adminClient().performRequest(putUserRequest));

        // Also create the same role and user on remote cluster
        // (needed for seeds mode with security - remote cluster needs to know about the user)
        RestClient remoteClient = buildRestClient(remoteCluster);
        try {
            // Create role on remote cluster (just indices, no remote_indices needed)
            // Need view_index_metadata for search_shards action
            Request putRemoteRoleRequest = new Request("PUT", "/_security/role/" + TEST_ROLE);
            putRemoteRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["*"],
                      "privileges": ["read", "view_index_metadata"]
                    }
                  ]
                }""");
            assertOK(performRequestWithAdminUser(remoteClient, putRemoteRoleRequest));

            // Create user on remote cluster
            Request putRemoteUserRequest = new Request("PUT", "/_security/user/" + TEST_USER);
            putRemoteUserRequest.setJsonEntity(Strings.format("""
                {
                  "password": "%s",
                  "roles": ["%s"],
                  "email": "%s@test.example.com",
                  "full_name": "Test Search User"
                }""", TEST_PASSWORD, TEST_ROLE, TEST_USER));
            assertOK(performRequestWithAdminUser(remoteClient, putRemoteUserRequest));

            // Create another user/role for negative testing (ownership checks)
            // This user will have similar permissions but be a different user
            Request putOtherRoleRequest = new Request("PUT", "/_security/role/" + OTHER_ROLE);
            putOtherRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["*"],
                      "privileges": ["read", "view_index_metadata"]
                    }
                  ]
                }""");
            assertOK(performRequestWithAdminUser(remoteClient, putOtherRoleRequest));

            Request putOtherUserRequest = new Request("PUT", "/_security/user/" + OTHER_USER);
            putOtherUserRequest.setJsonEntity(Strings.format("""
                {
                  "password": "%s",
                  "roles": ["%s"],
                  "email": "%s@test.example.com",
                  "full_name": "Other User"
                }""", OTHER_PASSWORD, OTHER_ROLE, OTHER_USER));
            assertOK(performRequestWithAdminUser(remoteClient, putOtherUserRequest));
        } finally {
            remoteClient.close();
        }

        // Also create the other user on local cluster with remote_indices permissions
        Request putOtherRoleLocalRequest = new Request("PUT", "/_security/role/" + OTHER_ROLE);
        putOtherRoleLocalRequest.setJsonEntity(Strings.format("""
            {
              "remote_indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"],
                  "clusters": ["%s"]
                }
              ]
            }""", REMOTE_CLUSTER_ALIAS));
        assertOK(adminClient().performRequest(putOtherRoleLocalRequest));

        Request putOtherUserLocalRequest = new Request("PUT", "/_security/user/" + OTHER_USER);
        putOtherUserLocalRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"],
              "email": "%s@test.example.com",
              "full_name": "Other User"
            }""", OTHER_PASSWORD, OTHER_ROLE, OTHER_USER));
        assertOK(adminClient().performRequest(putOtherUserLocalRequest));
    }

    @Before
    public void setupIndices() throws Exception {
        // Create indices on both clusters
        Request createLocalIndex = new Request("PUT", "/" + LOCAL_INDEX);
        createLocalIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "field": { "type": "text" }
                }
              }
            }""");
        assertOK(client().performRequest(createLocalIndex));

        // Index some documents on local cluster
        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            bulkBody.append(Strings.format("""
                { "index": { "_index": "%s", "_id": "%d" } }
                { "field": "value%d" }
                """, LOCAL_INDEX, i, i));
        }
        bulkRequest.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(bulkRequest));

        // Create index on remote cluster
        RestClient remoteClient = buildRestClient(remoteCluster);
        try {
            Request createRemoteIndex = new Request("PUT", "/" + REMOTE_INDEX);
            createRemoteIndex.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "field": { "type": "text" }
                    }
                  }
                }""");
            assertOK(performRequestWithAdminUser(remoteClient, createRemoteIndex));

            // Index some documents on remote cluster
            bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkBody = new StringBuilder();
            for (int i = 0; i < 10; i++) {
                bulkBody.append(Strings.format("""
                    { "index": { "_index": "%s", "_id": "%d" } }
                    { "field": "value%d" }
                    """, REMOTE_INDEX, i, i));
            }
            bulkRequest.setJsonEntity(bulkBody.toString());
            assertOK(performRequestWithAdminUser(remoteClient, bulkRequest));
        } finally {
            remoteClient.close();
        }
    }

    /**
     * Test that verifies users with only remote_indices permissions can check async search status.
     *
     * This test verifies that RBACEngine also checks remote_indices permissions when
     * determining if a user can check async search status.
     */
    public void testAsyncCrossClusterSearchStatusCheckWithoutMonitorPermission() throws Exception {
        // Submit an async cross-cluster search as the test user; this should succeed
        // Note: User only has remote_indices permissions, not local indices permissions
        // So they can only search remote indices, not local ones
        Request submitRequest = new Request("POST", "/" + REMOTE_CLUSTER_ALIAS + ":" + REMOTE_INDEX + "/_async_search");
        setRunAsHeader(submitRequest, TEST_USER);
        submitRequest.addParameter("wait_for_completion_timeout", "0ms");
        submitRequest.addParameter("keep_on_completion", "true");
        submitRequest.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              },
              "size": 10
            }""");

        Response submitResponse = client().performRequest(submitRequest);
        assertOK(submitResponse);
        Map<String, Object> submitResponseMap = responseAsMap(submitResponse);
        String searchId = (String) submitResponseMap.get("id");
        assertNotNull("Search ID should be returned", searchId);

        try {
            // Check the status - this should succeed even though the user doesn't have
            // cluster:monitor/async_search/status permission, because RBACEngine's workaround
            // checks both local indices permissions (via role.checkIndicesAction) and
            // remote_indices permissions (via canSubmitAsyncSearch helper method).
            // The user can submit async searches to remote indices, so they should be able
            // to check the status of those searches.
            Request statusRequest = new Request("GET", "/_async_search/status/" + searchId);
            setRunAsHeader(statusRequest, TEST_USER);

            Response statusResponse = client().performRequest(statusRequest);
            assertOK(statusResponse);

            // Verify we got a valid status response
            Map<String, Object> statusResponseMap = responseAsMap(statusResponse);
            assertNotNull("Status response should contain id", statusResponseMap.get("id"));
            assertThat("Status response id should match search id", statusResponseMap.get("id"), equalTo(searchId));
        } finally {
            // Clean up
            Request deleteRequest = new Request("DELETE", "/_async_search/" + searchId);
            setRunAsHeader(deleteRequest, TEST_USER);
            try {
                client().performRequest(deleteRequest);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Negative test: Verifies that a user cannot check the status of async searches
     * created by a different user, even if they have similar permissions.
     * This ensures that ownership checks are still enforced after the workaround in RBACEngine.
     */
    public void testCannotCheckStatusOfOtherUsersSearch() throws Exception {
        // Submit an async search as TEST_USER
        Request submitRequest = new Request("POST", "/" + REMOTE_CLUSTER_ALIAS + ":" + REMOTE_INDEX + "/_async_search");
        setRunAsHeader(submitRequest, TEST_USER);
        submitRequest.addParameter("wait_for_completion_timeout", "0ms");
        submitRequest.addParameter("keep_on_completion", "true");
        submitRequest.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              },
              "size": 10
            }""");

        Response submitResponse = client().performRequest(submitRequest);
        assertOK(submitResponse);
        Map<String, Object> submitResponseMap = responseAsMap(submitResponse);
        String searchId = (String) submitResponseMap.get("id");
        assertNotNull("Search ID should be returned", searchId);

        try {
            // Try to check the status as OTHER_USER. This should fail with 404
            // because ownership is enforced by AsyncSearchSecurity
            Request statusRequest = new Request("GET", "/_async_search/status/" + searchId);
            setRunAsHeader(statusRequest, OTHER_USER);

            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(statusRequest));

            // Verify it's a 404 (not found) because the user doesn't own the search
            assertThat(
                "Status code should indicate search not found (ownership check)",
                exception.getResponse().getStatusLine().getStatusCode(),
                equalTo(404)
            );
        } finally {
            // Clean up
            Request deleteRequest = new Request("DELETE", "/_async_search/" + searchId);
            setRunAsHeader(deleteRequest, TEST_USER);
            try {
                client().performRequest(deleteRequest);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Negative test: Verifies that a user without remote_indices permissions
     * cannot check status, even if they try to use a valid search ID.
     * This ensures the fix doesn't grant permissions to unauthorized users.
     */
    public void testCannotCheckStatusWithoutRemoteIndicesPermissions() throws Exception {
        // First, create a user with NO permissions
        String noPermsUser = "no_perms_user";
        String noPermsRole = "no_perms_role";

        Request putNoPermsRoleRequest = new Request("PUT", "/_security/role/" + noPermsRole);
        putNoPermsRoleRequest.setJsonEntity("""
            {
              "indices": [],
              "remote_indices": []
            }""");
        assertOK(adminClient().performRequest(putNoPermsRoleRequest));

        Request putNoPermsUserRequest = new Request("PUT", "/_security/user/" + noPermsUser);
        putNoPermsUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "no-perms-password",
              "roles": ["%s"],
              "email": "%s@test.example.com",
              "full_name": "No Permissions User"
            }""", noPermsRole, noPermsUser));
        assertOK(adminClient().performRequest(putNoPermsUserRequest));

        try {
            // Submit a search as TEST_USER to get a valid search ID
            Request submitRequest = new Request("POST", "/" + REMOTE_CLUSTER_ALIAS + ":" + REMOTE_INDEX + "/_async_search");
            setRunAsHeader(submitRequest, TEST_USER);
            submitRequest.addParameter("wait_for_completion_timeout", "0ms");
            submitRequest.addParameter("keep_on_completion", "true");
            submitRequest.setJsonEntity("""
                {
                  "query": {
                    "match_all": {}
                  },
                  "size": 10
                }""");

            Response submitResponse = client().performRequest(submitRequest);
            assertOK(submitResponse);
            Map<String, Object> submitResponseMap = responseAsMap(submitResponse);
            String searchId = (String) submitResponseMap.get("id");
            assertNotNull("Search ID should be returned", searchId);

            // Try to check the status as the user with no permissions
            Request statusRequest = new Request("GET", "/_async_search/status/" + searchId);
            setRunAsHeader(statusRequest, noPermsUser);

            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(statusRequest));

            // Verify it's a 403 (forbidden) because the user doesn't have permissions
            assertThat(
                "Status code should indicate authorization failure",
                exception.getResponse().getStatusLine().getStatusCode(),
                equalTo(403)
            );
            assertThat("Exception should be related to authorization", exception.getMessage(), containsString("unauthorized"));
        } finally {
            // Clean up users/roles
            try {
                Request deleteUserRequest = new Request("DELETE", "/_security/user/" + noPermsUser);
                adminClient().performRequest(deleteUserRequest);
                Request deleteRoleRequest = new Request("DELETE", "/_security/role/" + noPermsRole);
                adminClient().performRequest(deleteRoleRequest);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    private static void setRunAsHeader(Request request, String user) {
        request.setOptions(request.getOptions().toBuilder().addHeader(RUN_AS_USER_HEADER, user).build());
    }

    private static RestClient buildRestClient(ElasticsearchCluster cluster) {
        String httpAddress = cluster.getHttpAddress(0);
        int portSeparator = httpAddress.lastIndexOf(':');
        String host = httpAddress.substring(0, portSeparator);
        int port = Integer.parseInt(httpAddress.substring(portSeparator + 1));
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"));
        try {
            doConfigureClient(builder, Settings.EMPTY);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    private static Response performRequestWithAdminUser(RestClient client, Request request) throws IOException {
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(ADMIN_USER, new SecureString(ADMIN_PASSWORD.toCharArray())))
                .build()
        );
        return client.performRequest(request);
    }
}
