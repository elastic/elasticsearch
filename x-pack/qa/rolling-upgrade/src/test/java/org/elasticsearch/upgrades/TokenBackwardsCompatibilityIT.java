/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TokenBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    public void testGeneratingTokenInOldCluster() throws Exception {
        assumeTrue("this test should only run against the old cluster", CLUSTER_TYPE == ClusterType.OLD);
        final StringEntity tokenPostBody = new StringEntity("{\n" +
                "    \"username\": \"test_user\",\n" +
                "    \"password\": \"x-pack-test-password\",\n" +
                "    \"grant_type\": \"password\"\n" +
                "}", ContentType.APPLICATION_JSON);
        Response response = client().performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenPostBody);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        String token = (String) responseMap.get("access_token");
        assertNotNull(token);
        assertTokenWorks(token);

        StringEntity oldClusterToken = new StringEntity("{\n" +
                "    \"token\": \"" + token + "\"\n" +
                "}", ContentType.APPLICATION_JSON);
        Response indexResponse = client().performRequest("PUT", "token_backwards_compatibility_it/doc/old_cluster_token1",
                Collections.emptyMap(), oldClusterToken);
        assertOK(indexResponse);

        response = client().performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenPostBody);
        assertOK(response);
        responseMap = entityAsMap(response);
        token = (String) responseMap.get("access_token");
        assertNotNull(token);
        assertTokenWorks(token);
        oldClusterToken = new StringEntity("{\n" +
                "    \"token\": \"" + token + "\"\n" +
                "}", ContentType.APPLICATION_JSON);
        indexResponse = client().performRequest("PUT", "token_backwards_compatibility_it/doc/old_cluster_token2",
                Collections.emptyMap(), oldClusterToken);
        assertOK(indexResponse);
    }

    public void testTokenWorksInMixedOrUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the mixed or upgraded cluster",
                CLUSTER_TYPE == ClusterType.MIXED || CLUSTER_TYPE == ClusterType.UPGRADED);
        Response getResponse = client().performRequest("GET", "token_backwards_compatibility_it/doc/old_cluster_token1");
        assertOK(getResponse);
        Map<String, Object> source = (Map<String, Object>) entityAsMap(getResponse).get("_source");
        assertTokenWorks((String) source.get("token"));
    }

    public void testMixedCluster() throws Exception {
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        assumeTrue("the master must be on the latest version before we can write", isMasterOnLatestVersion());
        Response getResponse = client().performRequest("GET", "token_backwards_compatibility_it/doc/old_cluster_token2");
        assertOK(getResponse);
        Map<String, Object> source = (Map<String, Object>) entityAsMap(getResponse).get("_source");
        final String token = (String) source.get("token");
        assertTokenWorks(token);

        final StringEntity body = new StringEntity("{\"token\": \"" + token + "\"}", ContentType.APPLICATION_JSON);
        Response invalidationResponse = client().performRequest("DELETE", "_xpack/security/oauth2/token", Collections.emptyMap(), body);
        assertOK(invalidationResponse);
        assertTokenDoesNotWork(token);

        // create token and refresh on version that supports it
        final StringEntity tokenPostBody = new StringEntity("{\n" +
                "    \"username\": \"test_user\",\n" +
                "    \"password\": \"x-pack-test-password\",\n" +
                "    \"grant_type\": \"password\"\n" +
                "}", ContentType.APPLICATION_JSON);
        try (RestClient client = getRestClientForCurrentVersionNodesOnly()) {
            Response response = client.performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenPostBody);
            assertOK(response);
            Map<String, Object> responseMap = entityAsMap(response);
            String accessToken = (String) responseMap.get("access_token");
            String refreshToken = (String) responseMap.get("refresh_token");
            assertNotNull(accessToken);
            assertNotNull(refreshToken);
            assertTokenWorks(accessToken);

            final StringEntity tokenRefresh = new StringEntity("{\n" +
                    "    \"refresh_token\": \"" + refreshToken + "\",\n" +
                    "    \"grant_type\": \"refresh_token\"\n" +
                    "}", ContentType.APPLICATION_JSON);
            response = client.performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenRefresh);
            assertOK(response);
            responseMap = entityAsMap(response);
            String updatedAccessToken = (String) responseMap.get("access_token");
            String updatedRefreshToken = (String) responseMap.get("refresh_token");
            assertNotNull(updatedAccessToken);
            assertNotNull(updatedRefreshToken);
            assertTokenWorks(updatedAccessToken);
            assertTokenWorks(accessToken);
            assertNotEquals(accessToken, updatedAccessToken);
            assertNotEquals(refreshToken, updatedRefreshToken);
        }
    }

    public void testUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        Response getResponse = client().performRequest("GET", "token_backwards_compatibility_it/doc/old_cluster_token2");
        assertOK(getResponse);
        Map<String, Object> source = (Map<String, Object>) entityAsMap(getResponse).get("_source");
        final String token = (String) source.get("token");

        // invalidate again since this may not have been invalidated in the mixed cluster
        final StringEntity body = new StringEntity("{\"token\": \"" + token + "\"}", ContentType.APPLICATION_JSON);
        Response invalidationResponse = client().performRequest("DELETE", "_xpack/security/oauth2/token",
                Collections.singletonMap("error_trace", "true"), body);
        assertOK(invalidationResponse);
        assertTokenDoesNotWork(token);

        getResponse = client().performRequest("GET", "token_backwards_compatibility_it/doc/old_cluster_token1");
        assertOK(getResponse);
        source = (Map<String, Object>) entityAsMap(getResponse).get("_source");
        final String workingToken = (String) source.get("token");
        assertTokenWorks(workingToken);

        final StringEntity tokenPostBody = new StringEntity("{\n" +
                "    \"username\": \"test_user\",\n" +
                "    \"password\": \"x-pack-test-password\",\n" +
                "    \"grant_type\": \"password\"\n" +
                "}", ContentType.APPLICATION_JSON);
        Response response = client().performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenPostBody);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        String accessToken = (String) responseMap.get("access_token");
        String refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(accessToken);
        assertNotNull(refreshToken);
        assertTokenWorks(accessToken);

        final StringEntity tokenRefresh = new StringEntity("{\n" +
                "    \"refresh_token\": \"" + refreshToken + "\",\n" +
                "    \"grant_type\": \"refresh_token\"\n" +
                "}", ContentType.APPLICATION_JSON);
        response = client().performRequest("POST", "_xpack/security/oauth2/token", Collections.emptyMap(), tokenRefresh);
        assertOK(response);
        responseMap = entityAsMap(response);
        String updatedAccessToken = (String) responseMap.get("access_token");
        String updatedRefreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(updatedAccessToken);
        assertNotNull(updatedRefreshToken);
        assertTokenWorks(updatedAccessToken);
        assertTokenWorks(accessToken);
        assertNotEquals(accessToken, updatedAccessToken);
        assertNotEquals(refreshToken, updatedRefreshToken);
    }

    private void assertTokenWorks(String token) throws IOException {
        Response authenticateResponse = client().performRequest("GET", "_xpack/security/_authenticate", Collections.emptyMap(),
                new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token));
        assertOK(authenticateResponse);
        assertEquals("test_user", entityAsMap(authenticateResponse).get("username"));
    }

    private void assertTokenDoesNotWork(String token) {
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("GET", "_xpack/security/_authenticate", Collections.emptyMap(),
                        new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)));
        assertEquals(401, e.getResponse().getStatusLine().getStatusCode());
        Response response = e.getResponse();
        assertEquals("Bearer realm=\"security\", error=\"invalid_token\", error_description=\"The access token expired\"",
                response.getHeader("WWW-Authenticate"));
    }

    private boolean isMasterOnLatestVersion() throws Exception {
        Response response = client().performRequest("GET", "_cluster/state");
        assertOK(response);
        final String masterNodeId = ObjectPath.createFromResponse(response).evaluate("master_node");
        response = client().performRequest("GET", "_nodes");
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        return Version.CURRENT.equals(Version.fromString(objectPath.evaluate("nodes." + masterNodeId + ".version")));
    }

    private RestClient getRestClientForCurrentVersionNodesOnly() throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        List<HttpHost> hosts = new ArrayList<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            Version version = Version.fromString((String) nodeDetails.get("version"));
            if (Version.CURRENT.equals(version)) {
                Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
                hosts.add(HttpHost.create((String) httpInfo.get("publish_address")));
            }
        }

        return buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }
}
