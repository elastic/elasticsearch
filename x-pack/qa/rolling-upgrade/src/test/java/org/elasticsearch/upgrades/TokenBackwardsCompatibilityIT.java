/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNot.not;

public class TokenBackwardsCompatibilityIT extends AbstractUpgradeTestCase {

    private Collection<RestClient> twoClients = null;

    @Before
    private void collectClientsByVersion() throws IOException {
        Map<Version, RestClient> clientsByVersion = getRestClientByVersion();
        if (clientsByVersion.size() == 2) {
            // usual case, clients have different versions
            twoClients = clientsByVersion.values();
        } else {
            assert clientsByVersion.size() == 1
                : "A rolling upgrade has a maximum of two distinct node versions, found: " + clientsByVersion.keySet();
            // tests assumes exactly two clients to simplify some logic
            twoClients = new ArrayList<>();
            twoClients.add(clientsByVersion.values().iterator().next());
            twoClients.add(clientsByVersion.values().iterator().next());
        }
    }

    @After
    private void closeClientsByVersion() throws IOException {
        for (RestClient client : twoClients) {
            client.close();
        }
        twoClients = null;
    }

    public void testGeneratingTokensInOldCluster() throws Exception {
        assumeTrue("this test should only run against the old cluster", CLUSTER_TYPE == ClusterType.OLD);
        // Creates two access and refresh tokens and stores them in the token_backwards_compatibility_it index to be used for tests in the
        // mixed/upgraded clusters
        Map<String, Object> responseMap = createTokens(client(), "test_user", "x-pack-test-password");
        String accessToken = (String) responseMap.get("access_token");
        assertNotNull(accessToken);
        assertAccessTokenWorks(accessToken);
        String refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(refreshToken);

        storeTokens(client(), 1, accessToken, refreshToken);

        responseMap = createTokens(client(), "test_user", "x-pack-test-password");
        accessToken = (String) responseMap.get("access_token");
        assertNotNull(accessToken);
        assertAccessTokenWorks(accessToken);
        refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(refreshToken);

        storeTokens(client(), 2, accessToken, refreshToken);
    }

    public void testRefreshingTokensInOldCluster() throws Exception {
        assumeTrue("this test should only run against the old cluster", CLUSTER_TYPE == ClusterType.OLD);
        // Creates access and refresh tokens and uses the refresh token. The new resulting tokens are used in different phases
        Map<String, Object> responseMap = createTokens(client(), "test_user", "x-pack-test-password");
        String accessToken = (String) responseMap.get("access_token");
        assertNotNull(accessToken);
        assertAccessTokenWorks(accessToken);
        String refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(refreshToken);

        storeTokens(client(), 3, accessToken, refreshToken);

        // refresh the token just created. The old token is invalid (tested further) and the new refresh token is tested in the upgraded
        // cluster
        Map<String, Object> refreshResponseMap = refreshToken(client(), refreshToken);
        String refreshedAccessToken = (String) refreshResponseMap.get("access_token");
        String refreshedRefreshToken = (String) refreshResponseMap.get("refresh_token");
        assertNotNull(refreshedAccessToken);
        assertNotNull(refreshedRefreshToken);
        assertAccessTokenWorks(refreshedAccessToken);
        // assert previous access token still works
        assertAccessTokenWorks(accessToken);

        storeTokens(client(), 4, refreshedAccessToken, refreshedRefreshToken);
    }

    public void testInvalidatingTokensInOldCluster() throws Exception {
        assumeTrue("this test should only run against the old cluster", CLUSTER_TYPE == ClusterType.OLD);
        // Creates access and refresh tokens and tries to use the access tokens several times
        Map<String, Object> responseMap = createTokens(client(), "test_user", "x-pack-test-password");
        String accessToken = (String) responseMap.get("access_token");
        assertNotNull(accessToken);
        assertAccessTokenWorks(accessToken);
        String refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(refreshToken);

        storeTokens(client(), 5, accessToken, refreshToken);

        // invalidate access token
        invalidateAccessToken(client(), accessToken);
        assertAccessTokenDoesNotWork(accessToken);
        // invalidate refresh token
        invalidateRefreshToken(client(), refreshToken);
        assertRefreshTokenInvalidated(refreshToken);
    }

    public void testAccessTokensWorkInMixedCluster() throws Exception {
        // Verify that an old token continues to work during all stages of the rolling upgrade
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        extendExpirationTimeForAllTokens();
        for (int tokenIdx : Arrays.asList(1, 3, 4)) { // 2 is invalidated in another mixed-cluster test, 5 is invalidated in the old cluster
            Map<String, Object> source = retrieveStoredTokens(client(), tokenIdx);
            assertAccessTokenWorks((String) source.get("token"));
        }
    }

    public void testTokensStayInvalidatedInMixedCluster() throws Exception {
        // Verify that an old, invalidated token remains invalidated during all stages of the rolling upgrade
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        Map<String, Object> source = retrieveStoredTokens(client(), 5);
        assertAccessTokenDoesNotWork((String) source.get("token"));
        assertRefreshTokenInvalidated((String) source.get("refresh_token"));
    }

    public void testGeneratingTokensInMixedCluster() throws Exception {
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        // Creates two access and refresh tokens and stores them in the token_backwards_compatibility_it index to be used for tests in the
        // mixed/upgraded clusters
        int generatedTokenIdxDuringMixed = 10;
        for (RestClient client : twoClients) {
            Map<String, Object> responseMap = createTokens(client, "test_user", "x-pack-test-password");
            String accessToken = (String) responseMap.get("access_token");
            assertNotNull(accessToken);
            assertAccessTokenWorks(accessToken);
            String refreshToken = (String) responseMap.get("refresh_token");
            assertNotNull(refreshToken);

            storeTokens(client(), generatedTokenIdxDuringMixed++, accessToken, refreshToken);

            responseMap = createTokens(client, "test_user", "x-pack-test-password");
            accessToken = (String) responseMap.get("access_token");
            assertNotNull(accessToken);
            assertAccessTokenWorks(accessToken);
            refreshToken = (String) responseMap.get("refresh_token");
            assertNotNull(refreshToken);

            storeTokens(client(), generatedTokenIdxDuringMixed++, accessToken, refreshToken);
        }
    }

    public void testRefreshingTokensInMixedCluster() throws Exception {
        // verify new nodes can refresh tokens created by old nodes and vice versa
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        for (RestClient client1 : twoClients) {
            Map<String, Object> responseMap = createTokens(client1, "test_user", "x-pack-test-password");
            String accessToken = (String) responseMap.get("access_token");
            assertNotNull(accessToken);
            assertAccessTokenWorks(accessToken);
            String refreshToken = (String) responseMap.get("refresh_token");
            assertNotNull(refreshToken);
            for (RestClient client2 : twoClients) {
                responseMap = refreshToken(client2, refreshToken);
                accessToken = (String) responseMap.get("access_token");
                assertNotNull(accessToken);
                assertAccessTokenWorks(accessToken);
                refreshToken = (String) responseMap.get("refresh_token");
                assertNotNull(refreshToken);
            }
        }
    }

    public void testInvalidatingTokensInMixedCluster() throws Exception {
        // Verify that we can invalidate an access and refresh token in a mixed cluster
        assumeTrue("this test should only run against the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        Map<String, Object> source = retrieveStoredTokens(client(), 2);
        String accessToken = (String) source.get("token");
        String refreshToken = (String) source.get("refresh_token");
        // The token might be already invalidated by running testInvalidatingTokenInMixedCluster in a previous stage
        // we don't try to assert it works before invalidating. This case is handled by testTokenWorksInMixedCluster
        invalidateAccessToken(client(), accessToken);
        assertAccessTokenDoesNotWork(accessToken);
        // invalidate refresh token
        invalidateRefreshToken(client(), refreshToken);
        assertRefreshTokenInvalidated(refreshToken);
    }

    public void testTokensStayInvalidatedInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        for (int tokenIdx : Arrays.asList(2, 5)) {
            Map<String, Object> source = retrieveStoredTokens(client(), tokenIdx);
            assertAccessTokenDoesNotWork((String) source.get("token"));
            assertRefreshTokenInvalidated((String) source.get("refresh_token"));
        }
    }

    public void testAccessTokensWorkInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        extendExpirationTimeForAllTokens();
        for (int tokenIdx : Arrays.asList(3, 4, 10, 12)) {
            Map<String, Object> source = retrieveStoredTokens(client(), tokenIdx);
            assertAccessTokenWorks((String) source.get("token"));
        }
    }

    public void testGeneratingTokensInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        Map<String, Object> responseMap = createTokens(client(), "test_user", "x-pack-test-password");
        String accessToken = (String) responseMap.get("access_token");
        assertNotNull(accessToken);
        assertAccessTokenWorks(accessToken);
        String refreshToken = (String) responseMap.get("refresh_token");
        assertNotNull(refreshToken);
    }

    public void testRefreshingTokensInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        for (int tokenIdx : Arrays.asList(4, 10, 12)) {
            Map<String, Object> source = retrieveStoredTokens(client(), tokenIdx);
            Map<String, Object> refreshedResponseMap = refreshToken(client(), (String) source.get("refresh_token"));
            String accessToken = (String) refreshedResponseMap.get("access_token");
            assertNotNull(accessToken);
            assertAccessTokenWorks(accessToken);
            String refreshToken = (String) refreshedResponseMap.get("refresh_token");
            assertNotNull(refreshToken);
        }
    }

    public void testInvalidatingTokensInUpgradedCluster() throws Exception {
        assumeTrue("this test should only run against the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        for (int tokenIdx : Arrays.asList(1, 11, 13)) {
            Map<String, Object> source = retrieveStoredTokens(client(), tokenIdx);
            String accessToken = (String) source.get("token");
            String refreshToken = (String) source.get("refresh_token");
            // invalidate access token
            invalidateAccessToken(client(), accessToken);
            assertAccessTokenDoesNotWork(accessToken);
            // invalidate refresh token
            invalidateRefreshToken(client(), refreshToken);
            assertRefreshTokenInvalidated(refreshToken);
        }
    }

    private void assertAccessTokenWorks(String token) throws IOException {
        for (RestClient client : twoClients) {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
            request.setOptions(options);
            Response authenticateResponse = client.performRequest(request);
            assertOK(authenticateResponse);
            assertEquals("test_user", entityAsMap(authenticateResponse).get("username"));
        }
    }

    private void assertAccessTokenDoesNotWork(String token) {
        for (RestClient client : twoClients) {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> client.performRequest(request));
            assertEquals(401, e.getResponse().getStatusLine().getStatusCode());
            Response response = e.getResponse();
            assertEquals("""
                Bearer realm="security", error="invalid_token", error_description="The access token expired"\
                """, response.getHeader("WWW-Authenticate"));
        }
    }

    private void assertRefreshTokenInvalidated(String refreshToken) throws IOException {
        for (RestClient client : twoClients) {
            Request refreshTokenRequest = new Request("POST", "/_security/oauth2/token");
            refreshTokenRequest.setJsonEntity(Strings.format("""
                {
                  "refresh_token": "%s",
                  "grant_type": "refresh_token"
                }
                """, refreshToken));
            ResponseException e = expectThrows(ResponseException.class, () -> client.performRequest(refreshTokenRequest));
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            Response response = e.getResponse();
            Map<String, Object> responseMap = entityAsMap(response);
            assertEquals("invalid_grant", responseMap.get("error"));
            assertEquals("token has been invalidated", responseMap.get("error_description"));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Version, RestClient> getRestClientByVersion() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Map<Version, List<HttpHost>> hostsByVersion = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            Version version = Version.fromString((String) nodeDetails.get("version"));
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            hostsByVersion.computeIfAbsent(version, k -> new ArrayList<>()).add(HttpHost.create((String) httpInfo.get("publish_address")));
        }
        Map<Version, RestClient> clientsByVersion = new HashMap<>();
        for (Map.Entry<Version, List<HttpHost>> entry : hostsByVersion.entrySet()) {
            clientsByVersion.put(entry.getKey(), buildClient(restClientSettings(), entry.getValue().toArray(new HttpHost[0])));
        }
        return clientsByVersion;
    }

    private Map<String, Object> createTokens(RestClient client, String username, String password) throws IOException {
        final Request createTokenRequest = new Request("POST", "/_security/oauth2/token");
        createTokenRequest.setJsonEntity(Strings.format("""
            {
              "username": "%s",
              "password": "%s",
              "grant_type": "password"
            }""", username, password));
        Response response = client().performRequest(createTokenRequest);
        assertOK(response);
        return entityAsMap(response);
    }

    private void storeTokens(RestClient client, int idx, String accessToken, String refreshToken) throws IOException {
        final Request indexRequest = new Request("PUT", "token_backwards_compatibility_it/_doc/old_cluster_token" + idx);
        indexRequest.setJsonEntity(Strings.format("""
            {
              "token": "%s",
              "refresh_token": "%s"
            }""", accessToken, refreshToken));
        Response indexResponse1 = client.performRequest(indexRequest);
        assertOK(indexResponse1);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> retrieveStoredTokens(RestClient client, int tokenIdx) throws IOException {
        Request getRequest = new Request("GET", "token_backwards_compatibility_it/_doc/old_cluster_token" + tokenIdx);
        Response getResponse = client().performRequest(getRequest);
        assertOK(getResponse);
        return (Map<String, Object>) entityAsMap(getResponse).get("_source");
    }

    private Map<String, Object> refreshToken(RestClient client, String refreshToken) throws IOException {
        final Request refreshTokenRequest = new Request("POST", "/_security/oauth2/token");
        refreshTokenRequest.setJsonEntity(Strings.format("""
            {
              "refresh_token": "%s",
              "grant_type": "refresh_token"
            }""", refreshToken));
        Response refreshResponse = client.performRequest(refreshTokenRequest);
        assertOK(refreshResponse);
        return entityAsMap(refreshResponse);
    }

    private void invalidateAccessToken(RestClient client, String accessToken) throws IOException {
        Request invalidateRequest = new Request("DELETE", "/_security/oauth2/token");
        invalidateRequest.setJsonEntity("{\"token\": \"" + accessToken + "\"}");
        invalidateRequest.addParameter("error_trace", "true");
        Response invalidateResponse = client.performRequest(invalidateRequest);
        assertOK(invalidateResponse);
    }

    private void invalidateRefreshToken(RestClient client, String refreshToken) throws IOException {
        Request invalidateRequest = new Request("DELETE", "/_security/oauth2/token");
        invalidateRequest.setJsonEntity("{\"refresh_token\": \"" + refreshToken + "\"}");
        invalidateRequest.addParameter("error_trace", "true");
        Response invalidateResponse = client.performRequest(invalidateRequest);
        assertOK(invalidateResponse);
    }

    /**
     * Hack to account for long-running tests. The max lifetime of a token is 1h, but sometimes our tests take longer so tokens created in
     * the old cluster may be expired by the time we run tests in the mixed/upgraded clusters.
     *
     * This method extends the expiration time of all tokens by writing to the `.security-token` index directly.
     *
     * We extend the expiration time for all tokens, instead of selected ones because it requires true hackery to get a hold of a docId
     * given only an access token and refresh token.
     */
    private void extendExpirationTimeForAllTokens() throws Exception {
        final List<String> tokensIds = getAllTokenIds();
        final var bulkRequest = new Request("POST", "/.security-tokens/_bulk?refresh=true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        final long newExpirationTime = Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli();
        bulkRequest.setJsonEntity(tokensIds.stream().map(tokenId -> Strings.format("""
            {"update": {"_id": "%s"}}
            {"doc": {"access_token": {"user_token": {"expiration_time": %s}}}}
            """, tokenId, newExpirationTime)).collect(Collectors.joining("\n")));
        final Response bulkResponse = client().performRequest(bulkRequest);
        assertOK(bulkResponse);
        final Map<String, Object> bulkResponseMap = entityAsMap(bulkResponse);
        assertEquals(false, bulkResponseMap.get("errors"));
    }

    private void refreshSecurityTokensIndex() throws IOException {
        // Ensure all tokens are available for search (token creation and other tokens operations have a WAIT_UNTIL refresh policy)
        final var refreshRequest = new Request("POST", "/.security-tokens/_refresh");
        refreshRequest.setOptions(refreshRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(refreshRequest));
    }

    private List<String> getAllTokenIds() throws IOException {
        refreshSecurityTokensIndex();
        final long searchSize = 100L;
        final var searchRequest = new Request("POST", "/.security-tokens/_search?size=" + searchSize);
        searchRequest.setOptions(searchRequest.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        searchRequest.setJsonEntity("""
            {
              "query": {
                "term": {
                  "doc_type": "token"
                }
              }
            }""");
        final Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        final SearchHits searchHits = SearchResponse.fromXContent(responseAsParser(searchResponse)).getHits();
        assertThat(
            "Search request used with size parameter that was too small to fetch all tokens.",
            searchHits.getTotalHits().value,
            lessThanOrEqualTo(searchSize)
        );
        final List<String> tokenIds = Arrays.stream(searchHits.getHits()).map(searchHit -> {
            assertNotNull(searchHit.getId());
            return searchHit.getId();
        }).toList();
        assertThat(tokenIds, not(empty()));
        return tokenIds;
    }
}
