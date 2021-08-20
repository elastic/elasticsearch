/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.directory.api.util.Strings;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class TokenAuthIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // crank up the deletion interval and set timeout for delete requests
            .put(TokenService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(200L))
            .put(TokenService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5L))
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Override
    protected int maxNumberOfNodes() {
        // we start one more node so we need to make sure if we hit max randomization we can still start one
        return defaultMaxNumberOfNodes() + 1;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void testTokenServiceBootstrapOnNodeJoin() throws Exception {
        final Client client = client();
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse response = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getTokenString(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }
        // start a new node and see if it can decrypt the token
        String nodeName = internalCluster().startNode();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getTokenString(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }

        TokenService tokenService = internalCluster().getInstance(TokenService.class, nodeName);
        PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
        tokenService.decodeToken(response.getTokenString(), userTokenFuture);
        assertNotNull(userTokenFuture.actionGet());
        assertNotNull(response.getAuthentication());
    }


    public void testTokenServiceCanRotateKeys() throws Exception {
        final Client client = client();
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse response = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        String masterName = internalCluster().getMasterName();
        TokenService masterTokenService = internalCluster().getInstance(TokenService.class, masterName);
        String activeKeyHash = masterTokenService.getActiveKeyHash();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getTokenString(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
            assertEquals(activeKeyHash, tokenService.getActiveKeyHash());
        }
        client().admin().cluster().prepareHealth().execute().get();
        PlainActionFuture<AcknowledgedResponse> rotateActionFuture = new PlainActionFuture<>();
        logger.info("rotate on master: {}", masterName);
        masterTokenService.rotateKeysOnMaster(rotateActionFuture);
        assertTrue(rotateActionFuture.actionGet().isAcknowledged());
        assertNotEquals(activeKeyHash, masterTokenService.getActiveKeyHash());

        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getTokenString(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
            assertNotEquals(activeKeyHash, tokenService.getActiveKeyHash());
        }
        assertNotNull(response.getAuthentication());
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.getAuthentication().getUser().principal());
    }

    public void testExpiredTokensDeletedAfterExpiration() throws Exception {
        final Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse response = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        final String accessToken = response.getTokenString();
        final String refreshToken = response.getRefreshToken();
        Instant created = Instant.now();

        InvalidateTokenResponse invalidateResponse = securityClient
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                .setSource(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.termQuery("doc_type", "token")))
                .setSize(1)
                .setTerminateAfter(1)
                .get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });

        // hack doc to modify the creation time to the day before
        Instant yesterday = created.minus(36L, ChronoUnit.HOURS);
        assertTrue(Instant.now().isAfter(yesterday));
        client.prepareUpdate(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, SINGLE_MAPPING_NAME, docId.get())
            .setDoc("creation_time", yesterday.toEpochMilli())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                // invalidate a invalid token... doesn't matter that it is bad... we just want this action to trigger the deletion
                InvalidateTokenResponse invalidateResponseTwo = securityClient.prepareInvalidateToken("fooobar")
                    .setType(randomFrom(InvalidateTokenRequest.Type.values()))
                    .execute()
                    .actionGet();
                assertThat(invalidateResponseTwo.getResult().getInvalidatedTokens(), equalTo(0));
                assertThat(invalidateResponseTwo.getResult().getPreviouslyInvalidatedTokens(), equalTo(0));
                assertThat(invalidateResponseTwo.getResult().getErrors(), empty());
            }
            client.admin().indices().prepareRefresh(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS).get();
            SearchResponse searchResponse = client.prepareSearch(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                .setSource(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.termQuery("doc_type", "token")))
                .setTerminateAfter(1)
                .get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        }, 30, TimeUnit.SECONDS);

        // Now the documents are deleted, try to invalidate the access token and refresh token again
        InvalidateTokenResponse invalidateAccessTokenResponse = securityClient.prepareInvalidateToken(accessToken)
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .execute()
            .actionGet();
        assertThat(invalidateAccessTokenResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateAccessTokenResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateAccessTokenResponse.getResult().getErrors(), empty());

        // Weird testing behaviour ahead...
        // invalidating by access token (above) is a Get, but invalidating by refresh token (below) is a Search
        // In a multi node cluster, in a small % of cases, the search might find a document that has been deleted but not yet refreshed
        // in that node's shard.
        // Our assertion, therefore, is that an attempt to invalidate the refresh token must not actually invalidate
        // anything (concurrency controls must prevent that), nor may return any errors,
        // but it might _temporarily_ find an "already deleted" token.
        InvalidateTokenResponse invalidateRefreshTokenResponse = securityClient.prepareInvalidateToken(refreshToken)
            .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
            .execute()
            .actionGet();
        assertThat(invalidateRefreshTokenResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateRefreshTokenResponse.getResult().getPreviouslyInvalidatedTokens(), empty());

        // 99% of the time, this will already be empty, but if not ensure it goes to empty within the allowed timeframe
        if (false == invalidateRefreshTokenResponse.getResult().getErrors().isEmpty()) {
            assertBusy(() -> {
                InvalidateTokenResponse newResponse = securityClient.prepareInvalidateToken(refreshToken)
                    .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                    .execute()
                    .actionGet();
                assertThat(newResponse.getResult().getErrors(), empty());
            });
        }
    }

    public void testAccessTokenAndRefreshTokenCanBeInvalidatedIndependently() {
        CreateTokenResponse response = securityClient().prepareCreateToken().setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        final InvalidateTokenResponse response1, response2;
        if (randomBoolean()) {
            response1 = securityClient().prepareInvalidateToken(response.getTokenString())
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .execute().actionGet();
            response2 = securityClient().prepareInvalidateToken(response.getRefreshToken())
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .execute().actionGet();
        } else {
            response1 = securityClient().prepareInvalidateToken(response.getRefreshToken())
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .execute().actionGet();
            response2 = securityClient().prepareInvalidateToken(response.getTokenString())
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .execute().actionGet();
        }

        assertThat(response1.getResult().getInvalidatedTokens(), hasSize(1));
        assertThat(response1.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(response1.getResult().getErrors(), empty());

        assertThat(response2.getResult().getInvalidatedTokens(), hasSize(1));
        assertThat(response2.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(response2.getResult().getErrors(), empty());
    }

    public void testInvalidateAllTokensForUser() throws Exception {
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            securityClient().prepareCreateToken()
                .setGrantType("password")
                .setUsername(SecuritySettingsSource.TEST_USER_NAME)
                .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
                .get();
        }
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClientSuperuser = new SecurityClient(client);
        InvalidateTokenResponse invalidateResponse = securityClientSuperuser
            .prepareInvalidateToken()
            .setUserName(SecuritySettingsSource.TEST_USER_NAME)
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
    }

    public void testInvalidateAllTokensForRealm() throws Exception{
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            securityClient().prepareCreateToken()
                .setGrantType("password")
                .setUsername(SecuritySettingsSource.TEST_USER_NAME)
                .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
                .get();
        }
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClientSuperuser = new SecurityClient(client);
        InvalidateTokenResponse invalidateResponse = securityClientSuperuser
            .prepareInvalidateToken()
            .setRealmName("file")
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
    }

    public void testInvalidateAllTokensForRealmThatHasNone() {
        final int numOfRequests = randomIntBetween(2, 4);
        for (int i = 0; i < numOfRequests; i++) {
            securityClient().prepareCreateToken()
                .setGrantType("password")
                .setUsername(SecuritySettingsSource.TEST_USER_NAME)
                .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
                .get();
        }
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClientSuperuser = new SecurityClient(client);
        InvalidateTokenResponse invalidateResponse = securityClientSuperuser
            .prepareInvalidateToken()
            .setRealmName("saml")
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
    }

    public void testExpireMultipleTimes() {
        CreateTokenResponse response = securityClient().prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();

        InvalidateTokenResponse invalidateResponse = securityClient()
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
        InvalidateTokenResponse invalidateAgainResponse = securityClient()
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateAgainResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateAgainResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateAgainResponse.getResult().getErrors(), empty());
    }

    public void testInvalidateMultipleTimes() {
        CreateTokenResponse response = securityClient().prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        InvalidateTokenResponse invalidateResponse = securityClient()
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
        InvalidateTokenResponse invalidateAgainResponse = securityClient()
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateAgainResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateAgainResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateAgainResponse.getResult().getErrors(), empty());
    }

    public void testInvalidateNotValidAccessTokens() throws Exception {
        // Perform a request to invalidate a token, before the tokens index is created
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> securityClient()
            .prepareInvalidateToken(generateAccessToken(Version.CURRENT))
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get());
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        // Create a token to trigger index creation
        securityClient().prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();

        InvalidateTokenResponse invalidateResponse =
            securityClient()
                .prepareInvalidateToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it")
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        invalidateResponse =
            securityClient()
                .prepareInvalidateToken("10we+might+assume+this+is+valid+old+token")
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        invalidateResponse =
            securityClient()
                .prepareInvalidateToken(generateInvalidShortAccessToken(Version.CURRENT))
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse =
            securityClient()
                .prepareInvalidateToken(generateAccessToken(Version.CURRENT))
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());
    }

    public void testInvalidateNotValidRefreshTokens() throws Exception {
        // Perform a request to invalidate a refresh token, before the tokens index is created
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> securityClient()
            .prepareInvalidateToken(TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID()))
            .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
            .get());
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        // Create a token to trigger index creation
        securityClient().prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();

        InvalidateTokenResponse invalidateResponse =
            securityClient()
                .prepareInvalidateToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it")
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        invalidateResponse =
            securityClient()
                .prepareInvalidateToken(TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, randomAlphaOfLength(32)))
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        invalidateResponse =
            securityClient()
                .prepareInvalidateToken("10we+might+assume+this+is+valid+old+token")
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse =
            securityClient()
                .prepareInvalidateToken(TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID()))
                .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

    }

    public void testRefreshingToken() {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());
        // get cluster health with token
        assertNoTimeout(client()
            .filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
            .admin().cluster().prepareHealth().get());
        CreateTokenResponse refreshResponse = securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get();
        assertNotNull(refreshResponse.getRefreshToken());
        assertNotEquals(refreshResponse.getRefreshToken(), createTokenResponse.getRefreshToken());
        assertNotEquals(refreshResponse.getTokenString(), createTokenResponse.getTokenString());

        assertNoTimeout(client().filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + refreshResponse.getTokenString()))
            .admin().cluster().prepareHealth().get());
        assertNotNull(refreshResponse.getAuthentication());
    }

    public void testRefreshingInvalidatedToken() {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());
        InvalidateTokenResponse invalidateResponse = securityClient
            .prepareInvalidateToken(createTokenResponse.getRefreshToken())
            .setType(InvalidateTokenRequest.Type.REFRESH_TOKEN)
            .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponse.getResult().getErrors(), empty());

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get());
        assertEquals("invalid_grant", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("token has been invalidated", e.getHeader("error_description").get(0));
    }

    public void testRefreshingMultipleTimesFails() throws Exception {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());
        CreateTokenResponse refreshResponse = securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get();
        assertNotNull(refreshResponse);
        // We now have two documents, the original(now refreshed) token doc and the new one with the new access doc
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                .setSource(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("doc_type", TokenService.TOKEN_DOC_TYPE))
                        .must(QueryBuilders.termQuery("refresh_token.refreshed", "true"))))
                .setSize(1)
                .setTerminateAfter(1)
                .get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });

        // hack doc to modify the refresh time to 50 seconds ago so that we don't hit the lenient refresh case
        Instant refreshed = Instant.now();
        Instant aWhileAgo = refreshed.minus(50L, ChronoUnit.SECONDS);
        assertTrue(Instant.now().isAfter(aWhileAgo));
        UpdateResponse updateResponse = client.prepareUpdate(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, SINGLE_MAPPING_NAME, docId.get())
            .setDoc("refresh_token", Collections.singletonMap("refresh_time", aWhileAgo.toEpochMilli()))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setFetchSource("refresh_token", Strings.EMPTY_STRING)
            .get();
        assertNotNull(updateResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> refreshTokenMap = (Map<String, Object>) updateResponse.getGetResult().sourceAsMap().get("refresh_token");
        assertTrue(
            Instant.ofEpochMilli((long) refreshTokenMap.get("refresh_time")).isBefore(Instant.now().minus(30L, ChronoUnit.SECONDS)));
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get());
        assertEquals("invalid_grant", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("token has already been refreshed more than 30 seconds in the past", e.getHeader("error_description").get(0));
    }

    public void testRefreshingMultipleTimesWithinWindowSucceeds() throws Exception {
        final Clock clock = Clock.systemUTC();
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
        final List<RestStatus> authStatuses = Collections.synchronizedList(new ArrayList<>());
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween((numberOfProcessors + 1) / 2, numberOfProcessors * 3);
        List<Thread> threads = new ArrayList<>(numberOfThreads);
        final CountDownLatch readyLatch = new CountDownLatch(numberOfThreads + 1);
        final CountDownLatch completedLatch = new CountDownLatch(numberOfThreads);
        AtomicBoolean failed = new AtomicBoolean();
        final Instant t1 = clock.instant();
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread(() -> {
                // Each thread gets its own client so that more than one nodes will be hit
                Client threadClient = client().filterWithHeader(Collections.singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
                SecurityClient threadSecurityClient = new SecurityClient(threadClient);
                CreateTokenRequest refreshRequest =
                    threadSecurityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).request();
                readyLatch.countDown();
                try {
                    readyLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    completedLatch.countDown();
                    return;
                }
                threadSecurityClient.refreshToken(refreshRequest, ActionListener.wrap(result -> {
                    final Instant t2 = clock.instant();
                    if (t1.plusSeconds(30L).isBefore(t2)){
                        logger.warn("Tokens [{}], [{}] were received more than 30 seconds after the request, not checking them",
                            result.getTokenString(), result.getRefreshToken());
                    } else {
                        tokens.add(result.getTokenString() + result.getRefreshToken());
                        threadClient.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + result.getTokenString()))
                            .admin().cluster().health(new ClusterHealthRequest(), ActionListener.wrap(
                            r -> authStatuses.add(RestStatus.OK),
                            e -> authStatuses.add(RestStatus.BAD_REQUEST)));
                    }
                    logger.info("received access token [{}] and refresh token [{}]", result.getTokenString(), result.getRefreshToken());
                    completedLatch.countDown();
                }, e -> {
                    failed.set(true);
                    completedLatch.countDown();
                    logger.error("caught exception", e);
                }));
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        readyLatch.countDown();
        readyLatch.await();
        for (Thread thread : threads) {
            thread.join();
        }
        completedLatch.await();
        assertThat(failed.get(), equalTo(false));
        // Assert that we only ever got one token/refresh_token pair
        synchronized (tokens) {
            assertThat((int) tokens.stream().distinct().count(), equalTo(1));
        }
        // Assert that all requests from all threads could authenticate at the time they received the access token
        // see: https://github.com/elastic/elasticsearch/issues/54289
        synchronized (authStatuses) {
            assertThat((int) authStatuses.stream().distinct().count(), equalTo(1));
            assertThat(authStatuses, hasItem(RestStatus.OK));
        }
    }

    public void testRefreshAsDifferentUser() {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
            () -> new SecurityClient(client()
                .filterWithHeader(Collections.singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING))))
                .prepareRefreshToken(createTokenResponse.getRefreshToken()).get());
        assertEquals("invalid_grant", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("tokens must be refreshed by the creating client", e.getHeader("error_description").get(0));
    }

    public void testCreateThenRefreshAsDifferentUser() {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("password")
            .setUsername(SecuritySettingsSource.TEST_USER_NAME)
            .setPassword(new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()))
            .get();
        assertNotNull(createTokenResponse.getRefreshToken());

        CreateTokenResponse refreshResponse = securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get();
        assertNotEquals(refreshResponse.getTokenString(), createTokenResponse.getTokenString());
        assertNotEquals(refreshResponse.getRefreshToken(), createTokenResponse.getRefreshToken());

        PlainActionFuture<AuthenticateResponse> authFuture = new PlainActionFuture<>();
        AuthenticateRequest request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_SUPERUSER);
        client.execute(AuthenticateAction.INSTANCE, request, authFuture);
        AuthenticateResponse response = authFuture.actionGet();
        assertEquals(SecuritySettingsSource.TEST_SUPERUSER, response.authentication().getUser().principal());
        assertEquals(Authentication.AuthenticationType.REALM, response.authentication().getAuthenticationType());

        authFuture = new PlainActionFuture<>();
        request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_USER_NAME);
        client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
            .execute(AuthenticateAction.INSTANCE, request, authFuture);
        response = authFuture.actionGet();
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.authentication().getUser().principal());
        assertEquals(Authentication.AuthenticationType.TOKEN, response.authentication().getAuthenticationType());

        authFuture = new PlainActionFuture<>();
        request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_USER_NAME);
        client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + refreshResponse.getTokenString()))
            .execute(AuthenticateAction.INSTANCE, request, authFuture);
        response = authFuture.actionGet();
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.authentication().getUser().principal());
        assertEquals(Authentication.AuthenticationType.TOKEN, response.authentication().getAuthenticationType());
    }

    public void testClientCredentialsGrant() throws Exception {
        Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse createTokenResponse = securityClient.prepareCreateToken()
            .setGrantType("client_credentials")
            .get();
        assertNull(createTokenResponse.getRefreshToken());

        AuthenticateRequest request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_SUPERUSER);
        PlainActionFuture<AuthenticateResponse> authFuture = new PlainActionFuture<>();
        client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
            .execute(AuthenticateAction.INSTANCE, request, authFuture);
        AuthenticateResponse response = authFuture.get();
        assertEquals(SecuritySettingsSource.TEST_SUPERUSER, response.authentication().getUser().principal());
        assertEquals(Authentication.AuthenticationType.TOKEN, response.authentication().getAuthenticationType());

        // invalidate
        PlainActionFuture<InvalidateTokenResponse> invalidateResponseFuture = new PlainActionFuture<>();
        InvalidateTokenRequest invalidateTokenRequest =
            new InvalidateTokenRequest(createTokenResponse.getTokenString(), InvalidateTokenRequest.Type.ACCESS_TOKEN.getValue());
        securityClient.invalidateToken(invalidateTokenRequest, invalidateResponseFuture);
        assertThat(invalidateResponseFuture.get().getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponseFuture.get().getResult().getPreviouslyInvalidatedTokens(), empty());
        assertThat(invalidateResponseFuture.get().getResult().getErrors(), empty());

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<AuthenticateResponse> responseFuture = new PlainActionFuture<>();
            client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
                .execute(AuthenticateAction.INSTANCE, request, responseFuture);
            responseFuture.actionGet();
        });
    }

    public void testAuthenticateWithWrongToken() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        org.elasticsearch.client.security.CreateTokenResponse response = restClient.security().createToken(
            org.elasticsearch.client.security.CreateTokenRequest.passwordGrant(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(response.getAccessToken());
        // First authenticate with token
        RequestOptions correctAuthOptions =
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + response.getAccessToken()).build();
        org.elasticsearch.client.security.AuthenticateResponse validResponse = restClient.security().authenticate(correctAuthOptions);
        assertThat(validResponse.getUser().getUsername(), equalTo(SecuritySettingsSource.TEST_USER_NAME));
        // Now attempt to authenticate with an invalid access token string
        RequestOptions wrongAuthOptions =
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + randomAlphaOfLengthBetween(0, 128)).build();
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> restClient.security().authenticate(wrongAuthOptions));
        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
        // Now attempt to authenticate with an invalid access token with valid structure (pre 7.2)
        RequestOptions wrongAuthOptionsPre72 =
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + generateAccessToken(Version.V_7_1_0)).build();
        ElasticsearchStatusException e1 = expectThrows(ElasticsearchStatusException.class,
            () -> restClient.security().authenticate(wrongAuthOptionsPre72));
        assertThat(e1.status(), equalTo(RestStatus.UNAUTHORIZED));
        // Now attempt to authenticate with an invalid access token with valid structure (after 7.2)
        RequestOptions wrongAuthOptionsAfter72 =
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + generateAccessToken(Version.V_7_4_0)).build();
        ElasticsearchStatusException e2 = expectThrows(ElasticsearchStatusException.class,
            () -> restClient.security().authenticate(wrongAuthOptionsAfter72));
        assertThat(e2.status(), equalTo(RestStatus.UNAUTHORIZED));
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() throws Exception {
        // get the token service and wait until token expiration is not in progress!
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            assertBusy(() -> assertFalse(tokenService.isExpirationInProgress()));
        }
        super.deleteSecurityIndex();
    }

    public void testMetadataIsNotSentToClient() {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setCustoms(true).get();
        assertFalse(clusterStateResponse.getState().customs().containsKey(TokenMetadata.TYPE));
    }

    private String generateAccessToken(Version version) throws Exception {
        TokenService tokenService = internalCluster().getInstance(TokenService.class);
        String accessTokenString = UUIDs.randomBase64UUID();
        return tokenService.prependVersionAndEncodeAccessToken(version, accessTokenString);
    }

    private String generateInvalidShortAccessToken(Version version) throws Exception {
        TokenService tokenService = internalCluster().getInstance(TokenService.class);
        String accessTokenString = randomAlphaOfLength(32); // UUIDs are 36
        return tokenService.prependVersionAndEncodeAccessToken(version, accessTokenString);
    }
}
