/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.directory.api.util.Strings;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
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

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

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
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(response.getAuthentication());
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getAccessToken(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }
        // start a new node and see if it can decrypt the token
        String nodeName = internalCluster().startNode();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getAccessToken(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }

        TokenService tokenService = internalCluster().getInstance(TokenService.class, nodeName);
        PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
        tokenService.decodeToken(response.getAccessToken(), userTokenFuture);
        assertNotNull(userTokenFuture.actionGet());
    }


    public void testTokenServiceCanRotateKeys() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        String masterName = internalCluster().getMasterName();
        TokenService masterTokenService = internalCluster().getInstance(TokenService.class, masterName);
        String activeKeyHash = masterTokenService.getActiveKeyHash();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(response.getAccessToken(), userTokenFuture);
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
            tokenService.decodeToken(response.getAccessToken(), userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
            assertNotEquals(activeKeyHash, tokenService.getActiveKeyHash());
        }
        assertNotNull(response.getAuthentication());
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.getAuthentication().getUser().getUsername());
    }

    public void testExpiredTokensDeletedAfterExpiration() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        final String accessToken = response.getAccessToken();
        final String refreshToken = response.getRefreshToken();
        Instant created = Instant.now();

        InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(
            InvalidateTokenRequest.accessToken(accessToken), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = restClient.search(new SearchRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                    .source(SearchSourceBuilder.searchSource()
                        .size(1)
                        .terminateAfter(1)
                        .query(QueryBuilders.termQuery("doc_type", "token"))), SECURITY_REQUEST_OPTIONS);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });

        // hack doc to modify the creation time to the day before
        Instant yesterday = created.minus(36L, ChronoUnit.HOURS);
        assertTrue(Instant.now().isAfter(yesterday));
        restClient.update(new UpdateRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, docId.get())
            .doc("creation_time", yesterday.toEpochMilli())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), SECURITY_REQUEST_OPTIONS);

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                // invalidate a invalid token... doesn't matter that it is bad... we just want this action to trigger the deletion
                InvalidateTokenResponse invalidateResponseTwo = restClient.security()
                    .invalidateToken(InvalidateTokenRequest.accessToken("fooobar"),
                        SECURITY_REQUEST_OPTIONS);
                assertThat(invalidateResponseTwo.getInvalidatedTokens(), equalTo(0));
                assertThat(invalidateResponseTwo.getPreviouslyInvalidatedTokens(), equalTo(0));
                assertThat(invalidateResponseTwo.getErrors(), empty());
            }
            restClient.indices().refresh(new RefreshRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS), SECURITY_REQUEST_OPTIONS);
            SearchResponse searchResponse = restClient.search(new SearchRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.termQuery("doc_type", "token")).terminateAfter(1)), SECURITY_REQUEST_OPTIONS);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        }, 30, TimeUnit.SECONDS);

        // Now the documents are deleted, try to invalidate the access token and refresh token again
        InvalidateTokenResponse invalidateAccessTokenResponse = restClient.security().invalidateToken(
            InvalidateTokenRequest.accessToken(accessToken), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateAccessTokenResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateAccessTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateAccessTokenResponse.getErrors(), empty());

        // Weird testing behaviour ahead...
        // invalidating by access token (above) is a Get, but invalidating by refresh token (below) is a Search
        // In a multi node cluster, in a small % of cases, the search might find a document that has been invalidated but not yet deleted
        // from that node's shard.
        // Our assertion, therefore, is that an attempt to invalidate the (already invalidated) refresh token must not actually invalidate
        // anything (concurrency controls must prevent that), nor may return any errors,
        // but it might _temporarily_ find an "already invalidated" token.
        final InvalidateTokenRequest invalidateRefreshTokenRequest = InvalidateTokenRequest.refreshToken(refreshToken);
        InvalidateTokenResponse invalidateRefreshTokenResponse = restClient.security().invalidateToken(
            invalidateRefreshTokenRequest, SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateRefreshTokenResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateRefreshTokenResponse.getErrors(), empty());

        // 99% of the time, this will already be zero, but if not ensure it goes to zero within the allowed timeframe
        if (invalidateRefreshTokenResponse.getPreviouslyInvalidatedTokens() > 0) {
            assertBusy(() -> {
                var newResponse = restClient.security().invalidateToken(invalidateRefreshTokenRequest, SECURITY_REQUEST_OPTIONS);
                assertThat(newResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
            });
        }
    }

    public void testInvalidateAllTokensForUser() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            restClient.security().createToken(CreateTokenRequest.passwordGrant(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        }
        InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(
            InvalidateTokenRequest.userTokens(SecuritySettingsSource.TEST_USER_NAME),
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build());
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
    }

    public void testInvalidateAllTokensForRealm() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            restClient.security().createToken(CreateTokenRequest.passwordGrant(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        }
        InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(
            InvalidateTokenRequest.realmTokens("file"),
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build());
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
    }

    public void testInvalidateAllTokensForRealmThatHasNone() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final int numOfRequests = randomIntBetween(2, 4);
        for (int i = 0; i < numOfRequests; i++) {
            restClient.security().createToken(CreateTokenRequest.passwordGrant(SecuritySettingsSource.TEST_USER_NAME,
                SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        }
        InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(
            InvalidateTokenRequest.realmTokens("saml"),
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build());
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
    }

    public void testInvalidateMultipleTimes() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);

        InvalidateTokenResponse invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(response.getAccessToken()), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
        InvalidateTokenResponse invalidateAgainResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(response.getAccessToken()), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateAgainResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateAgainResponse.getPreviouslyInvalidatedTokens(), equalTo(1));
        assertThat(invalidateAgainResponse.getErrors(), empty());
    }

    public void testInvalidateNotValidAccessTokens() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        // Perform a request to invalidate a token, before the tokens index is created
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(generateAccessToken(Version.CURRENT)),
                SECURITY_REQUEST_OPTIONS));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        // Create a token to trigger index creation
        restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        InvalidateTokenResponse invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it"),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken("10we+might+assume+this+is+valid+old+token"), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(generateInvalidShortAccessToken(Version.CURRENT)),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(generateAccessToken(Version.CURRENT)),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
    }

    public void testInvalidateNotValidRefreshTokens() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        // Perform a request to invalidate a refresh token, before the tokens index is created
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken(
                TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID())),
                SECURITY_REQUEST_OPTIONS));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        // Create a token to trigger index creation
        restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        InvalidateTokenResponse invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it"),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken("10we+might+assume+this+is+valid+old+token"), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken(
                TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, randomAlphaOfLength(32))),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken(
                    TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID())),
                SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());
    }

    public void testRefreshingToken() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(response.getRefreshToken());
        // Assert that we can authenticate with the access token
        assertAuthenticateWithToken(response.getAccessToken(), SecuritySettingsSource.TEST_USER_NAME);
        CreateTokenResponse refreshResponse = restClient.security()
            .createToken(CreateTokenRequest.refreshTokenGrant(response.getRefreshToken()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(refreshResponse.getRefreshToken());
        assertNotEquals(refreshResponse.getRefreshToken(), response.getRefreshToken());
        assertNotEquals(refreshResponse.getAccessToken(), response.getAccessToken());

        // Assert that we can authenticate with the refreshed access token
        assertAuthenticateWithToken(refreshResponse.getAccessToken(), SecuritySettingsSource.TEST_USER_NAME);
        assertNotNull(refreshResponse.getAuthentication());
    }

    public void testRefreshingInvalidatedToken() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(createTokenResponse.getRefreshToken());
        InvalidateTokenResponse invalidateResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.refreshToken(createTokenResponse.getRefreshToken()), SECURITY_REQUEST_OPTIONS);
        assertThat(invalidateResponse.getInvalidatedTokens(), equalTo(1));
        assertThat(invalidateResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateResponse.getErrors(), empty());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> restClient.security().createToken(
                    CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()), SECURITY_REQUEST_OPTIONS));
        assertThat(e.getCause().getMessage(), containsString("invalid_grant"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getCause().getMessage(), containsString("token has been invalidated"));
    }

    public void testRefreshingMultipleTimesFails() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(createTokenResponse.getRefreshToken());

        CreateTokenResponse refreshResponse = restClient.security()
            .createToken(CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(refreshResponse);
        // We now have two documents, the original(now refreshed) token doc and the new one with the new access doc
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = restClient.search(new SearchRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS)
                .source(SearchSourceBuilder.searchSource()
                    .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("doc_type", TokenService.TOKEN_DOC_TYPE))
                        .must(QueryBuilders.termQuery("refresh_token.refreshed", "true")))
                    .size(1)
                    .terminateAfter(1)), SECURITY_REQUEST_OPTIONS);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
            docId.set(searchResponse.getHits().getAt(0).getId());
        });

        // hack doc to modify the refresh time to 50 seconds ago so that we don't hit the lenient refresh case
        Instant refreshed = Instant.now();
        Instant aWhileAgo = refreshed.minus(50L, ChronoUnit.SECONDS);
        assertTrue(Instant.now().isAfter(aWhileAgo));
        UpdateResponse updateResponse = restClient.update(new UpdateRequest(RestrictedIndicesNames.SECURITY_TOKENS_ALIAS, docId.get())
            .doc("refresh_token", Collections.singletonMap("refresh_time", aWhileAgo.toEpochMilli()))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .fetchSource("refresh_token", Strings.EMPTY_STRING), SECURITY_REQUEST_OPTIONS);
        assertNotNull(updateResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> refreshTokenMap = (Map<String, Object>) updateResponse.getGetResult().sourceAsMap().get("refresh_token");
        assertTrue(
            Instant.ofEpochMilli((long) refreshTokenMap.get("refresh_time")).isBefore(Instant.now().minus(30L, ChronoUnit.SECONDS)));
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> restClient.security()
            .createToken(CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()), SECURITY_REQUEST_OPTIONS));
        assertThat(e.getCause().getMessage(), containsString("invalid_grant"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getCause().getMessage(), containsString("token has already been refreshed more than 30 seconds in the past"));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/55816")
    public void testRefreshingMultipleTimesWithinWindowSucceeds() throws Exception {
        final Clock clock = Clock.systemUTC();
        final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
        final List<RestStatus> authStatuses = Collections.synchronizedList(new ArrayList<>());
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
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
                readyLatch.countDown();
                try {
                    readyLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    completedLatch.countDown();
                    return;
                }

                try {
                    // safe to use same rest client across threads since it round robins between nodes
                    CreateTokenResponse result = restClient.security()
                        .createToken(CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()), SECURITY_REQUEST_OPTIONS);
                    final Instant t2 = clock.instant();
                    if (t1.plusSeconds(30L).isBefore(t2)) {
                        logger.warn("Tokens [{}], [{}] were received more than 30 seconds after the request, not checking them",
                            result.getAccessToken(), result.getRefreshToken());
                    } else {
                        authStatuses.add(getAuthenticationResponseCode(result.getAccessToken()));
                        tokens.add(result.getAccessToken() + result.getRefreshToken());
                    }
                    logger.info("received access token [{}] and refresh token [{}]", result.getAccessToken(), result.getRefreshToken());
                    completedLatch.countDown();
                } catch (IOException e) {
                    failed.set(true);
                    completedLatch.countDown();
                    logger.error("caught exception", e);
                }
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

    public void testRefreshAsDifferentUser() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(createTokenResponse.getRefreshToken());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> restClient.security().createToken(CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()),
                    RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                        UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build()));
        assertThat(e.getCause().getMessage(), containsString("invalid_grant"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertThat(e.getCause().getMessage(), containsString("tokens must be refreshed by the creating client"));
    }

    public void testCreateThenRefreshAsDifferentUser() throws IOException {
        final RequestOptions superuserOptions = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build();
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), superuserOptions);
        assertNotNull(createTokenResponse.getRefreshToken());

        CreateTokenResponse refreshResponse = restClient.security()
            .createToken(CreateTokenRequest.refreshTokenGrant(createTokenResponse.getRefreshToken()), superuserOptions);
        assertNotEquals(refreshResponse.getAccessToken(), createTokenResponse.getAccessToken());
        assertNotEquals(refreshResponse.getRefreshToken(), createTokenResponse.getRefreshToken());

        AuthenticateResponse response = restClient.security().authenticate(superuserOptions);

        assertEquals(SecuritySettingsSource.TEST_SUPERUSER, response.getUser().getUsername());
        assertEquals("realm", response.getAuthenticationType());

        assertAuthenticateWithToken(createTokenResponse.getAccessToken(), SecuritySettingsSource.TEST_USER_NAME);
        assertAuthenticateWithToken(refreshResponse.getAccessToken(), SecuritySettingsSource.TEST_USER_NAME);
    }

    public void testClientCredentialsGrant() throws Exception {
        final RequestOptions superuserOptions = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)).build();
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse createTokenResponse =
            restClient.security().createToken(CreateTokenRequest.clientCredentialsGrant(), superuserOptions);
        assertNull(createTokenResponse.getRefreshToken());

        assertAuthenticateWithToken(createTokenResponse.getAccessToken(), SecuritySettingsSource.TEST_SUPERUSER);

        // invalidate
        InvalidateTokenResponse invalidateTokenResponse = restClient.security()
            .invalidateToken(InvalidateTokenRequest.accessToken(createTokenResponse.getAccessToken()), superuserOptions);
        assertThat(invalidateTokenResponse.getInvalidatedTokens(), equalTo(1));
        assertThat(invalidateTokenResponse.getPreviouslyInvalidatedTokens(), equalTo(0));
        assertThat(invalidateTokenResponse.getErrors(), empty());

       assertUnauthorizedToken(createTokenResponse.getAccessToken());
    }

    public void testAuthenticateWithWrongToken() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        CreateTokenResponse response = restClient.security().createToken(CreateTokenRequest.passwordGrant(
            SecuritySettingsSource.TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()), SECURITY_REQUEST_OPTIONS);
        assertNotNull(response.getRefreshToken());
        // Assert that we can authenticate with the access token
        assertAuthenticateWithToken(response.getAccessToken(), SecuritySettingsSource.TEST_USER_NAME);
        // Now attempt to authenticate with an invalid access token string
        assertUnauthorizedToken(randomAlphaOfLengthBetween(0, 128));
        // Now attempt to authenticate with an invalid access token with valid structure (pre 7.2)
        assertUnauthorizedToken(generateAccessToken(Version.V_7_1_0));
        // Now attempt to authenticate with an invalid access token with valid structure (after 7.2)
        assertUnauthorizedToken(generateAccessToken(Version.V_7_4_0));
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

    private void assertAuthenticateWithToken(String accessToken, String expectedUser) throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        AuthenticateResponse authResponse = restClient.security().authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            "Bearer " + accessToken).build());
        assertThat(authResponse.getUser().getUsername(), equalTo(expectedUser));
        assertThat(authResponse.getAuthenticationType(), equalTo("token"));
    }

    private void assertUnauthorizedToken(String accessToken) {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> restClient.security().authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                "Bearer " + accessToken).build()));
        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
    }

    private RestStatus getAuthenticationResponseCode(String accessToken) throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        try {
            restClient.security().authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                "Bearer " + accessToken).build());
            return RestStatus.OK;
        } catch (ElasticsearchStatusException esse) {
            return esse.status();
        }
    }
}
