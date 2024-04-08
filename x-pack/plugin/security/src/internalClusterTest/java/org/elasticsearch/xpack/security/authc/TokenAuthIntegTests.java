/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.TestSecurityClient.OAuth2Token;
import org.elasticsearch.test.TestSecurityClient.TokenInvalidation;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.TestMatchers.hasStatusCode;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TokenAuthIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // crank up the deletion interval and set timeout for delete requests
            .put(TokenService.DELETE_INTERVAL.getKey(), TimeValue.timeValueMillis(200L))
            .put(TokenService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5L))
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            // used to diagnose Token authn failures, see: https://github.com/elastic/elasticsearch/issues/85697
            .put("logger.org.elasticsearch.xpack.security.authc.TokenService", "TRACE")
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
        final OAuth2Token token = createToken(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(token.accessToken(), false, userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }
        // start a new node and see if it can decrypt the token
        String nodeName = internalCluster().startNode();
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(token.accessToken(), false, userTokenFuture);
            assertNotNull(userTokenFuture.actionGet());
        }

        TokenService tokenService = internalCluster().getInstance(TokenService.class, nodeName);
        PlainActionFuture<UserToken> userTokenFuture = new PlainActionFuture<>();
        tokenService.decodeToken(token.accessToken(), false, userTokenFuture);
        assertNotNull(userTokenFuture.actionGet());
    }

    public void testExpiredTokensDeletedAfterExpiration() throws Exception {
        OAuth2Token response = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        final String accessToken = response.accessToken();
        final String refreshToken = response.getRefreshToken();
        Instant created = Instant.now();

        TokenInvalidation invalidateResponse = invalidateAccessToken(accessToken);
        assertThat(invalidateResponse.invalidated(), equalTo(1));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            Request searchRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_search");
            searchRequest.setOptions(SECURITY_REQUEST_OPTIONS);
            searchRequest.setJsonEntity("""
                {
                    "query" : {
                        "term" : {
                            "doc_type" : "token"
                        }
                    },
                    "terminate_after" : 1,
                    "size" : 1
                }
                """);
            Response searchResponse = getRestClient().performRequest(searchRequest);
            ObjectPath path = ObjectPath.createFromResponse(searchResponse);
            assertThat(path.evaluate("hits.total.value"), equalTo(1));
            final List<Map<String, ?>> hits = path.evaluate("hits.hits");
            final String id = ObjectPath.evaluate(hits.get(0), "_id");
            assertThat(id, notNullValue());
            docId.set(id);
        });

        // hack doc to modify the creation time to the day before
        Instant yesterday = created.minus(36L, ChronoUnit.HOURS);
        assertTrue(Instant.now().isAfter(yesterday));
        Request updateRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_update/" + docId.get());
        updateRequest.addParameter("refresh", WriteRequest.RefreshPolicy.IMMEDIATE.getValue());
        updateRequest.setOptions(SECURITY_REQUEST_OPTIONS);
        updateRequest.setJsonEntity(Strings.format("""
            {
              "doc": {
                "creation_time": %s
              }
            }
            """, yesterday.toEpochMilli()));
        getRestClient().performRequest(updateRequest);

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                // invalidate a invalid token... doesn't matter that it is bad... we just want this action to trigger the deletion
                TokenInvalidation invalidateResponseTwo = invalidateAccessToken("fooobar");
                assertThat(invalidateResponseTwo.invalidated(), equalTo(0));
                assertThat(invalidateResponseTwo.previouslyInvalidated(), equalTo(0));
                assertThat(invalidateResponseTwo.errors(), empty());
            }
            Request refreshRequest = new Request("POST", "/" + SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_refresh");
            refreshRequest.setOptions(SECURITY_REQUEST_OPTIONS);
            getRestClient().performRequest(refreshRequest);

            Request searchRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_search");
            searchRequest.setOptions(SECURITY_REQUEST_OPTIONS);
            searchRequest.setJsonEntity("""
                {
                    "query" : {
                        "term" : {
                            "doc_type" : "token"
                        }
                    },
                    "terminate_after" : 1,
                    "size" : 1
                }
                """);
            Response searchResponse = getRestClient().performRequest(searchRequest);
            assertThat(ObjectPath.createFromResponse(searchResponse).evaluate("hits.total.value"), equalTo(0));
        }, 30, TimeUnit.SECONDS);

        // Weird testing behaviour ahead...
        // In a multi node cluster, invalidating by access token (get) or refresh token (search) can both,
        // in a small % of cases, find a document that has been deleted but not yet refreshed
        // in that node's shard.
        // Our assertion, therefore, is that an attempt to invalidate the token must not actually invalidate
        // anything (concurrency controls must prevent that), nor may return any errors,
        // but it might _temporarily_ find an "already deleted" token.

        // Now the documents are deleted, try to invalidate the access token and refresh token again
        TokenInvalidation invalidateAccessTokenResponse = invalidateAccessToken(accessToken);
        assertThat(invalidateAccessTokenResponse.invalidated(), equalTo(0));
        assertThat(invalidateAccessTokenResponse.previouslyInvalidated(), equalTo(0));

        // 99% of the time, this will already be empty, but if not ensure it goes to empty within the allowed timeframe
        if (false == invalidateAccessTokenResponse.errors().isEmpty()) {
            assertBusy(() -> {
                var newResponse = invalidateAccessToken(accessToken);
                assertThat(newResponse.errors(), empty());
            });
        }

        TokenInvalidation invalidateRefreshTokenResponse = invalidateRefreshToken(refreshToken);
        assertThat(invalidateRefreshTokenResponse.invalidated(), equalTo(0));
        assertThat(invalidateRefreshTokenResponse.previouslyInvalidated(), equalTo(0));

        // 99% of the time, this will already be empty, but if not ensure it goes to empty within the allowed timeframe
        if (false == invalidateRefreshTokenResponse.errors().isEmpty()) {
            assertBusy(() -> {
                var newResponse = invalidateRefreshToken(refreshToken);
                assertThat(newResponse.errors(), empty());
            });
        }
    }

    public void testAccessTokenAndRefreshTokenCanBeInvalidatedIndependently() throws IOException {
        final OAuth2Token response = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        final CheckedSupplier<TokenInvalidation, IOException> invalidateRequest1, invalidateRequest2;
        if (randomBoolean()) {
            invalidateRequest1 = () -> invalidateAccessToken(response.accessToken());
            invalidateRequest2 = () -> invalidateRefreshToken(response.getRefreshToken());
        } else {
            invalidateRequest1 = () -> invalidateRefreshToken(response.getRefreshToken());
            invalidateRequest2 = () -> invalidateAccessToken(response.accessToken());
        }

        final TokenInvalidation response1 = invalidateRequest1.get();
        assertThat(response1.invalidated(), equalTo(1));
        assertThat(response1.previouslyInvalidated(), equalTo(0));
        assertThat(response1.errors(), empty());

        final TokenInvalidation response2 = invalidateRequest2.get();
        assertThat(response2.invalidated(), equalTo(1));
        assertThat(response2.previouslyInvalidated(), equalTo(0));
        assertThat(response2.errors(), empty());
    }

    public void testInvalidateAllTokensForUser() throws Exception {
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        }
        TokenInvalidation invalidateResponse = super.getSecurityClient(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(
                        SecuritySettingsSource.ES_TEST_ROOT_USER,
                        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                    )
                )
                .build()
        ).invalidateTokensForUser(TEST_USER_NAME);
        assertThat(invalidateResponse.invalidated(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
    }

    public void testInvalidateAllTokensForRealm() throws Exception {
        final int numOfRequests = randomIntBetween(5, 10);
        for (int i = 0; i < numOfRequests; i++) {
            createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        }
        final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
            .build();
        TokenInvalidation invalidateResponse = super.getSecurityClient(requestOptions).invalidateTokensForRealm("file");
        assertThat(invalidateResponse.invalidated(), equalTo(2 * (numOfRequests)));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
    }

    public void testInvalidateAllTokensForRealmThatHasNone() throws IOException {
        final int numOfRequests = randomIntBetween(2, 4);
        for (int i = 0; i < numOfRequests; i++) {
            createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        }
        final RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING))
            .build();
        TokenInvalidation invalidateResponse = super.getSecurityClient(requestOptions).invalidateTokensForRealm("saml");
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
    }

    public void testInvalidateMultipleTimes() throws IOException {
        OAuth2Token response = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

        TokenInvalidation invalidateResponse = invalidateAccessToken(response.accessToken());
        assertThat(invalidateResponse.invalidated(), equalTo(1));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
        TokenInvalidation invalidateAgainResponse = invalidateAccessToken((response.accessToken()));
        assertThat(invalidateAgainResponse.invalidated(), equalTo(0));
        assertThat(invalidateAgainResponse.previouslyInvalidated(), equalTo(1));
        assertThat(invalidateAgainResponse.errors(), empty());
    }

    public void testInvalidateNotValidAccessTokens() throws Exception {
        final TokenService tokenService = internalCluster().getInstance(TokenService.class);
        // Perform a request to invalidate a token, before the tokens index is created
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> invalidateAccessToken(
                tokenService.prependVersionAndEncodeAccessToken(
                    TransportVersion.current(),
                    tokenService.getRandomTokenBytes(TransportVersion.current(), randomBoolean()).v1()
                )
            )
        );
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        e = expectThrows(
            ResponseException.class,
            () -> invalidateAccessToken(
                tokenService.prependVersionAndEncodeAccessToken(
                    TransportVersions.V_7_3_2,
                    tokenService.getRandomTokenBytes(TransportVersions.V_7_3_2, randomBoolean()).v1()
                )
            )
        );
        // Create a token to trigger index creation
        createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        TokenInvalidation invalidateResponse = invalidateAccessToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it");
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        invalidateResponse = invalidateAccessToken(("10we+might+assume+this+is+valid+old+token"));
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        byte[] longerAccessToken = new byte[randomIntBetween(17, 24)];
        random().nextBytes(longerAccessToken);
        invalidateResponse = invalidateAccessToken(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersions.V_7_3_2, longerAccessToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        longerAccessToken = new byte[randomIntBetween(25, 32)];
        random().nextBytes(longerAccessToken);
        invalidateResponse = invalidateAccessToken(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersion.current(), longerAccessToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        byte[] shorterAccessToken = new byte[randomIntBetween(12, 15)];
        random().nextBytes(shorterAccessToken);
        invalidateResponse = invalidateAccessToken(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersions.V_7_3_2, shorterAccessToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        shorterAccessToken = new byte[randomIntBetween(16, 23)];
        random().nextBytes(shorterAccessToken);
        invalidateResponse = invalidateRefreshToken(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersion.current(), shorterAccessToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse = invalidateAccessToken(
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersion.current(),
                tokenService.getRandomTokenBytes(TransportVersion.current(), randomBoolean()).v1()
            )
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        invalidateResponse = invalidateAccessToken(
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersions.V_7_3_2,
                tokenService.getRandomTokenBytes(TransportVersions.V_7_3_2, randomBoolean()).v1()
            )
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
    }

    public void testInvalidateNotValidRefreshTokens() throws Exception {
        final TokenService tokenService = internalCluster().getInstance(TokenService.class);
        // Perform a request to invalidate a refresh token, before the tokens index is created
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> invalidateRefreshToken(
                TokenService.prependVersionAndEncodeRefreshToken(
                    TransportVersion.current(),
                    tokenService.getRandomTokenBytes(TransportVersion.current(), true).v2()
                )
            )
        );
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        e = expectThrows(
            ResponseException.class,
            () -> invalidateRefreshToken(
                TokenService.prependVersionAndEncodeRefreshToken(
                    TransportVersions.V_7_3_2,
                    tokenService.getRandomTokenBytes(TransportVersions.V_7_3_2, true).v2()
                )
            )
        );
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        // Create a token to trigger index creation
        createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        TokenInvalidation invalidateResponse = invalidateRefreshToken("!this_is_not_a_base64_string_and_we_should_fail_decoding_it");
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        invalidateResponse = invalidateRefreshToken("10we+might+assume+this+is+valid+old+token");
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        byte[] longerRefreshToken = new byte[randomIntBetween(17, 24)];
        random().nextBytes(longerRefreshToken);
        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersions.V_7_3_2, longerRefreshToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        longerRefreshToken = new byte[randomIntBetween(25, 32)];
        random().nextBytes(longerRefreshToken);
        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersion.current(), longerRefreshToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        byte[] shorterRefreshToken = new byte[randomIntBetween(12, 15)];
        random().nextBytes(shorterRefreshToken);
        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersions.V_7_3_2, shorterRefreshToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        shorterRefreshToken = new byte[randomIntBetween(16, 23)];
        random().nextBytes(shorterRefreshToken);
        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersion.current(), shorterRefreshToken)
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        // Generate a token that could be a valid token string for the version we are on, and should decode fine, but is not found in our
        // tokens index
        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(
                TransportVersion.current(),
                tokenService.getRandomTokenBytes(TransportVersion.current(), true).v2()
            )
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        invalidateResponse = invalidateRefreshToken(
            TokenService.prependVersionAndEncodeRefreshToken(
                TransportVersions.V_7_3_2,
                tokenService.getRandomTokenBytes(TransportVersions.V_7_3_2, true).v2()
            )
        );
        assertThat(invalidateResponse.invalidated(), equalTo(0));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());
    }

    public void testRefreshingToken() throws IOException {
        OAuth2Token response = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        assertNotNull(response.getRefreshToken());
        // Assert that we can authenticate with the access token
        assertAuthenticateWithToken(response.accessToken(), TEST_USER_NAME);
        OAuth2Token refreshResponse = refreshToken(response.getRefreshToken());
        assertNotNull(refreshResponse.getRefreshToken());
        assertNotEquals(refreshResponse.getRefreshToken(), response.getRefreshToken());
        assertNotEquals(refreshResponse.accessToken(), response.accessToken());

        // Assert that we can authenticate with the refreshed access token
        assertAuthenticateWithToken(refreshResponse.accessToken(), TEST_USER_NAME);
        assertNotNull(refreshResponse.principal());
    }

    public void testRefreshingInvalidatedToken() throws IOException {
        OAuth2Token createTokenResponse = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        assertNotNull(createTokenResponse.getRefreshToken());
        TokenInvalidation invalidateResponse = invalidateRefreshToken(createTokenResponse.getRefreshToken());
        assertThat(invalidateResponse.invalidated(), equalTo(1));
        assertThat(invalidateResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateResponse.errors(), empty());

        ResponseException e = expectThrows(ResponseException.class, () -> refreshToken(createTokenResponse.getRefreshToken()));
        assertThat(e, throwableWithMessage(containsString("invalid_grant")));
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        assertThat(e, throwableWithMessage(containsString("token has been invalidated")));
    }

    public void testRefreshingMultipleTimesFails() throws Exception {
        OAuth2Token createTokenResponse = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        assertNotNull(createTokenResponse.getRefreshToken());

        OAuth2Token refreshResponse = refreshToken(createTokenResponse.getRefreshToken());
        assertNotNull(refreshResponse);
        // We now have two documents, the original(now refreshed) token doc and the new one with the new access doc
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            // refresh to make sure the token docs are visible
            Request refreshRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_refresh");
            refreshRequest.setOptions(SECURITY_REQUEST_OPTIONS);
            getRestClient().performRequest(refreshRequest);
            Request searchRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_search");
            searchRequest.setOptions(SECURITY_REQUEST_OPTIONS);
            searchRequest.setJsonEntity("""
                {
                    "query" : {
                        "bool" : {
                            "must" : [
                              {"term" : { "doc_type" : "token" }},
                              {"term" : { "refresh_token.refreshed" : "true" }}
                            ]
                        }
                    },
                    "terminate_after" : 1,
                    "size" : 1
                }
                """);
            Response searchResponse = getRestClient().performRequest(searchRequest);
            ObjectPath path = ObjectPath.createFromResponse(searchResponse);
            assertThat(path.evaluate("hits.total.value"), equalTo(1));
            final List<Map<String, ?>> hits = path.evaluate("hits.hits");
            final String id = ObjectPath.evaluate(hits.get(0), "_id");
            assertThat(id, notNullValue());
            docId.set(id);
        });

        // hack doc to modify the refresh time to 50 seconds ago so that we don't hit the lenient refresh case
        Instant refreshed = Instant.now();
        Instant aWhileAgo = refreshed.minus(50L, ChronoUnit.SECONDS);
        assertTrue(Instant.now().isAfter(aWhileAgo));

        Request updateRequest = new Request(HttpPost.METHOD_NAME, SecuritySystemIndices.SECURITY_TOKENS_ALIAS + "/_update/" + docId.get());
        updateRequest.addParameter("refresh", WriteRequest.RefreshPolicy.IMMEDIATE.getValue());
        updateRequest.setOptions(SECURITY_REQUEST_OPTIONS);
        updateRequest.setJsonEntity(Strings.format("""
            {
              "doc": {
                "refresh_token": {
                    "refresh_time" : %s
                }
              }
            }
            """, aWhileAgo.toEpochMilli()));
        updateRequest.addParameter("_source_includes", "refresh_token");
        Response updateResponse = getRestClient().performRequest(updateRequest);
        assertNotNull(updateResponse);

        Map<String, Object> refreshTokenMap = ObjectPath.createFromResponse(updateResponse).evaluate("get._source.refresh_token");
        assertTrue(Instant.ofEpochMilli((long) refreshTokenMap.get("refresh_time")).isBefore(Instant.now().minus(30L, ChronoUnit.SECONDS)));
        ResponseException e = expectThrows(ResponseException.class, () -> refreshToken(createTokenResponse.getRefreshToken()));
        assertThat(e, throwableWithMessage(containsString("invalid_grant")));
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        assertThat(e, throwableWithMessage(containsString("token has already been refreshed more than 30 seconds in the past")));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/85697")
    public void testRefreshingMultipleTimesWithinWindowSucceeds() throws Exception {
        final Clock clock = Clock.systemUTC();
        final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
        OAuth2Token createTokenResponse = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
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
                    OAuth2Token result = refreshToken(createTokenResponse.getRefreshToken());
                    final Instant t2 = clock.instant();
                    if (t1.plusSeconds(30L).isBefore(t2)) {
                        logger.warn(
                            "Tokens [{}], [{}] were received more than 30 seconds after the request, not checking them",
                            result.accessToken(),
                            result.getRefreshToken()
                        );
                    } else {
                        tokens.add(result.accessToken() + result.getRefreshToken());
                        // Assert that all requests from all threads could authenticate at the time they received the access token
                        // see: https://github.com/elastic/elasticsearch/issues/54289
                        try {
                            getSecurityClient(result.accessToken()).authenticate();
                        } catch (ResponseException esse) {
                            fail(esse);
                        }
                    }
                    logger.info("received access token [{}] and refresh token [{}]", result.accessToken(), result.getRefreshToken());
                } catch (IOException e) {
                    failed.set(true);
                    logger.error("caught exception", e);
                } finally {
                    completedLatch.countDown();
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
            Set<String> uniqueTokens = new HashSet<>(tokens);
            logger.info("Unique tokens received from refreshToken call [{}]", uniqueTokens);
            assertThat(uniqueTokens.size(), equalTo(1));
        }
    }

    public void testRefreshAsDifferentUser() throws IOException {
        OAuth2Token createTokenResponse = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        assertNotNull(createTokenResponse.getRefreshToken());

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> refreshToken(
                createTokenResponse.getRefreshToken(),
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(
                        "Authorization",
                        UsernamePasswordToken.basicAuthHeaderValue(
                            SecuritySettingsSource.ES_TEST_ROOT_USER,
                            SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                        )
                    )
                    .build()
            )
        );
        assertThat(e, throwableWithMessage(containsString("invalid_grant")));
        assertThat(e.getResponse(), hasStatusCode(RestStatus.BAD_REQUEST));
        assertThat(e, throwableWithMessage(containsString("tokens must be refreshed by the creating client")));
    }

    public void testCreateThenRefreshAsRunAsUser() throws IOException {
        final String nativeOtherUser = "other_user";
        getSecurityClient().putUser(new User(nativeOtherUser, "superuser"), SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        final RequestOptions runAsOtherOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(
                    SecuritySettingsSource.ES_TEST_ROOT_USER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                )
            )
            .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, nativeOtherUser)
            .build();
        final RequestOptions otherOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(nativeOtherUser, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            )
            .build();

        OAuth2Token createTokenResponse = createToken(
            TEST_USER_NAME,
            TEST_PASSWORD_SECURE_STRING,
            randomBoolean() ? runAsOtherOptions : otherOptions
        );
        assertNotNull(createTokenResponse.getRefreshToken());

        OAuth2Token refreshResponse = refreshToken(
            createTokenResponse.getRefreshToken(),
            randomBoolean() ? runAsOtherOptions : otherOptions
        );
        assertNotEquals(refreshResponse.accessToken(), createTokenResponse.accessToken());
        assertNotEquals(refreshResponse.getRefreshToken(), createTokenResponse.getRefreshToken());

        assertAuthenticateWithToken(createTokenResponse.accessToken(), TEST_USER_NAME);
        assertAuthenticateWithToken(refreshResponse.accessToken(), TEST_USER_NAME);
    }

    public void testClientCredentialsGrant() throws Exception {
        final RequestOptions superuserOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(
                    SecuritySettingsSource.ES_TEST_ROOT_USER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                )
            )
            .build();
        OAuth2Token createTokenResponse = createToken(superuserOptions);
        assertNull(createTokenResponse.getRefreshToken());

        assertAuthenticateWithToken(createTokenResponse.accessToken(), SecuritySettingsSource.ES_TEST_ROOT_USER);

        // invalidate
        TokenInvalidation invalidateTokenResponse = invalidateAccessToken(createTokenResponse.accessToken(), superuserOptions);
        assertThat(invalidateTokenResponse.invalidated(), equalTo(1));
        assertThat(invalidateTokenResponse.previouslyInvalidated(), equalTo(0));
        assertThat(invalidateTokenResponse.errors(), empty());

        assertUnauthorizedToken(createTokenResponse.accessToken());
    }

    public void testAuthenticateWithWrongToken() throws Exception {
        final TokenService tokenService = internalCluster().getInstance(TokenService.class);
        OAuth2Token response = createToken(TEST_USER_NAME, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        assertNotNull(response.getRefreshToken());
        // Assert that we can authenticate with the access token
        assertAuthenticateWithToken(response.accessToken(), TEST_USER_NAME);
        // Now attempt to authenticate with an invalid access token string
        assertUnauthorizedToken(randomAlphaOfLengthBetween(0, 128));
        // Now attempt to authenticate with an invalid access token with valid structure (pre 7.2)
        assertUnauthorizedToken(
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersions.V_7_1_0,
                tokenService.getRandomTokenBytes(TransportVersions.V_7_1_0, randomBoolean()).v1()
            )
        );
        // Now attempt to authenticate with an invalid access token with valid structure (after 7.2 pre 8.10)
        assertUnauthorizedToken(
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersions.V_7_4_0,
                tokenService.getRandomTokenBytes(TransportVersions.V_7_4_0, randomBoolean()).v1()
            )
        );
        // Now attempt to authenticate with an invalid access token with valid structure (current version)
        assertUnauthorizedToken(
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersion.current(),
                tokenService.getRandomTokenBytes(TransportVersion.current(), randomBoolean()).v1()
            )
        );
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
        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().setCustoms(true).get();
        assertFalse(clusterStateResponse.getState().customs().containsKey(TokenMetadata.TYPE));
    }

    public void testCreatorRealmCaptureWillWorkWithClientRunAs() throws IOException {
        final String nativeTokenUsername = "native_token_user";
        getSecurityClient().putUser(new User(nativeTokenUsername, "superuser"), SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        // File realm user run-as a native realm user
        final TestSecurityClient runAsClient = getSecurityClient(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING.clone())
                )
                .addHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, nativeTokenUsername)
                .build()
        );

        // Create a token with client credentials and run-as, the token should be owned by the run-as user (native realm)
        final OAuth2Token oAuth2Token1 = runAsClient.createTokenWithClientCredentialsGrant();
        // Token is usable
        final Map<String, Object> authenticateMap = getSecurityClient(oAuth2Token1.accessToken()).authenticate();
        assertThat(ObjectPath.evaluate(authenticateMap, "username"), equalTo(nativeTokenUsername));
        assertThat(ObjectPath.evaluate(authenticateMap, "lookup_realm.name"), equalTo("index"));
        assertThat(ObjectPath.evaluate(authenticateMap, "authentication_realm.name"), equalTo("file"));
        assertThat(ObjectPath.evaluate(authenticateMap, "authentication_type"), is("token"));

        final TokenInvalidation tokenInvalidation = getSecurityClient().invalidateTokens(Strings.format("""
            {
              "realm_name":"%s",
              "username":"%s"
            }""", "index", nativeTokenUsername));
        assertThat(tokenInvalidation.invalidated(), equalTo(1));

        // Create a token with password grant and run-as user (native realm)
        final OAuth2Token oAuth2Token2 = runAsClient.createToken(
            new UsernamePasswordToken(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING.clone())
        );

        // Refresh token is bound to the original user that creates it. In this case, it is the run-as user
        // refresh without run-as should fail
        final ResponseException e1 = expectThrows(
            ResponseException.class,
            () -> getSecurityClient(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(
                        "Authorization",
                        UsernamePasswordToken.basicAuthHeaderValue(ES_TEST_ROOT_USER, TEST_PASSWORD_SECURE_STRING.clone())
                    )
                    .build()
            ).refreshToken(oAuth2Token2.getRefreshToken())
        );
        assertThat(e1.getMessage(), containsString("tokens must be refreshed by the creating client"));

        // refresh with run-as should work
        final OAuth2Token oAuth2Token3 = runAsClient.refreshToken(oAuth2Token2.getRefreshToken());
        assertThat(oAuth2Token3.accessToken(), notNullValue());
    }

    private OAuth2Token createToken(RequestOptions options) throws IOException {
        return super.getSecurityClient(options).createTokenWithClientCredentialsGrant();
    }

    private OAuth2Token createToken(String user, SecureString password) throws IOException {
        return createToken(user, password, SECURITY_REQUEST_OPTIONS);
    }

    private OAuth2Token createToken(String user, SecureString password, RequestOptions options) throws IOException {
        return super.getSecurityClient(options).createToken(new UsernamePasswordToken(user, password));
    }

    private OAuth2Token refreshToken(String refreshToken) throws IOException {
        return refreshToken(refreshToken, SECURITY_REQUEST_OPTIONS);
    }

    private OAuth2Token refreshToken(String refreshToken, RequestOptions options) throws IOException {
        return super.getSecurityClient(options).refreshToken(refreshToken);
    }

    private TokenInvalidation invalidateAccessToken(String accessToken) throws IOException {
        return invalidateAccessToken(accessToken, SECURITY_REQUEST_OPTIONS);
    }

    private TokenInvalidation invalidateAccessToken(String accessToken, RequestOptions options) throws IOException {
        return super.getSecurityClient(options).invalidateAccessToken(accessToken);
    }

    private TokenInvalidation invalidateRefreshToken(String refreshToken) throws IOException {
        return invalidateRefreshToken(refreshToken, SECURITY_REQUEST_OPTIONS);
    }

    private TokenInvalidation invalidateRefreshToken(String refreshToken, RequestOptions options) throws IOException {
        return super.getSecurityClient(options).invalidateRefreshToken(refreshToken);
    }

    private void assertAuthenticateWithToken(String accessToken, String expectedUser) throws IOException {
        final TestSecurityClient securityClient = getSecurityClient(accessToken);
        final Map<String, Object> authResponse = securityClient.authenticate();
        assertThat(authResponse, hasEntry(User.Fields.USERNAME.getPreferredName(), expectedUser));
        assertThat(authResponse, hasEntry(User.Fields.AUTHENTICATION_TYPE.getPreferredName(), "token"));
    }

    private void assertUnauthorizedToken(String accessToken) {
        final TestSecurityClient securityClient = getSecurityClient(accessToken);
        ResponseException e = expectThrows(ResponseException.class, securityClient::authenticate);
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private TestSecurityClient getSecurityClient(String accessToken) {
        return getSecurityClient(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + accessToken).build());
    }
}
