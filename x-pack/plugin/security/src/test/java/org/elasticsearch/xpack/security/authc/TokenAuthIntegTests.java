/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;

@TestLogging("org.elasticsearch.xpack.security.authz.store.FileRolesStore:DEBUG")
public class TokenAuthIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
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
        PlainActionFuture<ClusterStateUpdateResponse> rotateActionFuture = new PlainActionFuture<>();
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
    }

    @TestLogging("org.elasticsearch.xpack.security.authc:DEBUG")
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

        Instant created = Instant.now();

        InvalidateTokenResponse invalidateResponse = securityClient
                .prepareInvalidateToken(response.getTokenString())
                .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
                .get();
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));
        AtomicReference<String> docId = new AtomicReference<>();
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
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
        client.prepareUpdate(SecurityIndexManager.SECURITY_INDEX_NAME, "doc", docId.get())
            .setDoc("creation_time", yesterday.toEpochMilli())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            if (deleteTriggered.compareAndSet(false, true)) {
                // invalidate a invalid token... doesn't matter that it is bad... we just want this action to trigger the deletion
                try {
                    securityClient.prepareInvalidateToken("fooobar")
                            .setType(randomFrom(InvalidateTokenRequest.Type.values()))
                            .execute()
                            .actionGet();
                } catch (ElasticsearchSecurityException e) {
                    assertEquals("token malformed", e.getMessage());
                }
            }
            client.admin().indices().prepareRefresh(SecurityIndexManager.SECURITY_INDEX_NAME).get();
            SearchResponse searchResponse = client.prepareSearch(SecurityIndexManager.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource()
                        .query(QueryBuilders.termQuery("doc_type", "token")))
                    .setTerminateAfter(1)
                    .get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        }, 30, TimeUnit.SECONDS);
    }

    public void testInvalidateAllTokensForUser() throws Exception{
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
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));
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
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));
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
        assertThat(invalidateResponse.getResult().getInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));
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
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));
        InvalidateTokenResponse invalidateAgainResponse = securityClient()
            .prepareInvalidateToken(response.getTokenString())
            .setType(InvalidateTokenRequest.Type.ACCESS_TOKEN)
            .get();
        assertThat(invalidateAgainResponse.getResult().getInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateAgainResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateAgainResponse.getResult().getErrors().size(), equalTo(0));
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
        assertThat(invalidateResponse.getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponse.getResult().getErrors().size(), equalTo(0));

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get());
        assertEquals("invalid_grant", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("token has been invalidated", e.getHeader("error_description").get(0));
    }

    public void testRefreshingMultipleTimes() {
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

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> securityClient.prepareRefreshToken(createTokenResponse.getRefreshToken()).get());
        assertEquals("invalid_grant", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("token has already been refreshed", e.getHeader("error_description").get(0));
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

        authFuture = new PlainActionFuture<>();
        request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_USER_NAME);
        client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
                .execute(AuthenticateAction.INSTANCE, request, authFuture);
        response = authFuture.actionGet();
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.authentication().getUser().principal());

        authFuture = new PlainActionFuture<>();
        request = new AuthenticateRequest();
        request.username(SecuritySettingsSource.TEST_USER_NAME);
        client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + refreshResponse.getTokenString()))
                .execute(AuthenticateAction.INSTANCE, request, authFuture);
        response = authFuture.actionGet();
        assertEquals(SecuritySettingsSource.TEST_USER_NAME, response.authentication().getUser().principal());
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

        // invalidate
        PlainActionFuture<InvalidateTokenResponse> invalidateResponseFuture = new PlainActionFuture<>();
        InvalidateTokenRequest invalidateTokenRequest =
            new InvalidateTokenRequest(createTokenResponse.getTokenString(), InvalidateTokenRequest.Type.ACCESS_TOKEN.getValue());
        securityClient.invalidateToken(invalidateTokenRequest, invalidateResponseFuture);
        assertThat(invalidateResponseFuture.get().getResult().getInvalidatedTokens().size(), equalTo(1));
        assertThat(invalidateResponseFuture.get().getResult().getPreviouslyInvalidatedTokens().size(), equalTo(0));
        assertThat(invalidateResponseFuture.get().getResult().getErrors().size(), equalTo(0));

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
            PlainActionFuture<AuthenticateResponse> responseFuture = new PlainActionFuture<>();
            client.filterWithHeader(Collections.singletonMap("Authorization", "Bearer " + createTokenResponse.getTokenString()))
                .execute(AuthenticateAction.INSTANCE, request, responseFuture);
            responseFuture.actionGet();
        });
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexActive();
    }

    @After
    public void wipeSecurityIndex() throws InterruptedException {
        // get the token service and wait until token expiration is not in progress!
        for (TokenService tokenService : internalCluster().getInstances(TokenService.class)) {
            final boolean done = awaitBusy(() -> tokenService.isExpirationInProgress() == false);
            assertTrue(done);
        }
        super.deleteSecurityIndex();
    }

    public void testMetadataIsNotSentToClient() {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setCustoms(true).get();
        assertFalse(clusterStateResponse.getState().customs().containsKey(TokenMetaData.TYPE));
    }
}
