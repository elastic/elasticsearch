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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TokenAuthIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // turn down token expiration interval and crank up the deletion interval
                .put(TokenService.TOKEN_EXPIRATION.getKey(), TimeValue.timeValueSeconds(1L))
                .put(TokenService.DELETE_INTERVAL.getKey(), TimeValue.timeValueSeconds(1L))
                .put(TokenService.DELETE_TIMEOUT.getKey(), TimeValue.timeValueSeconds(2L))
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
                .setPassword(new SecureString(SecuritySettingsSource.TEST_PASSWORD.toCharArray()))
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
                .setPassword(new SecureString(SecuritySettingsSource.TEST_PASSWORD.toCharArray()))
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

    public void testExpiredTokensDeletedAfterExpiration() throws Exception {
        final Client client = client().filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                        SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING)));
        SecurityClient securityClient = new SecurityClient(client);
        CreateTokenResponse response = securityClient.prepareCreateToken()
                .setGrantType("password")
                .setUsername(SecuritySettingsSource.TEST_USER_NAME)
                .setPassword(new SecureString(SecuritySettingsSource.TEST_PASSWORD.toCharArray()))
                .get();

        Instant created = Instant.now();

        InvalidateTokenResponse invalidateResponse = securityClient.prepareInvalidateToken(response.getTokenString()).get();
        assertTrue(invalidateResponse.isCreated());
        assertBusy(() -> {
            SearchResponse searchResponse = client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", TokenService.DOC_TYPE)))
                    .setSize(0)
                    .setTerminateAfter(1)
                    .get();
            assertThat(searchResponse.getHits().getTotalHits(), greaterThan(0L));
        });

        AtomicBoolean deleteTriggered = new AtomicBoolean(false);
        assertBusy(() -> {
            assertTrue(Instant.now().isAfter(created.plusSeconds(1L).plusMillis(500L)));
            if (deleteTriggered.compareAndSet(false, true)) {
                // invalidate a invalid token... doesn't matter that it is bad... we just want this action to trigger the deletion
                try {
                    securityClient.prepareInvalidateToken("fooobar").execute().actionGet();
                } catch (ElasticsearchSecurityException e) {
                    assertEquals("token malformed", e.getMessage());
                }
            }
            client.admin().indices().prepareRefresh(SecurityLifecycleService.SECURITY_INDEX_NAME).get();
            SearchResponse searchResponse = client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                    .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("doc_type", TokenService.DOC_TYPE)))
                    .setSize(0)
                    .setTerminateAfter(1)
                    .get();
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(0L));
        }, 30, TimeUnit.SECONDS);
    }

    public void testExpireMultipleTimes() throws Exception {
        CreateTokenResponse response = securityClient().prepareCreateToken()
                .setGrantType("password")
                .setUsername(SecuritySettingsSource.TEST_USER_NAME)
                .setPassword(new SecureString(SecuritySettingsSource.TEST_PASSWORD.toCharArray()))
                .get();

        InvalidateTokenResponse invalidateResponse = securityClient().prepareInvalidateToken(response.getTokenString()).get();

        // if the token is expired then the API will return false for created so we need to handle that
        final boolean correctResponse = invalidateResponse.isCreated() || isTokenExpired(response.getTokenString());
        assertTrue(correctResponse);
        assertFalse(securityClient().prepareInvalidateToken(response.getTokenString()).get().isCreated());
    }

    private static boolean isTokenExpired(String token) {
        try {
            TokenService tokenService = internalCluster().getInstance(TokenService.class);
            PlainActionFuture<UserToken> tokenFuture = new PlainActionFuture<>();
            tokenService.decodeToken(token, tokenFuture);
            return tokenFuture.actionGet().getExpirationTime().isBefore(Instant.now());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Before
    public void waitForSecurityIndexWritable() throws Exception {
        assertSecurityIndexWriteable();
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
