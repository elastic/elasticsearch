/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static java.time.Clock.systemUTC;
import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenServiceTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();

    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private boolean mixedCluster;
    private Settings tokenServiceEnabledSettings;

    @Before
    public void setupClient() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                    .setType((String) invocationOnMock.getArguments()[1])
                    .setId((String) invocationOnMock.getArguments()[2]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString(), anyString());
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[1];
            MultiGetResponse response = mock(MultiGetResponse.class);
            MultiGetItemResponse[] responses = new MultiGetItemResponse[2];
            when(response.getResponses()).thenReturn(responses);

            GetResponse oldGetResponse = mock(GetResponse.class);
            when(oldGetResponse.isExists()).thenReturn(false);
            responses[0] = new MultiGetItemResponse(oldGetResponse, null);

            GetResponse getResponse = mock(GetResponse.class);
            responses[1] = new MultiGetItemResponse(getResponse, null);
            when(getResponse.isExists()).thenReturn(false);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));
        when(client.prepareIndex(any(String.class), any(String.class), any(String.class)))
                .thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class), any(String.class)))
                .thenReturn(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(new IndexResponse());
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));

        // setup lifecycle service
        securityIndex = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isAvailable()).thenReturn(true);
        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
        this.mixedCluster = randomBoolean();
        if (mixedCluster) {
            Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_6_0, Version.V_5_6_10);
            logger.info("adding a node with version [{}] to the cluster service", version);
            ClusterState updatedState = ClusterState.builder(clusterService.state())
                .nodes(DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(new DiscoveryNode("56node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                        EnumSet.allOf(DiscoveryNode.Role.class), version))
                    .build())
                .build();
            ClusterServiceUtils.setState(clusterService, updatedState);
        }
        tokenServiceEnabledSettings = Settings.builder()
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            .put(TokenService.BWC_ENABLED.getKey(), mixedCluster)
            .build();
    }

    @After
    public void stopClusterService() {
        if (clusterService != null) {
            clusterService.close();
        }
    }

    @BeforeClass
    public static void startThreadPool() throws IOException {
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
        new Authentication(new User("foo"), new RealmRef("realm", "type", "node"), null).writeToContext(threadPool.getThreadContext());
    }

    @AfterClass
    public static void shutdownThreadpool() throws InterruptedException {
        terminate(threadPool);
        threadPool = null;
    }

    public void testAttachAndGetToken() throws Exception {
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", randomFrom("Bearer ", "BEARER ", "bearer ") + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // verify a second separate token service with its own salt can also verify
            TokenService anotherService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex
                    , clusterService);
            anotherService.refreshMetaData(tokenService.getTokenMetaData());
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            UserToken fromOtherService = future.get();
            assertAuthenticationEquals(authentication, fromOtherService.getAuthentication());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testInvalidAuthorizationHeader() throws Exception {
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        String token = randomFrom("", "          ");
        String authScheme = randomFrom("Bearer ", "BEARER ", "bearer ", "Basic ");
        requestContext.putHeader("Authorization", authScheme + token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertThat(serialized, nullValue());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testRotateKey() throws Exception {
        assumeFalse("internally managed keys do not work in a mixed cluster", mixedCluster);
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }
        rotateKeys(tokenService);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        PlainActionFuture<Tuple<UserToken, String>> newTokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, newTokenFuture, Collections.emptyMap(), true);
        final UserToken newToken = newTokenFuture.get().v1();
        assertNotNull(newToken);
        assertNotEquals(tokenService.getUserTokenString(newToken), tokenService.getUserTokenString(token));

        requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(newToken));
        mockGetTokenFromId(newToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    private void rotateKeys(TokenService tokenService) {
        TokenMetaData tokenMetaData = tokenService.generateSpareKey();
        tokenService.refreshMetaData(tokenMetaData);
        tokenMetaData = tokenService.rotateToSpareKey();
        tokenService.refreshMetaData(tokenMetaData);
    }

    public void testKeyExchange() throws Exception {
        assumeFalse("internally managed keys do not work in a mixed cluster", mixedCluster);
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        int numRotations = randomIntBetween(1, 5);
        for (int i = 0; i < numRotations; i++) {
            rotateKeys(tokenService);
        }
        TokenService otherTokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex,
                clusterService);
        otherTokenService.refreshMetaData(tokenService.getTokenMetaData());
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            otherTokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        rotateKeys(tokenService);

        otherTokenService.refreshMetaData(tokenService.getTokenMetaData());

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            otherTokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testPruneKeys() throws Exception {
        assumeFalse("internally managed keys do not work in a mixed cluster", mixedCluster);
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }
        TokenMetaData metaData = tokenService.pruneKeys(randomIntBetween(0, 100));
        tokenService.refreshMetaData(metaData);

        int numIterations = scaledRandomIntBetween(1, 5);
        for (int i = 0; i < numIterations; i++) {
            rotateKeys(tokenService);
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        PlainActionFuture<Tuple<UserToken, String>> newTokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, newTokenFuture, Collections.emptyMap(), true);
        final UserToken newToken = newTokenFuture.get().v1();
        assertNotNull(newToken);
        assertNotEquals(tokenService.getUserTokenString(newToken), tokenService.getUserTokenString(token));

        metaData = tokenService.pruneKeys(1);
        tokenService.refreshMetaData(metaData);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertNull(serialized);
        }

        requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(newToken));
        mockGetTokenFromId(newToken);
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testPassphraseWorks() throws Exception {
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertAuthenticationEquals(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // verify a second separate token service with its own passphrase cannot verify
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(8, 30));
            Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
            TokenService anotherService = new TokenService(settings, systemUTC(), client, securityIndex, clusterService);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED, TokenService.TOKEN_PASSPHRASE });
    }

    public void testGetTokenWhenKeyCacheHasExpired() throws Exception {
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);

        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        UserToken token = tokenFuture.get().v1();
        assertThat(tokenService.getUserTokenString(token), notNullValue());

        tokenService.clearActiveKeyCache();
        assertThat(tokenService.getUserTokenString(token), notNullValue());

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testInvalidatedToken() throws Exception {
        when(securityIndex.indexExists()).thenReturn(true);
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        doAnswer(invocationOnMock -> {
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[1];
            MultiGetResponse response = mock(MultiGetResponse.class);
            MultiGetItemResponse[] responses = new MultiGetItemResponse[2];
            when(response.getResponses()).thenReturn(responses);

            final boolean newExpired = randomBoolean();
            GetResponse oldGetResponse = mock(GetResponse.class);
            when(oldGetResponse.isExists()).thenReturn(newExpired == false);
            responses[0] = new MultiGetItemResponse(oldGetResponse, null);

            GetResponse getResponse = mock(GetResponse.class);
            responses[1] = new MultiGetItemResponse(getResponse, null);
            when(getResponse.isExists()).thenReturn(newExpired);
            if (newExpired) {
                Map<String, Object> source = MapBuilder.<String, Object>newMapBuilder()
                        .put("access_token", Collections.singletonMap("invalidated", true))
                        .immutableMap();
                when(getResponse.getSource()).thenReturn(source);
            }
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));
        mockGetTokenFromId(token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
            final String headerValue = e.getHeader("WWW-Authenticate").get(0);
            assertThat(headerValue, containsString("Bearer realm="));
            assertThat(headerValue, containsString("expired"));
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testComputeSecretKeyIsConsistent() throws Exception {
        byte[] saltArr = new byte[32];
        random().nextBytes(saltArr);
        SecretKey key = TokenService.computeSecretKey("some random passphrase".toCharArray(), saltArr);
        SecretKey key2 = TokenService.computeSecretKey("some random passphrase".toCharArray(), saltArr);
        assertArrayEquals(key.getEncoded(), key2.getEncoded());
    }

    public void testTokenExpiryConfig() {
        TimeValue expiration =  TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueMinutes(20L)));
        // Configure Minimum expiration
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "1s").build();
        expiration =  TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueSeconds(1L)));
        // Configure Maximum expiration
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "60m").build();
        expiration =  TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueHours(1L)));
        // Outside range should fail
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "1ms").build();
        IllegalArgumentException ile = expectThrows(IllegalArgumentException.class,
                () -> TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings));
        assertThat(ile.getMessage(),
                containsString("failed to parse value [1ms] for setting [xpack.security.authc.token.timeout], must be >= [1s]"));
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "120m").build();
        ile = expectThrows(IllegalArgumentException.class, () -> TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings));
        assertThat(ile.getMessage(),
                containsString("failed to parse value [120m] for setting [xpack.security.authc.token.timeout], must be <= [1h]"));
    }

    public void testTokenExpiry() throws Exception {
        ClockMock clock = ClockMock.frozen();
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, clock, client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        mockGetTokenFromId(token);
        mockCheckTokenInvalidationFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // the clock is still frozen, so the cookie should be valid
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertAuthenticationEquals(authentication, future.get().getAuthentication());
        }

        final TimeValue defaultExpiration = TokenService.TOKEN_EXPIRATION.get(Settings.EMPTY);
        final int fastForwardAmount = randomIntBetween(1, Math.toIntExact(defaultExpiration.getSeconds()) - 5);
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // move the clock forward but don't go to expiry
            clock.fastForwardSeconds(fastForwardAmount);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertAuthenticationEquals(authentication, future.get().getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // move to expiry
            clock.fastForwardSeconds(Math.toIntExact(defaultExpiration.getSeconds()) - fastForwardAmount);
            clock.rewind(TimeValue.timeValueNanos(clock.instant().getNano())); // trim off nanoseconds since don't store them in the index
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertAuthenticationEquals(authentication, future.get().getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // move one second past expiry
            clock.fastForwardSeconds(1);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
            final String headerValue = e.getHeader("WWW-Authenticate").get(0);
            assertThat(headerValue, containsString("Bearer realm="));
            assertThat(headerValue, containsString("expired"));
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testTokenServiceDisabled() throws Exception {
        TokenService tokenService = new TokenService(Settings.builder()
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), false)
                .build(),
                systemUTC(), client, securityIndex, clusterService);
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> tokenService.createUserToken(null, null, null, null, true));
        assertEquals("tokens are not enabled", e.getMessage());

        PlainActionFuture<UserToken> future = new PlainActionFuture<>();
        tokenService.getAndValidateToken(null, future);
        assertNull(future.get());

        e = expectThrows(IllegalStateException.class, () -> {
            PlainActionFuture<TokensInvalidationResult> invalidateFuture = new PlainActionFuture<>();
            tokenService.invalidateAccessToken((String) null, invalidateFuture);
            invalidateFuture.actionGet();
        });
        assertEquals("tokens are not enabled", e.getMessage());
    }

    public void testBytesKeyEqualsHashCode() {
        final int dataLength = randomIntBetween(2, 32);
        final byte[] data = randomBytes(dataLength);
        BytesKey bytesKey = new BytesKey(data);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(bytesKey, (b) -> new BytesKey(b.bytes.clone()), (b) -> {
            final byte[] copy = b.bytes.clone();
            final int randomlyChangedValue = randomIntBetween(0, copy.length - 1);
            final byte original = copy[randomlyChangedValue];
            boolean loop;
            do {
                byte value = randomByte();
                if (value == original) {
                    loop = true;
                } else {
                    loop = false;
                    copy[randomlyChangedValue] = value;
                }
            } while (loop);
            return new BytesKey(copy);
        });
    }

    public void testMalformedToken() throws Exception {
        final int numBytes = randomIntBetween(1, TokenService.MINIMUM_BYTES + 32);
        final byte[] randomBytes = new byte[numBytes];
        random().nextBytes(randomBytes);
        TokenService tokenService = new TokenService(Settings.EMPTY, systemUTC(), client, securityIndex, clusterService);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + Base64.getEncoder().encodeToString(randomBytes));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }
    }

    public void testIndexNotAvailable() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        tokenService.createUserToken(authentication, authentication, tokenFuture, Collections.emptyMap(), true);
        final UserToken token = tokenFuture.get().v1();
        assertNotNull(token);
        mockGetTokenFromId(token);
        authentication = token.getAuthentication();

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        doAnswer(invocationOnMock -> {
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new NoShardAvailableActionException(new ShardId(new Index("foo", "uuid"), 0), "shard oh shard"));
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertNull(future.get());

            when(securityIndex.isAvailable()).thenReturn(false);
            when(securityIndex.indexExists()).thenReturn(true);
            future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertNull(future.get());

            when(securityIndex.indexExists()).thenReturn(false);
            future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            if (mixedCluster) {
                assertAuthenticationEquals(authentication, future.get().getAuthentication());
            } else {
                assertNull(future.get());
            }


            when(securityIndex.isAvailable()).thenReturn(true);
            when(securityIndex.indexExists()).thenReturn(true);
            mockCheckTokenInvalidationFromId(token);
            future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertAuthenticationEquals(token.getAuthentication(), future.get().getAuthentication());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    public void testDecodePre6xToken() throws GeneralSecurityException, ExecutionException, InterruptedException, IOException {
        String token = "g+y0AiDWsbLNzUGTywPa3VCz053RUPW7wAx4xTAonlcqjOmO1AzMhQDTUku/+ZtdtMgDobKqIrNdNvchvFMX0pvZLY6i4nAG2OhkApSstPfQQP" +
                "J1fxg/JZNQDPufePg1GxV/RAQm2Gr8mYAelijEVlWIdYaQ3R76U+P/w6Q1v90dGVZQn6DKMOfgmkfwAFNY";
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), "xpack_token_passpharse");
        Settings settings = Settings.builder()
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put(TokenService.BWC_ENABLED.getKey(), true)
            .setSecureSettings(secureSettings).build();
        TokenService tokenService = new TokenService(settings, systemUTC(), client, securityIndex, clusterService);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.decodeToken(tokenService.getFromHeader(requestContext), future);
            UserToken serialized = future.get();
            assertNotNull(serialized);
            assertEquals("joe", serialized.getAuthentication().getUser().principal());
            assertEquals(Version.V_5_6_0, serialized.getAuthentication().getVersion());
            assertArrayEquals(new String[]{"admin"}, serialized.getAuthentication().getUser().roles());
        }

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED, TokenService.TOKEN_PASSPHRASE });
    }

    public void testV5TokensAreNotAcceptedByDefault() throws Exception {
        String token = "g+y0AiDWsbLNzUGTywPa3VCz053RUPW7wAx4xTAonlcqjOmO1AzMhQDTUku/+ZtdtMgDobKqIrNdNvchvFMX0pvZLY6i4nAG2OhkApSstPfQQP" +
            "J1fxg/JZNQDPufePg1GxV/RAQm2Gr8mYAelijEVlWIdYaQ3R76U+P/w6Q1v90dGVZQn6DKMOfgmkfwAFNY";
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), "xpack_token_passpharse");
        Settings settings = Settings.builder()
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .setSecureSettings(secureSettings).build();
        TokenService tokenService = new TokenService(settings, systemUTC(), client, securityIndex, clusterService);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.decodeToken(tokenService.getFromHeader(requestContext), future);
            final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(iae.getMessage(), containsString("Cannot authenticate using token from version [5.6.0]"));
        }
        assertWarnings("[xpack.security.authc.token.passphrase] setting was deprecated in Elasticsearch and will be removed in a future" +
            " release! See the breaking changes documentation for the next major version.");
    }

    public void testGetAuthenticationWorksWithExpiredToken() throws Exception {
        TokenService tokenService =
                new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        UserToken expired = new UserToken(authentication, Instant.now().minus(3L, ChronoUnit.DAYS));
        mockGetTokenFromId(expired);
        String userTokenString = tokenService.getUserTokenString(expired);
        PlainActionFuture<Tuple<Authentication, Map<String, Object>>> authFuture = new PlainActionFuture<>();
        tokenService.getAuthenticationAndMetaData(userTokenString, authFuture);
        Authentication retrievedAuth = authFuture.actionGet().v1();
        assertEquals(authentication, retrievedAuth);

        assertSettingDeprecationsAndWarnings(new Setting[] { TokenService.BWC_ENABLED });
    }

    private void mockGetTokenFromId(UserToken userToken) {
        mockGetTokenFromId(userToken, client);
    }

    public static void mockGetTokenFromId(UserToken userToken, Client client) {
        doAnswer(invocationOnMock -> {
            GetRequest getRequest = (GetRequest) invocationOnMock.getArguments()[0];
            ActionListener<GetResponse> getResponseListener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            GetResponse getResponse = mock(GetResponse.class);
            if (userToken.getId().equals(getRequest.id().replace("token_", ""))) {
                when(getResponse.isExists()).thenReturn(true);
                Map<String, Object> sourceMap = new HashMap<>();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    userToken.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    sourceMap.put("access_token",
                            Collections.singletonMap("user_token",
                                    XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false)));
                }
                when(getResponse.getSource()).thenReturn(sourceMap);
            }
            getResponseListener.onResponse(getResponse);
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));
    }

    private void assertAuthenticationEquals(Authentication expected, Authentication actual) {
        if (mixedCluster) {
            assertNotNull(expected);
            assertNotNull(actual);
            assertEquals(expected.getUser(), actual.getUser());
            assertEquals(expected.getAuthenticatedBy(), actual.getAuthenticatedBy());
            assertEquals(expected.getLookedUpBy(), actual.getLookedUpBy());
        } else {
            assertEquals(expected.getUser(), actual.getUser());
            assertEquals(expected.getAuthenticatedBy(), actual.getAuthenticatedBy());
            assertEquals(expected.getLookedUpBy(), actual.getLookedUpBy());
            assertEquals(expected.getMetadata(), actual.getMetadata());
            assertEquals(AuthenticationType.TOKEN, actual.getAuthenticationType());
        }
    }

    private void mockCheckTokenInvalidationFromId(UserToken userToken) {
        mockCheckTokenInvalidationFromId(userToken, client);
    }

    public static void mockCheckTokenInvalidationFromId(UserToken userToken, Client client) {
        doAnswer(invocationOnMock -> {
            MultiGetRequest request = (MultiGetRequest) invocationOnMock.getArguments()[0];
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[1];
            MultiGetResponse response = mock(MultiGetResponse.class);
            MultiGetItemResponse[] responses = new MultiGetItemResponse[2];
            when(response.getResponses()).thenReturn(responses);
            GetResponse legacyResponse = mock(GetResponse.class);
            responses[0] = new MultiGetItemResponse(legacyResponse, null);
            when(legacyResponse.isExists()).thenReturn(false);
            GetResponse tokenResponse = mock(GetResponse.class);
            if (userToken.getId().equals(request.getItems().get(1).id().replace("token_", ""))) {
                when(tokenResponse.isExists()).thenReturn(true);
                Map<String, Object> sourceMap = new HashMap<>();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    userToken.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    Map<String, Object> accessTokenMap = new HashMap<>();
                    accessTokenMap.put("user_token",
                        XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false));
                    accessTokenMap.put("invalidated", false);
                    sourceMap.put("access_token", accessTokenMap);
                }
                when(tokenResponse.getSource()).thenReturn(sourceMap);
            }
            responses[1] = new MultiGetItemResponse(tokenResponse, null);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));
    }
}
