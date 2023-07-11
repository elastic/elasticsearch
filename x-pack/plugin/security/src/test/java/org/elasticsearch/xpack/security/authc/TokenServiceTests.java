/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.TokenService.RefreshTokenStatus;
import org.elasticsearch.xpack.security.support.FeatureNotEnabledException;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import static java.time.Clock.systemUTC;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.security.authc.TokenService.VERSION_CLIENT_AUTH_FOR_REFRESH;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenServiceTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static final Settings settings = Settings.builder()
        .put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();

    private Client client;
    private SecurityIndexManager securityMainIndex;
    private SecurityIndexManager securityTokensIndex;
    private ClusterService clusterService;
    private DiscoveryNode oldNode;
    private Settings tokenServiceEnabledSettings = Settings.builder()
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();
    private MockLicenseState licenseState;
    private SecurityContext securityContext;

    @Before
    public void setupClient() {
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString());
        when(client.prepareIndex(any(String.class))).thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class))).thenAnswer(inv -> {
            final String index = (String) inv.getArguments()[0];
            final String id = (String) inv.getArguments()[1];
            return new UpdateRequestBuilder(client, UpdateAction.INSTANCE).setIndex(index).setId(id);
        });
        when(client.prepareSearch(any(String.class))).thenReturn(new SearchRequestBuilder(client, SearchAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(
                new IndexResponse(
                    new ShardId(".security", UUIDs.randomBase64UUID(), randomInt()),
                    randomAlphaOfLength(4),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    true
                )
            );
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), anyActionListener());
        doAnswer(invocationOnMock -> {
            BulkRequest request = (BulkRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<BulkResponse> responseActionListener = (ActionListener<BulkResponse>) invocationOnMock.getArguments()[1];
            BulkItemResponse[] responses = new BulkItemResponse[request.requests().size()];
            final String indexUUID = randomAlphaOfLength(22);
            for (int i = 0; i < responses.length; i++) {
                var shardId = new ShardId(securityTokensIndex.aliasName(), indexUUID, 1);
                var docId = request.requests().get(i).id();
                var result = new GetResult(shardId.getIndexName(), docId, 1, 1, 1, true, null, null, null);
                final UpdateResponse response = new UpdateResponse(
                    shardId,
                    result.getId(),
                    result.getSeqNo(),
                    result.getPrimaryTerm(),
                    result.getVersion() + 1,
                    DocWriteResponse.Result.UPDATED
                );
                response.setGetResult(result);
                responses[i] = BulkItemResponse.success(i, DocWriteRequest.OpType.UPDATE, response);
            }
            responseActionListener.onResponse(new BulkResponse(responses, randomLongBetween(1, 500)));
            return null;
        }).when(client).bulk(any(BulkRequest.class), anyActionListener());

        this.securityContext = new SecurityContext(settings, threadPool.getThreadContext());
        // setup lifecycle service
        this.securityMainIndex = SecurityMocks.mockSecurityIndexManager();
        this.securityTokensIndex = SecurityMocks.mockSecurityIndexManager();
        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);

        // License state (enabled by default)
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);

        // version 7.2 was an "inflection" point in the Token Service development (access_tokens as UUIDS, multiple concurrent refreshes,
        // tokens docs on a separate index), let's test the TokenService works in a mixed cluster with nodes with versions prior to these
        // developments
        if (randomBoolean()) {
            oldNode = addAnother7071DataNode(this.clusterService);
        }
    }

    private static DiscoveryNode addAnother7071DataNode(ClusterService clusterService) {
        Version version;
        TransportVersion transportVersion;
        if (randomBoolean()) {
            version = Version.V_7_0_0;
            transportVersion = TransportVersion.V_7_0_0;
        } else {
            version = Version.V_7_1_0;
            transportVersion = TransportVersion.V_7_1_0;
        }

        return addAnotherDataNodeWithVersion(clusterService, version, transportVersion);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @BeforeClass
    public static void startThreadPool() throws IOException {
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                TokenService.THREAD_POOL_NAME,
                1,
                1000,
                "xpack.security.authc.token.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        AuthenticationTestHelper.builder()
            .user(new User("foo"))
            .realmRef(new RealmRef("realm", "type", "node"))
            .build(false)
            .writeToContext(threadPool.getThreadContext());
    }

    @AfterClass
    public static void shutdownThreadpool() {
        terminate(threadPool);
        threadPool = null;
    }

    public void testAttachAndGetToken() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        mockGetTokenFromId(tokenService, userTokenId, authentication, false);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", randomFrom("Bearer ", "BEARER ", "bearer ") + accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            UserToken serialized = future.get();
            assertAuthentication(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // verify a second separate token service with its own salt can also verify
            TokenService anotherService = createTokenService(tokenServiceEnabledSettings, systemUTC());
            anotherService.refreshMetadata(tokenService.getTokenMetadata());
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            anotherService.tryAuthenticateToken(bearerToken, future);
            UserToken fromOtherService = future.get();
            assertAuthentication(authentication, fromOtherService.getAuthentication());
        }
    }

    public void testInvalidAuthorizationHeader() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        String token = randomFrom("", "          ");
        String authScheme = randomFrom("Bearer ", "BEARER ", "bearer ", "Basic ");
        requestContext.putHeader("Authorization", authScheme + token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            UserToken serialized = future.get();
            assertThat(serialized, nullValue());
        }
    }

    public void testPassphraseWorks() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // This test only makes sense in mixed clusters with pre v7.1.0 nodes where the Key is actually used
        if (null == oldNode) {
            oldNode = addAnother7071DataNode(this.clusterService);
        }
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        mockGetTokenFromId(tokenService, userTokenId, authentication, false);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            UserToken serialized = future.get();
            assertAuthentication(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // verify a second separate token service with its own passphrase cannot verify
            TokenService anotherService = createTokenService(tokenServiceEnabledSettings, systemUTC());
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            anotherService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testGetTokenWhenKeyCacheHasExpired() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // This test only makes sense in mixed clusters with pre v7.1.0 nodes where the Key is actually used
        if (null == oldNode) {
            oldNode = addAnother7071DataNode(this.clusterService);
        }
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);

        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        String accessToken = tokenFuture.get().getAccessToken();
        assertThat(accessToken, notNullValue());

        tokenService.clearActiveKeyCache();

        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        accessToken = tokenFuture.get().getAccessToken();
        assertThat(accessToken, notNullValue());
    }

    public void testInvalidatedToken() throws Exception {
        when(securityMainIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        mockGetTokenFromId(tokenService, userTokenId, authentication, true);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
            final String headerValue = e.getHeader("WWW-Authenticate").get(0);
            assertThat(headerValue, containsString("Bearer realm="));
            assertThat(headerValue, containsString("expired"));
        }
    }

    public void testInvalidateRefreshToken() throws Exception {
        when(securityMainIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String rawRefreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, rawRefreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        final String clientRefreshToken = tokenFuture.get().getRefreshToken();
        assertNotNull(accessToken);
        mockFindTokenFromRefreshToken(rawRefreshToken, buildUserToken(tokenService, userTokenId, authentication, Map.of()), null);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<TokensInvalidationResult> future = new PlainActionFuture<>();
            tokenService.invalidateRefreshToken(clientRefreshToken, future);
            final TokensInvalidationResult result = future.get();
            assertThat(result.getInvalidatedTokens(), hasSize(1));
            assertThat(result.getPreviouslyInvalidatedTokens(), empty());
            assertThat(result.getErrors(), empty());
        }
    }

    public void testInvalidateRefreshTokenThatIsAlreadyInvalidated() throws Exception {
        when(securityMainIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTests.randomAuthentication(null, null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String rawRefreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, rawRefreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        final String clientRefreshToken = tokenFuture.get().getRefreshToken();
        assertNotNull(accessToken);
        mockFindTokenFromRefreshToken(
            rawRefreshToken,
            buildUserToken(tokenService, userTokenId, authentication, Map.of()),
            newRefreshTokenStatus(true, authentication, false, null, null, null, null)
        );

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<TokensInvalidationResult> future = new PlainActionFuture<>();
            tokenService.invalidateRefreshToken(clientRefreshToken, future);
            final TokensInvalidationResult result = future.get();
            assertThat(result.getPreviouslyInvalidatedTokens(), hasSize(1));
            assertThat(result.getInvalidatedTokens(), empty());
            assertThat(result.getErrors(), empty());
        }
    }

    private RefreshTokenStatus newRefreshTokenStatus(
        boolean invalidated,
        Authentication authentication,
        boolean refreshed,
        Instant refreshInstant,
        String supersedingTokens,
        String iv,
        String salt
    ) {
        if (authentication.getEffectiveSubject().getTransportVersion().onOrAfter(VERSION_CLIENT_AUTH_FOR_REFRESH)) {
            return new RefreshTokenStatus(invalidated, authentication, refreshed, refreshInstant, supersedingTokens, iv, salt);
        } else {
            return new RefreshTokenStatus(
                invalidated,
                authentication.getEffectiveSubject().getUser().principal(),
                authentication.getAuthenticatingSubject().getRealm().getName(),
                refreshed,
                refreshInstant,
                supersedingTokens,
                iv,
                salt
            );
        }
    }

    private void storeTokenHeader(ThreadContext requestContext, String tokenString) {
        requestContext.putHeader("Authorization", "Bearer " + tokenString);
    }

    public void testComputeSecretKeyIsConsistent() throws Exception {
        byte[] saltArr = new byte[32];
        random().nextBytes(saltArr);
        SecretKey key = TokenService.computeSecretKey(
            "some random passphrase".toCharArray(),
            saltArr,
            TokenService.TOKEN_SERVICE_KEY_ITERATIONS
        );
        SecretKey key2 = TokenService.computeSecretKey(
            "some random passphrase".toCharArray(),
            saltArr,
            TokenService.TOKEN_SERVICE_KEY_ITERATIONS
        );
        assertArrayEquals(key.getEncoded(), key2.getEncoded());
    }

    public void testTokenExpiryConfig() {
        TimeValue expiration = TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueMinutes(20L)));
        // Configure Minimum expiration
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "1s").build();
        expiration = TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueSeconds(1L)));
        // Configure Maximum expiration
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "60m").build();
        expiration = TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings);
        assertThat(expiration, equalTo(TimeValue.timeValueHours(1L)));
        // Outside range should fail
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "1ms").build();
        IllegalArgumentException ile = expectThrows(
            IllegalArgumentException.class,
            () -> TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings)
        );
        assertThat(
            ile.getMessage(),
            containsString("failed to parse value [1ms] for setting [xpack.security.authc.token.timeout], must be >= [1s]")
        );
        tokenServiceEnabledSettings = Settings.builder().put(TokenService.TOKEN_EXPIRATION.getKey(), "120m").build();
        ile = expectThrows(IllegalArgumentException.class, () -> TokenService.TOKEN_EXPIRATION.get(tokenServiceEnabledSettings));
        assertThat(
            ile.getMessage(),
            containsString("failed to parse value [120m] for setting [xpack.security.authc.token.timeout], must be <= [1h]")
        );
    }

    public void testTokenExpiry() throws Exception {
        ClockMock clock = ClockMock.frozen();
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, clock);
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        final String userTokenId = UUIDs.randomBase64UUID();
        UserToken userToken = new UserToken(
            userTokenId,
            tokenService.getTokenVersionCompatibility(),
            authentication,
            tokenService.getExpirationTime(),
            Collections.emptyMap()
        );
        mockGetTokenFromId(userToken, false);
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            userTokenId
        );

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // the clock is still frozen, so the cookie should be valid
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertAuthentication(authentication, future.get().getAuthentication());
        }

        final TimeValue defaultExpiration = TokenService.TOKEN_EXPIRATION.get(Settings.EMPTY);
        final int fastForwardAmount = randomIntBetween(1, Math.toIntExact(defaultExpiration.getSeconds()) - 5);
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // move the clock forward but don't go to expiry
            clock.fastForwardSeconds(fastForwardAmount);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertAuthentication(authentication, future.get().getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // move to expiry, stripping nanoseconds, as we don't store them in the security-tokens index
            clock.setTime(userToken.getExpirationTime().truncatedTo(ChronoUnit.MILLIS).atZone(clock.getZone()));
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertAuthentication(authentication, future.get().getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            // move one second past expiry
            clock.fastForwardSeconds(1);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
            final String headerValue = e.getHeader("WWW-Authenticate").get(0);
            assertThat(headerValue, containsString("Bearer realm="));
            assertThat(headerValue, containsString("expired"));
        }
    }

    public void testTokenServiceDisabled() throws Exception {
        TokenService tokenService = new TokenService(
            Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), false).build(),
            Clock.systemUTC(),
            client,
            licenseState,
            securityContext,
            securityMainIndex,
            securityTokensIndex,
            clusterService
        );
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> tokenService.createOAuth2Tokens(null, null, null, true, null)
        );
        assertThat(e, throwableWithMessage("security tokens are not enabled"));
        assertThat(e, instanceOf(FeatureNotEnabledException.class));
        // Client can check the metadata for this value, and depend on an exact string match:
        assertThat(e.getMetadata(FeatureNotEnabledException.DISABLED_FEATURE_METADATA), contains("security_tokens"));

        PlainActionFuture<UserToken> future = new PlainActionFuture<>();
        tokenService.tryAuthenticateToken(null, future);
        assertNull(future.get());

        PlainActionFuture<TokensInvalidationResult> invalidateFuture = new PlainActionFuture<>();
        e = expectThrows(ElasticsearchException.class, () -> tokenService.invalidateAccessToken((String) null, invalidateFuture));
        assertThat(e, throwableWithMessage("security tokens are not enabled"));
        assertThat(e, instanceOf(FeatureNotEnabledException.class));
        // Client can check the metadata for this value, and depend on an exact string match:
        assertThat(e.getMetadata(FeatureNotEnabledException.DISABLED_FEATURE_METADATA), contains("security_tokens"));
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
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromId(tokenService, UUIDs.randomBase64UUID(), authentication, false);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, Base64.getEncoder().encodeToString(randomBytes));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testNotValidPre72Tokens() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromId(tokenService, UUIDs.randomBase64UUID(), authentication, false);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, generateAccessToken(tokenService, TransportVersion.V_7_1_0));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testNotValidAfter72Tokens() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromId(tokenService, UUIDs.randomBase64UUID(), authentication, false);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, generateAccessToken(tokenService, randomFrom(TransportVersion.V_7_2_0, TransportVersion.V_7_3_2)));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testIndexNotAvailable() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String userTokenId = UUIDs.randomBase64UUID();
        final String refreshToken = UUIDs.randomBase64UUID();
        tokenService.createOAuth2Tokens(userTokenId, refreshToken, authentication, authentication, Collections.emptyMap(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            listener.onFailure(new NoShardAvailableActionException(new ShardId(new Index("foo", "uuid"), 0), "shard oh shard"));
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), anyActionListener());

        final SecurityIndexManager tokensIndex;
        if (oldNode != null) {
            tokensIndex = securityMainIndex;
            when(securityTokensIndex.isAvailable()).thenReturn(false);
            when(securityTokensIndex.indexExists()).thenReturn(false);
            when(securityTokensIndex.freeze()).thenReturn(securityTokensIndex);
        } else {
            tokensIndex = securityTokensIndex;
            when(securityMainIndex.isAvailable()).thenReturn(false);
            when(securityMainIndex.indexExists()).thenReturn(false);
            when(securityMainIndex.freeze()).thenReturn(securityMainIndex);
        }
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken3 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken3, future);
            assertNull(future.get());

            when(tokensIndex.isAvailable()).thenReturn(false);
            when(tokensIndex.getUnavailableReason()).thenReturn(new UnavailableShardsException(null, "unavailable"));
            when(tokensIndex.indexExists()).thenReturn(true);
            future = new PlainActionFuture<>();
            final SecureString bearerToken2 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken2, future);
            assertNull(future.get());

            when(tokensIndex.indexExists()).thenReturn(false);
            future = new PlainActionFuture<>();
            final SecureString bearerToken1 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken1, future);
            assertNull(future.get());

            when(tokensIndex.isAvailable()).thenReturn(true);
            when(tokensIndex.indexExists()).thenReturn(true);
            mockGetTokenFromId(tokenService, userTokenId, authentication, false);
            future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertAuthentication(future.get().getAuthentication(), authentication);
        }
    }

    public void testGetAuthenticationWorksWithExpiredUserToken() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, Clock.systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        final String userTokenId = UUIDs.randomBase64UUID();
        UserToken expired = new UserToken(
            userTokenId,
            tokenService.getTokenVersionCompatibility(),
            authentication,
            Instant.now().minus(3L, ChronoUnit.DAYS),
            Collections.emptyMap()
        );
        mockGetTokenFromId(expired, false);
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            userTokenId
        );
        PlainActionFuture<Tuple<Authentication, Map<String, Object>>> authFuture = new PlainActionFuture<>();
        tokenService.getAuthenticationAndMetadata(accessToken, authFuture);
        Authentication retrievedAuth = authFuture.actionGet().v1();
        assertAuthentication(authentication, retrievedAuth);
    }

    public void testSupersedingTokenEncryption() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, Clock.systemUTC());
        Authentication authentication = AuthenticationTests.randomAuthentication(null, null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        final String refrehToken = UUIDs.randomBase64UUID();
        final String newAccessToken = UUIDs.randomBase64UUID();
        final String newRefreshToken = UUIDs.randomBase64UUID();
        final byte[] iv = tokenService.getRandomBytes(TokenService.IV_BYTES);
        final byte[] salt = tokenService.getRandomBytes(TokenService.SALT_BYTES);
        final TransportVersion version = tokenService.getTokenVersionCompatibility();
        String encryptedTokens = tokenService.encryptSupersedingTokens(newAccessToken, newRefreshToken, refrehToken, iv, salt);
        RefreshTokenStatus refreshTokenStatus = newRefreshTokenStatus(
            false,
            authentication,
            true,
            Instant.now().minusSeconds(5L),
            encryptedTokens,
            Base64.getEncoder().encodeToString(iv),
            Base64.getEncoder().encodeToString(salt)
        );
        refreshTokenStatus.setTransportVersion(version);
        mockGetTokenAsyncForDecryptedToken(newAccessToken);
        tokenService.decryptAndReturnSupersedingTokens(refrehToken, refreshTokenStatus, securityTokensIndex, authentication, tokenFuture);
        if (version.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            // previous versions serialized the access token encrypted and the cipher text was different each time (due to different IVs)
            assertThat(
                tokenService.prependVersionAndEncodeAccessToken(version, newAccessToken),
                equalTo(tokenFuture.get().getAccessToken())
            );
        }
        assertThat(
            TokenService.prependVersionAndEncodeRefreshToken(version, newRefreshToken),
            equalTo(tokenFuture.get().getRefreshToken())
        );
    }

    public void testCannotValidateTokenIfLicenseDoesNotAllowTokens() throws Exception {
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, Clock.systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        final String userTokenId = UUIDs.randomBase64UUID();
        UserToken token = new UserToken(
            userTokenId,
            tokenService.getTokenVersionCompatibility(),
            authentication,
            Instant.now().plusSeconds(180),
            Collections.emptyMap()
        );
        mockGetTokenFromId(token, false);
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            userTokenId
        );
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(threadContext, tokenService.prependVersionAndEncodeAccessToken(token.getTransportVersion(), accessToken));

        PlainActionFuture<UserToken> authFuture = new PlainActionFuture<>();
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(false);
        final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(threadContext);
        tokenService.tryAuthenticateToken(bearerToken, authFuture);
        UserToken authToken = authFuture.actionGet();
        assertThat(authToken, Matchers.nullValue());
    }

    public void testHashedTokenIsUrlSafe() {
        final String hashedId = TokenService.hashTokenString(UUIDs.randomBase64UUID());
        assertEquals(hashedId, URLEncoder.encode(hashedId, StandardCharsets.UTF_8));
    }

    private TokenService createTokenService(Settings settings, Clock clock) throws GeneralSecurityException {
        return new TokenService(
            settings,
            clock,
            client,
            licenseState,
            securityContext,
            securityMainIndex,
            securityTokensIndex,
            clusterService
        );
    }

    private void mockGetTokenFromId(TokenService tokenService, String accessToken, Authentication authentication, boolean isExpired) {
        mockGetTokenFromId(tokenService, accessToken, authentication, Map.of(), isExpired, client);
    }

    public static void mockGetTokenFromId(
        TokenService tokenService,
        String userTokenId,
        Authentication authentication,
        Map<String, Object> metadata,
        boolean isExpired,
        Client client
    ) {
        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            GetResponse response = mock(GetResponse.class);
            TransportVersion tokenVersion = tokenService.getTokenVersionCompatibility();
            final String possiblyHashedUserTokenId;
            if (tokenVersion.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
                possiblyHashedUserTokenId = TokenService.hashTokenString(userTokenId);
            } else {
                possiblyHashedUserTokenId = userTokenId;
            }
            if (possiblyHashedUserTokenId.equals(request.id().replace("token_", ""))) {
                when(response.isExists()).thenReturn(true);
                Map<String, Object> sourceMap = new HashMap<>();
                final UserToken userToken = buildUserToken(tokenService, userTokenId, authentication, metadata);
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    userToken.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    Map<String, Object> accessTokenMap = new HashMap<>();
                    accessTokenMap.put(
                        "user_token",
                        XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false)
                    );
                    accessTokenMap.put("invalidated", isExpired);
                    sourceMap.put("access_token", accessTokenMap);
                }
                when(response.getSource()).thenReturn(sourceMap);
            }
            listener.onResponse(response);
            return null;
        }).when(client).get(any(GetRequest.class), anyActionListener());
    }

    protected static UserToken buildUserToken(
        TokenService tokenService,
        String userTokenId,
        Authentication authentication,
        Map<String, Object> metadata
    ) {
        final TransportVersion tokenVersion = tokenService.getTokenVersionCompatibility();
        final String possiblyHashedUserTokenId;
        if (tokenVersion.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            possiblyHashedUserTokenId = TokenService.hashTokenString(userTokenId);
        } else {
            possiblyHashedUserTokenId = userTokenId;
        }

        final Authentication tokenAuth = authentication.token().maybeRewriteForOlderVersion(tokenVersion);
        return new UserToken(possiblyHashedUserTokenId, tokenVersion, tokenAuth, tokenService.getExpirationTime(), metadata);
    }

    private void mockGetTokenFromId(UserToken userToken, boolean isExpired) {
        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            GetResponse response = mock(GetResponse.class);
            final String possiblyHashedUserTokenId;
            if (userToken.getTransportVersion().onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
                possiblyHashedUserTokenId = TokenService.hashTokenString(userToken.getId());
            } else {
                possiblyHashedUserTokenId = userToken.getId();
            }
            if (possiblyHashedUserTokenId.equals(request.id().replace("token_", ""))) {
                when(response.isExists()).thenReturn(true);
                Map<String, Object> sourceMap = new HashMap<>();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    userToken.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    Map<String, Object> accessTokenMap = new HashMap<>();
                    Map<String, Object> userTokenMap = XContentHelper.convertToMap(
                        XContentType.JSON.xContent(),
                        Strings.toString(builder),
                        false
                    );
                    userTokenMap.put("id", possiblyHashedUserTokenId);
                    accessTokenMap.put("user_token", userTokenMap);
                    accessTokenMap.put("invalidated", isExpired);
                    sourceMap.put("access_token", accessTokenMap);
                }
                when(response.getSource()).thenReturn(sourceMap);
            }
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), anyActionListener());
    }

    private void mockFindTokenFromRefreshToken(String refreshToken, UserToken userToken, @Nullable RefreshTokenStatus refreshTokenStatus) {
        String storedRefreshToken;
        if (userToken.getTransportVersion().onOrAfter(TokenService.VERSION_HASHED_TOKENS)) {
            storedRefreshToken = TokenService.hashTokenString(refreshToken);
        } else {
            storedRefreshToken = refreshToken;
        }
        doAnswer(invocationOnMock -> {
            final SearchRequest request = (SearchRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[1];
            final SearchResponse response = mock(SearchResponse.class);

            assertThat(request.source().query(), instanceOf(BoolQueryBuilder.class));
            BoolQueryBuilder bool = (BoolQueryBuilder) request.source().query();
            assertThat(bool.filter(), hasSize(2));

            assertThat(bool.filter().get(0), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder docType = (TermQueryBuilder) bool.filter().get(0);
            assertThat(docType.fieldName(), is("doc_type"));
            assertThat(docType.value(), is("token"));

            assertThat(bool.filter().get(1), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder refreshFilter = (TermQueryBuilder) bool.filter().get(1);
            assertThat(refreshFilter.fieldName(), is("refresh_token.token"));
            assertThat(refreshFilter.value(), is(storedRefreshToken));

            final RealmRef realmRef = new RealmRef(
                refreshTokenStatus == null ? randomAlphaOfLength(6) : refreshTokenStatus.getAssociatedRealm(),
                "test",
                randomAlphaOfLength(12)
            );
            final Authentication clientAuthentication = AuthenticationTestHelper.builder()
                .user(new User(refreshTokenStatus == null ? randomAlphaOfLength(8) : refreshTokenStatus.getAssociatedUser()))
                .realmRef(realmRef)
                .build(false);

            final SearchHit hit = new SearchHit(randomInt(), "token_" + TokenService.hashTokenString(userToken.getId()));
            BytesReference source = TokenService.createTokenDocument(userToken, storedRefreshToken, clientAuthentication, Instant.now());
            if (refreshTokenStatus != null) {
                var sourceAsMap = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
                @SuppressWarnings("unchecked")
                var refreshTokenSource = (Map<String, Object>) sourceAsMap.get("refresh_token");
                refreshTokenSource.put("invalidated", refreshTokenStatus.isInvalidated());
                refreshTokenSource.put("refreshed", refreshTokenStatus.isRefreshed());
                source = XContentTestUtils.convertToXContent(sourceAsMap, XContentType.JSON);
            }
            hit.sourceRef(source);

            final SearchHits hits = new SearchHits(new SearchHit[] { hit }, null, 1);
            when(response.getHits()).thenReturn(hits);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).search(any(SearchRequest.class), any());
    }

    private void mockGetTokenAsyncForDecryptedToken(String accessToken) {
        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            GetResponse response = mock(GetResponse.class);
            if (request.id().replace("token_", "").equals(TokenService.hashTokenString(accessToken))) {
                when(response.isExists()).thenReturn(true);
            }
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).get(any(GetRequest.class), anyActionListener());
    }

    public static void assertAuthentication(Authentication result, Authentication expected) {
        assertEquals(expected.getEffectiveSubject().getUser(), result.getEffectiveSubject().getUser());
        assertEquals(expected.getAuthenticatingSubject().getRealm(), result.getAuthenticatingSubject().getRealm());
        assertEquals(expected.isRunAs(), result.isRunAs());
        assertEquals(expected.getEffectiveSubject().getRealm(), result.getEffectiveSubject().getRealm());
        assertEquals(expected.getAuthenticatingSubject().getMetadata(), result.getAuthenticatingSubject().getMetadata());
    }

    private static DiscoveryNode addAnotherDataNodeWithVersion(
        ClusterService clusterService,
        Version version,
        TransportVersion transportVersion
    ) {
        final ClusterState currentState = clusterService.state();
        final DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder(currentState.getNodes());
        final DiscoveryNode anotherDataNode = DiscoveryNodeUtils.builder("another_data_node#" + version)
            .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
            .version(version)
            .build();
        discoBuilder.add(anotherDataNode);
        final ClusterState.Builder newStateBuilder = ClusterState.builder(currentState);
        newStateBuilder.nodes(discoBuilder);
        newStateBuilder.putTransportVersion(anotherDataNode.getId(), transportVersion);
        setState(clusterService, newStateBuilder.build());
        return anotherDataNode;
    }

    private String generateAccessToken(TokenService tokenService, TransportVersion version) throws Exception {
        String accessTokenString = UUIDs.randomBase64UUID();
        if (version.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            accessTokenString = TokenService.hashTokenString(accessTokenString);
        }
        return tokenService.prependVersionAndEncodeAccessToken(version, accessTokenString);
    }
}
