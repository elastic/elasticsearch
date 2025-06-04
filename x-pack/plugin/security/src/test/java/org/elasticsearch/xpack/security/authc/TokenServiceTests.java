/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
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
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import static java.time.Clock.systemUTC;
import static org.elasticsearch.common.hash.MessageDigests.sha256;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.security.authc.TokenService.RAW_TOKEN_BYTES_LENGTH;
import static org.elasticsearch.xpack.security.authc.TokenService.RAW_TOKEN_BYTES_TOTAL_LENGTH;
import static org.elasticsearch.xpack.security.authc.TokenService.RAW_TOKEN_DOC_ID_BYTES_LENGTH;
import static org.elasticsearch.xpack.security.authc.TokenService.VERSION_CLIENT_AUTH_FOR_REFRESH;
import static org.elasticsearch.xpack.security.authc.TokenService.VERSION_GET_TOKEN_DOC_FOR_REFRESH;
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
    private static ThreadContext.StoredContext defaultContext;
    private static final Settings settings = Settings.builder()
        .put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();

    private Client client;
    private SecurityIndexManager securityMainIndex;
    private SecurityIndexManager securityTokensIndex;
    private ClusterService clusterService;
    private DiscoveryNode pre72OldNode;
    private DiscoveryNode pre8500040OldNode;
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
            GetRequestBuilder builder = new GetRequestBuilder(client);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString());
        when(client.prepareIndex(any(String.class))).thenReturn(new IndexRequestBuilder(client));
        when(client.prepareBulk()).thenReturn(new BulkRequestBuilder(client));
        when(client.prepareUpdate(any(String.class), any(String.class))).thenAnswer(inv -> {
            final String index = (String) inv.getArguments()[0];
            final String id = (String) inv.getArguments()[1];
            return new UpdateRequestBuilder(client).setIndex(index).setId(id);
        });
        when(client.prepareSearch(any(String.class))).thenReturn(new SearchRequestBuilder(client));
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
        }).when(client).execute(eq(TransportIndexAction.TYPE), any(IndexRequest.class), anyActionListener());
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

        try (var ignored = threadPool.getThreadContext().newStoredContext()) {
            defaultContext.restore();
            this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
        }

        // License state (enabled by default)
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);

        if (randomBoolean()) {
            // version 7.2 was an "inflection" point in the Token Service development (access_tokens as UUIDS, multiple concurrent
            // refreshes,
            // tokens docs on a separate index)
            pre72OldNode = addAnother7071DataNode(this.clusterService);
        }
        if (randomBoolean()) {
            // before refresh tokens used GET, i.e. TokenService#VERSION_GET_TOKEN_DOC_FOR_REFRESH
            pre8500040OldNode = addAnotherPre8500DataNode(this.clusterService);
        }
    }

    private static DiscoveryNode addAnother7071DataNode(ClusterService clusterService) {
        Version version;
        TransportVersion transportVersion;
        if (randomBoolean()) {
            version = Version.V_7_0_0;
            transportVersion = TransportVersions.V_7_0_0;
        } else {
            version = Version.V_7_1_0;
            transportVersion = TransportVersions.V_7_1_0;
        }
        return addAnotherDataNodeWithVersion(clusterService, version, transportVersion);
    }

    private static DiscoveryNode addAnotherPre8500DataNode(ClusterService clusterService) {
        Version version;
        TransportVersion transportVersion;
        if (randomBoolean()) {
            version = Version.V_8_8_1;
            transportVersion = TransportVersions.V_8_8_1;
        } else {
            version = Version.V_8_9_0;
            transportVersion = TransportVersions.V_8_9_X;
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
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders(),
            new FixedExecutorBuilder(
                settings,
                TokenService.THREAD_POOL_NAME,
                1,
                1000,
                "xpack.security.authc.token.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        defaultContext = threadPool.getThreadContext().newStoredContext();
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
        // This test only makes sense in mixed clusters with pre v7.2.0 nodes where the Token Service Key is used (to encrypt tokens)
        if (null == pre72OldNode) {
            pre72OldNode = addAnother7071DataNode(this.clusterService);
        }
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            authentication,
            authentication,
            Collections.emptyMap(),
            tokenFuture
        );
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, null);

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
        if (null == pre72OldNode) {
            pre72OldNode = addAnother7071DataNode(this.clusterService);
        }
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            authentication,
            authentication,
            Collections.emptyMap(),
            tokenFuture
        );
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, null);

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
        if (null == pre72OldNode) {
            pre72OldNode = addAnother7071DataNode(this.clusterService);
        }
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);

        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            authentication,
            authentication,
            Collections.emptyMap(),
            tokenFuture
        );
        String accessToken = tokenFuture.get().getAccessToken();
        assertThat(accessToken, notNullValue());

        tokenService.clearActiveKeyCache();

        tokenService.createOAuth2Tokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            authentication,
            authentication,
            Collections.emptyMap(),
            tokenFuture
        );
        accessToken = tokenFuture.get().getAccessToken();
        assertThat(accessToken, notNullValue());
    }

    public void testAuthnWithInvalidatedToken() throws Exception {
        IndexState projectIndex = securityMainIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(newTokenBytes.v1(), newTokenBytes.v2(), authentication, authentication, Map.of(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        assertNotNull(accessToken);
        // mock token as invalidated
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, true, null);

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
        IndexState projectIndex = securityMainIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        tokenService.createOAuth2Tokens(newTokenBytes.v1(), newTokenBytes.v2(), authentication, authentication, Map.of(), tokenFuture);
        final TokenService.CreateTokenResult tokenResult = tokenFuture.get(180, TimeUnit.SECONDS);
        final String accessToken = tokenResult.getAccessToken();
        final String clientRefreshToken = tokenResult.getRefreshToken();
        assertNotNull(accessToken);
        assertNotNull(clientRefreshToken);
        mockTokenForRefreshToken(newTokenBytes.v1(), newTokenBytes.v2(), tokenService, authentication, null);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(requestContext, accessToken);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<TokensInvalidationResult> future = new PlainActionFuture<>();
            tokenService.invalidateRefreshToken(clientRefreshToken, future);
            final TokensInvalidationResult result = future.get();
            assertThat(result.getInvalidatedTokens(), hasSize(1));
            assertThat(result.getPreviouslyInvalidatedTokens(), empty());
            assertThat(result.getErrors(), empty());
            PlainActionFuture<TokenService.CreateTokenResult> refreshTokenFuture = new PlainActionFuture<>();
            tokenService.refreshToken(clientRefreshToken, refreshTokenFuture);
            ExecutionException e = expectThrows(ExecutionException.class, refreshTokenFuture::get);
            assertThat(e.getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(e.getCause().toString(), containsString("invalid_grant"));
        }
    }

    public void testInvalidateRefreshTokenThatIsAlreadyInvalidated() throws Exception {
        IndexState projectIndex = securityMainIndex.forCurrentProject();
        when(projectIndex.indexExists()).thenReturn(true);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTests.randomAuthentication(null, null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        tokenService.createOAuth2Tokens(newTokenBytes.v1(), newTokenBytes.v2(), authentication, authentication, Map.of(), tokenFuture);
        final String accessToken = tokenFuture.get().getAccessToken();
        final String clientRefreshToken = tokenFuture.get().getRefreshToken();
        assertNotNull(accessToken);
        assertNotNull(clientRefreshToken);
        mockTokenForRefreshToken(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            tokenService,
            authentication,
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
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        UserToken userToken = mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, null);
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            newTokenBytes.v1()
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

    public void testMalformedAccessTokens() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        byte[] accessTokenBytes = tokenService.getRandomTokenBytes(randomBoolean()).v1();
        mockGetTokenFromAccessTokenBytes(tokenService, accessTokenBytes, authentication, false, null);
        String mockedAccessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            accessTokenBytes
        );
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        // test some random access tokens
        for (int numBytes = 1; numBytes < TokenService.MINIMUM_BYTES + 32; numBytes++) {
            final byte[] randomBytes = new byte[numBytes];
            random().nextBytes(randomBytes);
            String testAccessToken = Base64.getEncoder().encodeToString(randomBytes);
            assumeFalse("Test token must be different from mock", mockedAccessToken.equals(testAccessToken));
            try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
                storeTokenHeader(requestContext, testAccessToken);
                PlainActionFuture<UserToken> future = new PlainActionFuture<>();
                final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
                tokenService.tryAuthenticateToken(bearerToken, future);
                assertNull(future.get());
            }
        }
        // test garbled mocked access tokens
        for (int garbledByteIdx = 0; garbledByteIdx < accessTokenBytes.length; garbledByteIdx++) {
            final byte[] garbledAccessToken = new byte[accessTokenBytes.length];
            System.arraycopy(accessTokenBytes, 0, garbledAccessToken, 0, accessTokenBytes.length);
            garbledAccessToken[garbledByteIdx] = (byte) (garbledAccessToken[garbledByteIdx] ^ (byte) (random().nextInt(255) + 1));
            String testAccessToken = tokenService.prependVersionAndEncodeAccessToken(
                tokenService.getTokenVersionCompatibility(),
                garbledAccessToken
            );
            try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
                storeTokenHeader(requestContext, testAccessToken);
                PlainActionFuture<UserToken> future = new PlainActionFuture<>();
                final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
                tokenService.tryAuthenticateToken(bearerToken, future);
                assertNull(future.get());
            }
        }
    }

    public void testMalformedRefreshTokens() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        tokenService.createOAuth2Tokens(newTokenBytes.v1(), newTokenBytes.v2(), authentication, authentication, Map.of(), tokenFuture);
        byte[] mockedRawRefreshToken = newTokenBytes.v2();
        String mockedClientRefreshToken = tokenFuture.get().getRefreshToken();
        assertNotNull(mockedClientRefreshToken);
        mockTokenForRefreshToken(newTokenBytes.v1(), newTokenBytes.v2(), tokenService, authentication, null);
        // test some random access tokens
        for (int numBytes = 1; numBytes < RAW_TOKEN_BYTES_TOTAL_LENGTH + 8; numBytes++) {
            final byte[] randomBytes = new byte[numBytes];
            random().nextBytes(randomBytes);
            String testRefreshToken = Base64.getEncoder().encodeToString(randomBytes);
            assumeFalse("Test token must be different from mock", mockedClientRefreshToken.equals(testRefreshToken));
            PlainActionFuture<TokensInvalidationResult> future = new PlainActionFuture<>();
            tokenService.invalidateRefreshToken(testRefreshToken, future);
            final TokensInvalidationResult result = future.get();
            assertThat(result.getInvalidatedTokens(), hasSize(0));
            assertThat(result.getPreviouslyInvalidatedTokens(), empty());
            assertThat(result.getErrors(), empty());
            assertThat(result.getRestStatus(), is(RestStatus.NOT_FOUND));
        }
        // test garbled mocked refresh tokens
        for (int garbledByteIdx = 0; garbledByteIdx < mockedRawRefreshToken.length; garbledByteIdx++) {
            final byte[] garbledRefreshToken = new byte[mockedRawRefreshToken.length];
            System.arraycopy(mockedRawRefreshToken, 0, garbledRefreshToken, 0, mockedRawRefreshToken.length);
            garbledRefreshToken[garbledByteIdx] = (byte) (garbledRefreshToken[garbledByteIdx] ^ (byte) (random().nextInt(255) + 1));
            String testRefreshToken = TokenService.prependVersionAndEncodeRefreshToken(
                tokenService.getTokenVersionCompatibility(),
                garbledRefreshToken
            );
            PlainActionFuture<TokensInvalidationResult> future = new PlainActionFuture<>();
            tokenService.invalidateRefreshToken(testRefreshToken, future);
            final TokensInvalidationResult result = future.get();
            assertThat(result.getInvalidatedTokens(), hasSize(0));
            assertThat(result.getPreviouslyInvalidatedTokens(), empty());
            assertThat(result.getErrors(), empty());
            assertThat(result.getRestStatus(), is(RestStatus.NOT_FOUND));
        }
    }

    public void testNonExistingPre72Token() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromAccessTokenBytes(tokenService, tokenService.getRandomTokenBytes(randomBoolean()).v1(), authentication, false, null);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(
            requestContext,
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersions.V_7_1_0,
                tokenService.getRandomTokenBytes(TransportVersions.V_7_1_0, randomBoolean()).v1()
            )
        );

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testNonExistingUUIDToken() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromAccessTokenBytes(tokenService, tokenService.getRandomTokenBytes(randomBoolean()).v1(), authentication, false, null);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        TransportVersion uuidTokenVersion = randomFrom(TransportVersions.V_7_2_0, TransportVersions.V_7_3_2);
        storeTokenHeader(
            requestContext,
            tokenService.prependVersionAndEncodeAccessToken(
                uuidTokenVersion,
                tokenService.getRandomTokenBytes(uuidTokenVersion, randomBoolean()).v1()
            )
        );

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken, future);
            assertNull(future.get());
        }
    }

    public void testNonExistingLatestTokenVersion() throws Exception {
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, systemUTC());
        // mock another random token so that we don't find a token in TokenService#getUserTokenFromId
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new RealmRef("native_realm", "native", "node1"))
            .build(false);
        mockGetTokenFromAccessTokenBytes(tokenService, tokenService.getRandomTokenBytes(randomBoolean()).v1(), authentication, false, null);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(
            requestContext,
            tokenService.prependVersionAndEncodeAccessToken(
                TransportVersion.current(),
                tokenService.getRandomTokenBytes(TransportVersion.current(), randomBoolean()).v1()
            )
        );

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
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        tokenService.createOAuth2Tokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            authentication,
            authentication,
            Collections.emptyMap(),
            tokenFuture
        );
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
        if (pre72OldNode != null) {
            tokensIndex = securityMainIndex;
            final IndexState projectTokenIndex = securityTokensIndex.forCurrentProject();
            when(projectTokenIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
            when(projectTokenIndex.indexExists()).thenReturn(false);
        } else {
            tokensIndex = securityTokensIndex;
            final IndexState projectMainIndex = securityMainIndex.forCurrentProject();
            when(projectMainIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
            when(projectMainIndex.indexExists()).thenReturn(false);
        }

        IndexState projectIndex = tokensIndex.forCurrentProject();
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContextPreservingResponseHeaders()) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            final SecureString bearerToken3 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken3, future);
            assertNull(future.get());

            when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
            when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(
                new UnavailableShardsException(null, "unavailable")
            );
            when(projectIndex.indexExists()).thenReturn(true);
            future = new PlainActionFuture<>();
            final SecureString bearerToken2 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken2, future);
            assertNull(future.get());

            when(projectIndex.indexExists()).thenReturn(false);
            future = new PlainActionFuture<>();
            final SecureString bearerToken1 = Authenticator.extractBearerTokenFromHeader(requestContext);
            tokenService.tryAuthenticateToken(bearerToken1, future);
            assertNull(future.get());

            when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
            when(projectIndex.indexExists()).thenReturn(true);
            mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, null);
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
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, Instant.now().minus(3L, ChronoUnit.DAYS));
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            newTokenBytes.v1()
        );
        PlainActionFuture<Tuple<Authentication, Map<String, Object>>> authFuture = new PlainActionFuture<>();
        tokenService.getAuthenticationAndMetadata(accessToken, authFuture);
        Authentication retrievedAuth = authFuture.actionGet().v1();
        assertAuthentication(authentication, retrievedAuth);
    }

    public void testSupersedingTokenEncryption() throws Exception {
        assumeTrue("Superseding tokens are only created in post 7.2 clusters", pre72OldNode == null);
        TokenService tokenService = createTokenService(tokenServiceEnabledSettings, Clock.systemUTC());
        Authentication authentication = AuthenticationTests.randomAuthentication(null, null);
        PlainActionFuture<TokenService.CreateTokenResult> tokenFuture = new PlainActionFuture<>();
        byte[] refreshTokenBytes = new byte[RAW_TOKEN_BYTES_TOTAL_LENGTH];
        random().nextBytes(refreshTokenBytes);
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        final byte[] iv = tokenService.getRandomBytes(TokenService.IV_BYTES);
        final byte[] salt = tokenService.getRandomBytes(TokenService.SALT_BYTES);
        final TransportVersion version = tokenService.getTokenVersionCompatibility();
        String encryptedTokens = tokenService.encryptSupersedingTokens(
            newTokenBytes.v1(),
            newTokenBytes.v2(),
            Base64.getUrlEncoder().encodeToString(refreshTokenBytes),
            iv,
            salt
        );
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
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, null);
        tokenService.decryptAndReturnSupersedingTokens(
            Base64.getUrlEncoder().encodeToString(refreshTokenBytes),
            refreshTokenStatus,
            securityTokensIndex,
            authentication,
            tokenFuture
        );
        if (version.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            // previous versions serialized the access token encrypted and the cipher text was different each time (due to different IVs)
            assertThat(
                tokenService.prependVersionAndEncodeAccessToken(version, newTokenBytes.v1()),
                equalTo(tokenFuture.get().getAccessToken())
            );
        }
        assertThat(
            TokenService.prependVersionAndEncodeRefreshToken(version, newTokenBytes.v2()),
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
        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(randomBoolean());
        mockGetTokenFromAccessTokenBytes(tokenService, newTokenBytes.v1(), authentication, false, Instant.now().plusSeconds(180));
        final String accessToken = tokenService.prependVersionAndEncodeAccessToken(
            tokenService.getTokenVersionCompatibility(),
            newTokenBytes.v1()
        );
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        storeTokenHeader(threadContext, accessToken);

        PlainActionFuture<UserToken> authFuture = new PlainActionFuture<>();
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(false);
        final SecureString bearerToken = Authenticator.extractBearerTokenFromHeader(threadContext);
        tokenService.tryAuthenticateToken(bearerToken, authFuture);
        UserToken authToken = authFuture.actionGet();
        assertThat(authToken, Matchers.nullValue());
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

    private UserToken mockGetTokenFromAccessTokenBytes(
        TokenService tokenService,
        byte[] accessTokenBytes,
        Authentication authentication,
        boolean isInvalidated,
        @Nullable Instant expirationTime
    ) {
        return mockGetTokenFromAccessTokenBytes(
            tokenService,
            accessTokenBytes,
            authentication,
            Map.of(),
            isInvalidated,
            expirationTime,
            client
        );
    }

    public static UserToken mockGetTokenFromAccessTokenBytes(
        TokenService tokenService,
        byte[] accessTokenBytes,
        Authentication authentication,
        Map<String, Object> metadata,
        boolean isInvalidated,
        @Nullable Instant expirationTime,
        Client client
    ) {
        final UserToken userToken = buildUserToken(tokenService, accessTokenBytes, authentication, expirationTime, metadata);
        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
            GetResponse response = mock(GetResponse.class);
            if (userToken.getId().equals(request.id().replace("token_", ""))) {
                when(response.isExists()).thenReturn(true);
                Map<String, Object> sourceMap = new HashMap<>();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    userToken.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    Map<String, Object> accessTokenMap = new HashMap<>();
                    accessTokenMap.put(
                        "user_token",
                        XContentHelper.convertToMap(XContentType.JSON.xContent(), Strings.toString(builder), false)
                    );
                    accessTokenMap.put("invalidated", isInvalidated);
                    if (userToken.getTransportVersion().onOrAfter(VERSION_GET_TOKEN_DOC_FOR_REFRESH)) {
                        accessTokenMap.put(
                            "token",
                            Base64.getUrlEncoder().withoutPadding().encodeToString(sha256().digest(accessTokenBytes))
                        );
                    }
                    sourceMap.put("access_token", accessTokenMap);
                }
                when(response.getSource()).thenReturn(sourceMap);
            }
            listener.onResponse(response);
            return null;
        }).when(client).get(any(GetRequest.class), anyActionListener());
        return userToken;
    }

    protected static UserToken buildUserToken(
        TokenService tokenService,
        byte[] accessTokenBytes,
        Authentication authentication,
        @Nullable Instant expirationTime,
        Map<String, Object> metadata
    ) {
        final TransportVersion tokenVersion = tokenService.getTokenVersionCompatibility();
        final String userTokenId = tokenDocIdFromAccessTokenBytes(accessTokenBytes, tokenVersion);
        final Authentication tokenAuth = authentication.token().maybeRewriteForOlderVersion(tokenVersion);
        return new UserToken(
            userTokenId,
            tokenVersion,
            tokenAuth,
            expirationTime != null ? expirationTime : tokenService.getExpirationTime(),
            metadata
        );
    }

    // public for tests
    public static String tokenDocIdFromAccessTokenBytes(byte[] accessTokenBytes, TransportVersion tokenVersion) {
        if (tokenVersion.onOrAfter(TokenService.VERSION_GET_TOKEN_DOC_FOR_REFRESH)) {
            MessageDigest userTokenIdDigest = sha256();
            userTokenIdDigest.update(accessTokenBytes, RAW_TOKEN_BYTES_LENGTH, RAW_TOKEN_DOC_ID_BYTES_LENGTH);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(userTokenIdDigest.digest());
        } else if (tokenVersion.onOrAfter(TokenService.VERSION_ACCESS_TOKENS_AS_UUIDS)) {
            return TokenService.hashTokenString(Base64.getUrlEncoder().withoutPadding().encodeToString(accessTokenBytes));
        } else {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(accessTokenBytes);
        }
    }

    private void mockTokenForRefreshToken(
        byte[] accessTokenBytes,
        byte[] refreshTokenBytes,
        TokenService tokenService,
        Authentication authentication,
        @Nullable RefreshTokenStatus refreshTokenStatus
    ) throws IOException {
        UserToken userToken = buildUserToken(tokenService, accessTokenBytes, authentication, null, Map.of());
        final String storedAccessToken;
        final String storedRefreshToken;
        if (userToken.getTransportVersion().onOrAfter(VERSION_GET_TOKEN_DOC_FOR_REFRESH)) {
            storedAccessToken = Base64.getUrlEncoder().withoutPadding().encodeToString(sha256().digest(accessTokenBytes));
            storedRefreshToken = Base64.getUrlEncoder().withoutPadding().encodeToString(sha256().digest(refreshTokenBytes));
        } else if (userToken.getTransportVersion().onOrAfter(TokenService.VERSION_HASHED_TOKENS)) {
            storedAccessToken = null;
            storedRefreshToken = TokenService.hashTokenString(Base64.getUrlEncoder().withoutPadding().encodeToString(refreshTokenBytes));
        } else {
            storedAccessToken = null;
            storedRefreshToken = Base64.getUrlEncoder().withoutPadding().encodeToString(refreshTokenBytes);
        }
        final RealmRef realmRef = new RealmRef(
            refreshTokenStatus == null ? randomAlphaOfLength(6) : refreshTokenStatus.getAssociatedRealm(),
            "test",
            randomAlphaOfLength(12)
        );
        final Authentication clientAuthentication = AuthenticationTestHelper.builder()
            .user(new User(refreshTokenStatus == null ? randomAlphaOfLength(8) : refreshTokenStatus.getAssociatedUser()))
            .realmRef(realmRef)
            .build(false);

        BytesReference source = TokenService.createTokenDocument(
            userToken,
            storedAccessToken,
            storedRefreshToken,
            clientAuthentication,
            Instant.now()
        );
        if (refreshTokenStatus != null) {
            var sourceAsMap = XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
            @SuppressWarnings("unchecked")
            var refreshTokenSource = (Map<String, Object>) sourceAsMap.get("refresh_token");
            refreshTokenSource.put("invalidated", refreshTokenStatus.isInvalidated());
            refreshTokenSource.put("refreshed", refreshTokenStatus.isRefreshed());
            source = XContentTestUtils.convertToXContent(sourceAsMap, XContentType.JSON);
        }
        final BytesReference docSource = source;
        if (userToken.getTransportVersion().onOrAfter(VERSION_GET_TOKEN_DOC_FOR_REFRESH)) {
            doAnswer(invocationOnMock -> {
                GetRequest request = (GetRequest) invocationOnMock.getArguments()[0];
                @SuppressWarnings("unchecked")
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[1];
                GetResponse response = mock(GetResponse.class);
                if (userToken.getId().equals(request.id().substring("token_".length()))) {
                    when(response.isExists()).thenReturn(true);
                    when(response.getSource()).thenReturn(XContentHelper.convertToMap(docSource, false, XContentType.JSON).v2());
                }
                listener.onResponse(response);
                return null;
            }).when(client).get(any(GetRequest.class), anyActionListener());
        } else {
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
                final SearchHits hits;
                if (storedRefreshToken.equals(refreshFilter.value())) {
                    SearchHit hit = SearchHit.unpooled(randomInt(), "token_" + userToken.getId());
                    hit.sourceRef(docSource);
                    hits = SearchHits.unpooled(new SearchHit[] { hit }, null, 1);
                } else {
                    hits = SearchHits.EMPTY_WITH_TOTAL_HITS;
                }
                when(response.getHits()).thenReturn(hits);
                listener.onResponse(response);
                return Void.TYPE;
            }).when(client).search(any(SearchRequest.class), any());
        }
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
        newStateBuilder.putCompatibilityVersions(anotherDataNode.getId(), transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS);
        setState(clusterService, newStateBuilder.build());
        return anotherDataNode;
    }
}
