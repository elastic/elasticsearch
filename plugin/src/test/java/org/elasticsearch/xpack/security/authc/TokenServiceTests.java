/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.TokenService.BytesKey;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenServiceTests extends ESTestCase {

    private InternalClient internalClient;
    private static ThreadPool threadPool;
    private static final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();

    private Client client;
    private SecurityLifecycleService lifecycleService;
    private ClusterService clusterService;
    private Settings tokenServiceEnabledSettings = Settings.builder()
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();

    @Before
    public void setupClient() throws GeneralSecurityException {
        client = mock(Client.class);
        internalClient = new InternalClient(settings, threadPool, client);
        lifecycleService = mock(SecurityLifecycleService.class);
        when(lifecycleService.isSecurityIndexWriteable()).thenReturn(true);
        doAnswer(invocationOnMock -> {
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[2];
            GetResponse response = mock(GetResponse.class);
            when(response.isExists()).thenReturn(false);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), any(ActionListener.class));
        when(client.threadPool()).thenReturn(threadPool);
        this.clusterService = new ClusterService(settings, new ClusterSettings(settings, ClusterSettings
                .BUILT_IN_CLUSTER_SETTINGS), threadPool, Collections.emptyMap());
    }

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
    }

    @AfterClass
    public static void shutdownThreadpool() throws InterruptedException {
        terminate(threadPool);
        threadPool = null;
    }

    public void testAttachAndGetToken() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // verify a second separate token service with its own salt can also verify
            TokenService anotherService = new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService
                    , clusterService);
            anotherService.refreshMetaData(tokenService.getTokenMetaData());
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            UserToken fromOtherService = future.get();
            assertEquals(authentication, fromOtherService.getAuthentication());
        }
    }

    public void testRotateKey() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }
        rotateKeys(tokenService);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }

        final UserToken newToken = tokenService.createUserToken(authentication);
        assertNotNull(newToken);
        assertNotEquals(tokenService.getUserTokenString(newToken), tokenService.getUserTokenString(token));

        requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(newToken));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }
    }

    private void rotateKeys(TokenService tokenService) {
        TokenMetaData tokenMetaData = tokenService.generateSpareKey();
        tokenService.refreshMetaData(tokenMetaData);
        tokenMetaData = tokenService.rotateToSpareKey();
        tokenService.refreshMetaData(tokenMetaData);
    }

    public void testKeyExchange() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        int numRotations = 0;randomIntBetween(1, 5);
        for (int i = 0; i < numRotations; i++) {
            rotateKeys(tokenService);
        }
        TokenService otherTokenService = new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService,
                clusterService);
        otherTokenService.refreshMetaData(tokenService.getTokenMetaData());
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            otherTokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }

        rotateKeys(tokenService);

        otherTokenService.refreshMetaData(tokenService.getTokenMetaData());

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            otherTokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }
    }

    public void testPruneKeys() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
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
            assertEquals(authentication, serialized.getAuthentication());
        }

        final UserToken newToken = tokenService.createUserToken(authentication);
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

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }

    }

    public void testPassphraseWorks() throws Exception {
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // verify a second separate token service with its own passphrase cannot verify
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), randomAlphaOfLengthBetween(8, 30));
            Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
            TokenService anotherService = new TokenService(settings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }
    }

    public void testInvalidatedToken() throws Exception {
        when(lifecycleService.isSecurityIndexAvailable()).thenReturn(true);
        TokenService tokenService =
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);
        doAnswer(invocationOnMock -> {
            GetRequest request = (GetRequest) invocationOnMock.getArguments()[1];
            assertEquals(TokenService.DOC_TYPE + "_" + token.getId(), request.id());
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocationOnMock.getArguments()[2];
            GetResponse response = mock(GetResponse.class);
            when(response.isExists()).thenReturn(true);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(eq(GetAction.INSTANCE), any(GetRequest.class), any(ActionListener.class));

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
            final String headerValue = e.getHeader("WWW-Authenticate").get(0);
            assertThat(headerValue, containsString("Bearer realm="));
            assertThat(headerValue, containsString("expired"));
        }
    }

    public void testComputeSecretKeyIsConsistent() throws Exception {
        byte[] saltArr = new byte[32];
        random().nextBytes(saltArr);
        SecretKey key = TokenService.computeSecretKey("some random passphrase".toCharArray(), saltArr);
        SecretKey key2 = TokenService.computeSecretKey("some random passphrase".toCharArray(), saltArr);
        assertArrayEquals(key.getEncoded(), key2.getEncoded());
    }

    public void testTokenExpiry() throws Exception {
        ClockMock clock = ClockMock.frozen();
        TokenService tokenService = new TokenService(tokenServiceEnabledSettings, clock, internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // the clock is still frozen, so the cookie should be valid
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertEquals(authentication, future.get().getAuthentication());
        }

        final TimeValue defaultExpiration = TokenService.TOKEN_EXPIRATION.get(Settings.EMPTY);
        final int fastForwardAmount = randomIntBetween(1, Math.toIntExact(defaultExpiration.getSeconds()));
        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // move the clock forward but don't go to expiry
            clock.fastForwardSeconds(fastForwardAmount);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertEquals(authentication, future.get().getAuthentication());
        }

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            // move to expiry
            clock.fastForwardSeconds(Math.toIntExact(defaultExpiration.getSeconds()) - fastForwardAmount);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertEquals(authentication, future.get().getAuthentication());
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
    }

    public void testTokenServiceDisabled() throws Exception {
        TokenService tokenService = new TokenService(Settings.builder()
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), false)
                .build(),
                Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> tokenService.createUserToken(null));
        assertEquals("tokens are not enabled", e.getMessage());

        PlainActionFuture<UserToken> future = new PlainActionFuture<>();
        tokenService.getAndValidateToken(null, future);
        assertNull(future.get());

        e = expectThrows(IllegalStateException.class, () -> {
            PlainActionFuture<Boolean> invalidateFuture = new PlainActionFuture<>();
            tokenService.invalidateToken(null, invalidateFuture);
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
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService, clusterService);

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
            new TokenService(tokenServiceEnabledSettings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        final UserToken token = tokenService.createUserToken(authentication);
        assertNotNull(token);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + tokenService.getUserTokenString(token));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            UserToken serialized = future.get();
            assertEquals(authentication, serialized.getAuthentication());

            when(lifecycleService.isSecurityIndexAvailable()).thenReturn(false);
            when(lifecycleService.isSecurityIndexExisting()).thenReturn(true);
            future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }
    }


    public void testDecodePre6xToken() throws GeneralSecurityException, ExecutionException, InterruptedException, IOException {
        String token = "g+y0AiDWsbLNzUGTywPa3VCz053RUPW7wAx4xTAonlcqjOmO1AzMhQDTUku/+ZtdtMgDobKqIrNdNvchvFMX0pvZLY6i4nAG2OhkApSstPfQQP" +
                "J1fxg/JZNQDPufePg1GxV/RAQm2Gr8mYAelijEVlWIdYaQ3R76U+P/w6Q1v90dGVZQn6DKMOfgmkfwAFNY";
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(TokenService.TOKEN_PASSPHRASE.getKey(), "xpack_token_passpharse");
        Settings settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).setSecureSettings(secureSettings).build();
        TokenService tokenService = new TokenService(settings, Clock.systemUTC(), internalClient, lifecycleService, clusterService);
        Authentication authentication = new Authentication(new User("joe", "admin"), new RealmRef("native_realm", "native", "node1"), null);
        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + token);

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.decodeToken(tokenService.getFromHeader(requestContext), future);
            UserToken serialized = future.get();
            assertNotNull(serialized);
            assertEquals("joe", serialized.getAuthentication().getUser().principal());
            assertEquals(Version.V_5_6_0, serialized.getAuthentication().getVersion());
            assertArrayEquals(new String[] {"admin"}, serialized.getAuthentication().getUser().roles());
        }
    }
}
