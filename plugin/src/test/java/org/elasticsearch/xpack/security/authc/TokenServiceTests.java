/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
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
import org.junit.After;
import org.junit.Before;

import javax.crypto.SecretKey;
import java.time.Clock;
import java.util.Base64;

import static org.elasticsearch.repositories.ESBlobStoreTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenServiceTests extends ESTestCase {

    private InternalClient internalClient;
    private ThreadPool threadPool;
    private Client client;
    private SecurityLifecycleService lifecycleService;

    @Before
    public void setupClient() {
        client = mock(Client.class);
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests").build();
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
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
    }

    @After
    public void shutdownThreadpool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testAttachAndGetToken() throws Exception {
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);
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
            TokenService anotherService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            UserToken fromOtherService = future.get();
            assertEquals(authentication, fromOtherService.getAuthentication());
        }
    }

    public void testPassphraseWorks() throws Exception {
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);
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
            TokenService anotherService = new TokenService(settings, Clock.systemUTC(), internalClient, lifecycleService);
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            anotherService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }
    }

    public void testInvalidatedToken() throws Exception {
        when(lifecycleService.isSecurityIndexAvailable()).thenReturn(true);
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);
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
        SecretKey key = TokenService.computeSecretKey(TokenService.DEFAULT_PASSPHRASE.toCharArray(), saltArr);
        SecretKey key2 = TokenService.computeSecretKey(TokenService.DEFAULT_PASSPHRASE.toCharArray(), saltArr);
        assertArrayEquals(key.getEncoded(), key2.getEncoded());
    }

    public void testTokenExpiry() throws Exception {
        ClockMock clock = ClockMock.frozen();
        TokenService tokenService = new TokenService(Settings.EMPTY, clock, internalClient, lifecycleService);
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
                Clock.systemUTC(), internalClient, lifecycleService);
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
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);

        ThreadContext requestContext = new ThreadContext(Settings.EMPTY);
        requestContext.putHeader("Authorization", "Bearer " + Base64.getEncoder().encodeToString(randomBytes));

        try (ThreadContext.StoredContext ignore = requestContext.newStoredContext(true)) {
            PlainActionFuture<UserToken> future = new PlainActionFuture<>();
            tokenService.getAndValidateToken(requestContext, future);
            assertNull(future.get());
        }
    }

    public void testIndexNotAvailable() throws Exception {
        TokenService tokenService = new TokenService(Settings.EMPTY, Clock.systemUTC(), internalClient, lifecycleService);
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
}
