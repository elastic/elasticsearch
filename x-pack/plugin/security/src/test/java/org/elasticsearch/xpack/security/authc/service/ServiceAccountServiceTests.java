/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountServiceTests extends ESTestCase {

    private static final ServiceAccountId USER_MANAGED_ACCOUNT_ID = new ServiceAccountId("engineering", "deploy_bot");
    private static final ServiceAccountId BUILT_IN_ACCOUNT_ID = new ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet-server");

    private Client client;
    private ThreadPool threadPool;
    private FileServiceAccountTokenStore fileServiceAccountTokenStore;
    private IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private UserManagedServiceAccountStore userManagedServiceAccountStore;
    private ServiceAccountService serviceAccountService;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws UnknownHostException {
        threadPool = new TestThreadPool("service account service tests");
        fileServiceAccountTokenStore = mock(FileServiceAccountTokenStore.class);
        indexServiceAccountTokenStore = mock(IndexServiceAccountTokenStore.class);
        userManagedServiceAccountStore = mock(UserManagedServiceAccountStore.class);
        when(fileServiceAccountTokenStore.getTokenSource()).thenReturn(TokenInfo.TokenSource.FILE);
        when(indexServiceAccountTokenStore.getTokenSource()).thenReturn(TokenInfo.TokenSource.INDEX);
        stubNoUserManagedAccounts();
        stubListAccounts(List.of());
        stubTokenAuthentication(fileServiceAccountTokenStore, false);
        stubTokenAuthentication(indexServiceAccountTokenStore, false);
        stubHasTokensFor(false);
        stubDeleteAccount(false);
        stubTokenWrites();
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        serviceAccountService = newServiceAccountService(userManagedServiceAccountStore);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    private ServiceAccountService newServiceAccountService(@Nullable UserManagedServiceAccountStore accountStore) {
        return new ServiceAccountService(
            client,
            new CompositeServiceAccountTokenStore(
                List.of(fileServiceAccountTokenStore, indexServiceAccountTokenStore),
                threadPool.getThreadContext()
            ),
            indexServiceAccountTokenStore,
            accountStore
        );
    }

    public void testGetBuiltInServiceAccountPrincipals() {
        assertThat(
            ServiceAccountService.getBuiltInServiceAccountPrincipals(),
            containsInAnyOrder("elastic/auto-ops", "elastic/fleet-server", "elastic/fleet-server-remote", "elastic/kibana")
        );
    }

    /**
     * A user-managed account whose tokens have nowhere to live could never authenticate, so the combination is refused
     * at construction rather than left to fail per request.
     */
    public void testUserManagedAccountsRequireAnIndexBackedTokenStore() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServiceAccountService(
                client,
                new CompositeServiceAccountTokenStore(List.of(fileServiceAccountTokenStore), threadPool.getThreadContext()),
                null,
                userManagedServiceAccountStore
            )
        );
        assertThat(
            e.getMessage(),
            equalTo("cannot support user-managed service accounts without an index-backed service account token store")
        );
    }

    public void testTryParseToken() throws IOException {
        // Null for null
        assertNull(ServiceAccountService.tryParseToken(null));

        final byte[] magicBytes = { 0, 1, 0, 1 };

        final Logger satLogger = LogManager.getLogger(ServiceAccountToken.class);
        Loggers.setLevel(satLogger, Level.TRACE);
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        try (var satMockLog = MockLog.capture(ServiceAccountToken.class); var sasMockLog = MockLog.capture(ServiceAccountService.class)) {
            // Less than 4 bytes
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "less than 4 bytes",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "service account token expects the 4 leading bytes"
                )
            );
            final SecureString bearerString0 = createBearerString(List.of(Arrays.copyOfRange(magicBytes, 0, randomIntBetween(0, 3))));
            assertNull(ServiceAccountService.tryParseToken(bearerString0));
            satMockLog.assertAllExpectationsMatched();

            // Prefix mismatch
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "prefix mismatch",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "service account token expects the 4 leading bytes"
                )
            );
            final SecureString bearerString1 = createBearerString(
                List.of(
                    new byte[] { randomValueOtherThan((byte) 0, ESTestCase::randomByte) },
                    randomByteArrayOfLength(randomIntBetween(30, 50))
                )
            );
            assertNull(ServiceAccountService.tryParseToken(bearerString1));
            satMockLog.assertAllExpectationsMatched();

            // No colon
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "no colon",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "failed to extract qualified service token name and secret, missing ':'"
                )
            );
            final SecureString bearerString2 = createBearerString(
                List.of(magicBytes, randomAlphaOfLengthBetween(30, 50).getBytes(StandardCharsets.UTF_8))
            );
            assertNull(ServiceAccountService.tryParseToken(bearerString2));
            satMockLog.assertAllExpectationsMatched();

            // Invalid delimiter for qualified name
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid delimiter for qualified name",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
                )
            );
            if (randomBoolean()) {
                final SecureString bearerString3 = createBearerString(
                    List.of(
                        magicBytes,
                        (randomAlphaOfLengthBetween(10, 20) + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                    )
                );
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            } else {
                final SecureString bearerString3 = createBearerString(
                    List.of(
                        magicBytes,
                        (randomAlphaOfLengthBetween(3, 8)
                            + "/"
                            + randomAlphaOfLengthBetween(3, 8)
                            + ":"
                            + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                    )
                );
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            }
            satMockLog.assertAllExpectationsMatched();

            // Invalid token name
            sasMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid token name",
                    ServiceAccountService.class.getName(),
                    Level.TRACE,
                    "Cannot parse possible service account token"
                )
            );
            final SecureString bearerString4 = createBearerString(
                List.of(
                    magicBytes,
                    (randomAlphaOfLengthBetween(3, 8)
                        + "/"
                        + randomAlphaOfLengthBetween(3, 8)
                        + "/"
                        + randomValueOtherThanMany(n -> n.contains("/"), ValidationTests::randomInvalidTokenName)
                        + ":"
                        + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                )
            );
            assertNull(ServiceAccountService.tryParseToken(bearerString4));
            sasMockLog.assertAllExpectationsMatched();

            // Everything is good
            final String namespace = randomAlphaOfLengthBetween(3, 8);
            final String serviceName = randomAlphaOfLengthBetween(3, 8);
            final String tokenName = ValidationTests.randomTokenName();
            final ServiceAccountId accountId = new ServiceAccountId(namespace, serviceName);
            final String secret = randomAlphaOfLengthBetween(10, 20);
            final SecureString bearerString5 = createBearerString(
                List.of(magicBytes, (namespace + "/" + serviceName + "/" + tokenName + ":" + secret).getBytes(StandardCharsets.UTF_8))
            );
            final ServiceAccountToken serviceAccountToken1 = ServiceAccountService.tryParseToken(bearerString5);

            assertNotNull(serviceAccountToken1);
            assertThat(serviceAccountToken1.getAccountId(), equalTo(accountId));
            assertThat(serviceAccountToken1.getTokenName(), equalTo(tokenName));
            assertThat(serviceAccountToken1.getSecret(), equalTo(new SecureString(secret.toCharArray())));

            // Serialise and de-serialise service account token
            final ServiceAccountToken parsedToken = ServiceAccountService.tryParseToken(serviceAccountToken1.asBearerString());
            assertThat(parsedToken, equalTo(serviceAccountToken1));

            // Invalid magic byte
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid magic byte again",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "service account token expects the 4 leading bytes"
                )
            );
            assertNull(
                ServiceAccountService.tryParseToken(new SecureString("AQEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray()))
            );
            satMockLog.assertAllExpectationsMatched();

            // No colon
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "no colon again",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "failed to extract qualified service token name and secret, missing ':'"
                )
            );
            assertNull(
                ServiceAccountService.tryParseToken(new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xX3N1cGVyc2VjcmV0".toCharArray()))
            );
            satMockLog.assertAllExpectationsMatched();

            // Invalid qualified name
            satMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid delimiter for qualified name again",
                    ServiceAccountToken.class.getName(),
                    Level.TRACE,
                    "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
                )
            );
            assertNull(
                ServiceAccountService.tryParseToken(new SecureString("AAEAAWVsYXN0aWMvZmxlZXRfdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray()))
            );
            satMockLog.assertAllExpectationsMatched();

            // Invalid token name
            sasMockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid token name again",
                    ServiceAccountService.class.getName(),
                    Level.TRACE,
                    "Cannot parse possible service account token"
                )
            );
            assertNull(
                ServiceAccountService.tryParseToken(new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4hOnN1cGVyc2VjcmV0".toCharArray()))
            );
            sasMockLog.assertAllExpectationsMatched();

            ServiceAccountToken parsedServiceAccountToken = ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpzdXBlcnNlY3JldA".toCharArray())
            );

            // everything is fine
            assertNotNull(parsedServiceAccountToken);
            assertThat(parsedServiceAccountToken.getAccountId(), equalTo(new ServiceAccountId("elastic", "fleet-server")));
            assertThat(parsedServiceAccountToken.getTokenName(), equalTo("token1"));
            assertThat(parsedServiceAccountToken.getSecret(), equalTo(new SecureString("supersecret".toCharArray())));
        } finally {
            Loggers.setLevel(satLogger, Level.INFO);
            Loggers.setLevel(sasLogger, Level.INFO);
        }
    }

    private ServiceAccountToken newMockServiceAccountToken(ServiceAccountId accountId, String tokenName, SecureString secret) {
        ServiceAccountToken serviceAccountToken = mock(ServiceAccountToken.class);
        var serviceAccountTokenId = new ServiceAccountToken.ServiceAccountTokenId(accountId, tokenName);
        when(serviceAccountToken.getQualifiedName()).thenReturn(serviceAccountTokenId.getQualifiedName());
        when(serviceAccountToken.getSecret()).thenReturn(secret);
        when(serviceAccountToken.getAccountId()).thenReturn(accountId);
        when(serviceAccountToken.getTokenName()).thenReturn(tokenName);
        return serviceAccountToken;
    }

    public void testTryAuthenticateBearerToken() throws ExecutionException, InterruptedException {
        // Valid token
        final PlainActionFuture<Authentication> future5 = new PlainActionFuture<>();

        final CachingServiceAccountTokenStore authenticatingStore = randomFrom(fileServiceAccountTokenStore, indexServiceAccountTokenStore);
        Stream.of(fileServiceAccountTokenStore, indexServiceAccountTokenStore).forEach(store -> {
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener = (ActionListener<
                    ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(
                    ServiceAccountTokenStore.StoreAuthenticationResult.fromBooleanResult(
                        store.getTokenSource(),
                        store == authenticatingStore
                    )
                );
                return null;
            }).when(store).authenticate(any(), any());
        });

        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        serviceAccountService.authenticateToken(
            newMockServiceAccountToken(
                new ServiceAccountId("elastic", "fleet-server"),
                "token1",
                new SecureString("super-secret-value".toCharArray())
            ),
            nodeName,
            future5
        );
        assertThat(
            future5.get(),
            equalTo(
                Authentication.newServiceAccountAuthentication(
                    new User(
                        "elastic/fleet-server",
                        Strings.EMPTY_ARRAY,
                        "Service account - elastic/fleet-server",
                        null,
                        Map.of("_elastic_service_account", true),
                        true
                    ),
                    nodeName,
                    Map.of("_token_name", "token1", "_token_source", authenticatingStore.getTokenSource().name().toLowerCase(Locale.ROOT))
                )
            )
        );
    }

    public void testAuthenticateWithToken() throws ExecutionException, InterruptedException, IllegalAccessException {
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        try (var mockLog = MockLog.capture(ServiceAccountService.class)) {
            // A namespace outside the reserved one now names a user-managed account, so it is looked for in the
            // account store rather than rejected outright.
            final ServiceAccountId accountId1 = new ServiceAccountId(
                randomValueOtherThan(ElasticServiceAccounts.NAMESPACE, () -> randomAlphaOfLengthBetween(3, 8)),
                randomAlphaOfLengthBetween(3, 8)
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unknown user-managed service account",
                    ServiceAccountService.class.getName(),
                    Level.DEBUG,
                    "the [" + accountId1.asPrincipal() + "] user-managed service account does not exist or is disabled"
                )
            );
            final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
            final ServiceAccountToken token1 = newMockServiceAccountToken(accountId1, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future1 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token1, randomAlphaOfLengthBetween(3, 8), future1);
            final ExecutionException e1 = expectThrows(ExecutionException.class, future1::get);
            assertThat(e1.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(
                e1.getMessage(),
                containsString(
                    "failed to authenticate service account ["
                        + token1.getAccountId().asPrincipal()
                        + "] with token name ["
                        + token1.getTokenName()
                        + "]"
                )
            );
            mockLog.assertAllExpectationsMatched();

            // Unknown elastic service name
            final ServiceAccountId accountId2 = new ServiceAccountId(
                ElasticServiceAccounts.NAMESPACE,
                randomValueOtherThanMany(
                    serviceName -> ServiceAccountService.isBuiltInServiceAccountPrincipal(
                        ElasticServiceAccounts.NAMESPACE + "/" + serviceName
                    ),
                    () -> randomAlphaOfLengthBetween(3, 8)
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unknown elastic service name",
                    ServiceAccountService.class.getName(),
                    Level.DEBUG,
                    "the [" + accountId2.asPrincipal() + "] service account does not exist"
                )
            );
            final ServiceAccountToken token2 = newMockServiceAccountToken(accountId2, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future2 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token2, randomAlphaOfLengthBetween(3, 8), future2);
            final ExecutionException e2 = expectThrows(ExecutionException.class, future2::get);
            assertThat(e2.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(
                e2.getMessage(),
                containsString(
                    "failed to authenticate service account ["
                        + token2.getAccountId().asPrincipal()
                        + "] with token name ["
                        + token2.getTokenName()
                        + "]"
                )
            );
            mockLog.assertAllExpectationsMatched();

            // Length of secret value is too short
            final ServiceAccountId accountId3 = new ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet-server");
            final SecureString secret3 = new SecureString(randomAlphaOfLengthBetween(1, 9).toCharArray());
            final ServiceAccountToken token3 = newMockServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8), secret3);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "secret value too short",
                    ServiceAccountService.class.getName(),
                    Level.DEBUG,
                    "the provided credential has length ["
                        + secret3.length()
                        + "] but a token's secret value must be at least [10] characters"
                )
            );
            final PlainActionFuture<Authentication> future3 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token3, randomAlphaOfLengthBetween(3, 8), future3);
            final ExecutionException e3 = expectThrows(ExecutionException.class, future3::get);
            assertThat(e3.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(
                e3.getMessage(),
                containsString(
                    "failed to authenticate service account ["
                        + token3.getAccountId().asPrincipal()
                        + "] with token name ["
                        + token3.getTokenName()
                        + "]"
                )
            );
            mockLog.assertAllExpectationsMatched();

            final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.FILE, TokenInfo.TokenSource.INDEX);
            final CachingServiceAccountTokenStore store;
            final CachingServiceAccountTokenStore otherStore;
            if (tokenSource == TokenInfo.TokenSource.FILE) {
                store = fileServiceAccountTokenStore;
                otherStore = indexServiceAccountTokenStore;
            } else {
                store = indexServiceAccountTokenStore;
                otherStore = fileServiceAccountTokenStore;
            }

            // Success based on credential store
            final ServiceAccountId accountId4 = new ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet-server");
            final ServiceAccountToken token4 = newMockServiceAccountToken(accountId4, randomAlphaOfLengthBetween(3, 8), secret);
            final ServiceAccountToken token5 = newMockServiceAccountToken(
                accountId4,
                randomAlphaOfLengthBetween(3, 8),
                new SecureString(randomAlphaOfLength(20).toCharArray())
            );
            final String nodeName = randomAlphaOfLengthBetween(3, 8);
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener = (ActionListener<
                    ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(ServiceAccountTokenStore.StoreAuthenticationResult.successful(store.getTokenSource()));
                return null;
            }).when(store).authenticate(eq(token4), any());

            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener = (ActionListener<
                    ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(ServiceAccountTokenStore.StoreAuthenticationResult.failed(store.getTokenSource()));
                return null;
            }).when(store).authenticate(eq(token5), any());

            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener = (ActionListener<
                    ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(ServiceAccountTokenStore.StoreAuthenticationResult.failed(otherStore.getTokenSource()));
                return null;
            }).when(otherStore).authenticate(any(), any());

            final PlainActionFuture<Authentication> future4 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token4, nodeName, future4);
            final Authentication authentication = future4.get();
            assertThat(
                authentication,
                equalTo(
                    Authentication.newServiceAccountAuthentication(
                        new User(
                            "elastic/fleet-server",
                            Strings.EMPTY_ARRAY,
                            "Service account - elastic/fleet-server",
                            null,
                            Map.of("_elastic_service_account", true),
                            true
                        ),
                        nodeName,
                        Map.of("_token_name", token4.getTokenName(), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT))
                    )
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "invalid credential",
                    ServiceAccountService.class.getName(),
                    Level.DEBUG,
                    "failed to authenticate service account ["
                        + token5.getAccountId().asPrincipal()
                        + "] with token name ["
                        + token5.getTokenName()
                        + "]"
                )
            );
            final PlainActionFuture<Authentication> future5 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token5, nodeName, future5);
            final ExecutionException e5 = expectThrows(ExecutionException.class, future5::get);
            assertThat(e5.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(
                e5.getMessage(),
                containsString(
                    "failed to authenticate service account ["
                        + token5.getAccountId().asPrincipal()
                        + "] with token name ["
                        + token5.getTokenName()
                        + "]"
                )
            );
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(sasLogger, Level.INFO);
        }
    }

    public void testGetRoleDescriptor() throws ExecutionException, InterruptedException {
        final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
        final Authentication auth1 = Authentication.newServiceAccountAuthentication(
            new User(
                "elastic/fleet-server",
                Strings.EMPTY_ARRAY,
                "Service account - elastic/fleet-server",
                null,
                Map.of("_elastic_service_account", true),
                true
            ),
            randomAlphaOfLengthBetween(3, 8),
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT))
        );

        final PlainActionFuture<RoleDescriptor> future1 = new PlainActionFuture<>();
        ServiceAccountService.getRoleDescriptor(auth1, future1);
        final RoleDescriptor roleDescriptor1 = future1.get();
        assertNotNull(roleDescriptor1);
        assertThat(roleDescriptor1.getName(), equalTo("elastic/fleet-server"));

        final String username = randomValueOtherThan(
            "elastic/fleet-server",
            () -> randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)
        );
        final Authentication auth2 = Authentication.newServiceAccountAuthentication(
            new User(username, Strings.EMPTY_ARRAY, "Service account - " + username, null, Map.of("_elastic_service_account", true), true),
            randomAlphaOfLengthBetween(3, 8),
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT))
        );
        final PlainActionFuture<RoleDescriptor> future2 = new PlainActionFuture<>();
        ServiceAccountService.getRoleDescriptor(auth2, future2);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future2::actionGet);
        assertThat(
            e.getMessage(),
            containsString("cannot load role for built-in service account [" + username + "] - no such service account")
        );
    }

    /**
     * The reserved namespace is the whole of what selects the built-in path, so an account name in it must never be
     * looked for among the user-managed accounts — including one no built-in account carries.
     */
    public void testReservedNamespaceIsNeverResolvedAgainstTheUserManagedStore() {
        final ServiceAccountId accountId = new ServiceAccountId(
            ElasticServiceAccounts.NAMESPACE,
            randomValueOtherThanMany(
                serviceName -> ServiceAccountService.isBuiltInServiceAccountPrincipal(ElasticServiceAccounts.NAMESPACE + "/" + serviceName),
                () -> randomAlphaOfLengthBetween(3, 8)
            )
        );
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        serviceAccountService.authenticateToken(newTokenFor(accountId), randomAlphaOfLengthBetween(3, 8), future);
        expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        verify(userManagedServiceAccountStore, never()).getByPrincipal(any(), any());
    }

    public void testUserManagedTokenAuthenticatesAsTheStoredAccountsNamedRoles() {
        final UserManagedServiceAccount account = new UserManagedServiceAccount(
            USER_MANAGED_ACCOUNT_ID,
            List.of("deploy_bot_role_a", "deploy_bot_role_b"),
            true
        );
        stubUserManagedAccount(account);
        stubIndexTokenAuthentication(true);

        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        serviceAccountService.authenticateToken(newTokenFor(USER_MANAGED_ACCOUNT_ID), nodeName, future);

        final Authentication authentication = future.actionGet();
        final User user = authentication.getEffectiveSubject().getUser();
        assertThat(user.principal(), equalTo(USER_MANAGED_ACCOUNT_ID.asPrincipal()));
        assertThat(user.roles(), arrayContaining("deploy_bot_role_a", "deploy_bot_role_b"));
        // Without the marker the authorization layer would resolve a built-in account of the same name instead of the
        // roles above, so the routing decision has to survive into the Authentication this method produces.
        assertThat(authentication.isUserManagedServiceAccount(), is(true));
        // A file-backed token is written by an operator for a built-in account, so it must not be able to authenticate
        // a user-managed one. Only the index-backed store is consulted, never the composite the built-in path uses.
        verify(fileServiceAccountTokenStore, never()).authenticate(any(), any());
    }

    public void testUserManagedTokenIsRejectedWhenTheCredentialDoesNotVerify() {
        stubUserManagedAccount(enabledAccount());
        stubIndexTokenAuthentication(false);

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        serviceAccountService.authenticateToken(newTokenFor(USER_MANAGED_ACCOUNT_ID), randomAlphaOfLengthBetween(3, 8), future);
        assertUnauthorized(future);
        verify(userManagedServiceAccountStore).getByPrincipal(eq(USER_MANAGED_ACCOUNT_ID.asPrincipal()), any());
    }

    /**
     * Resolving the account before verifying the credential is what makes disabling — and deleting — an account take
     * effect immediately, rather than once the token cache expires.
     */
    public void testDisablingAnAccountDeniesItsTokensWithoutConsultingTheTokenStore() {
        stubUserManagedAccount(new UserManagedServiceAccount(USER_MANAGED_ACCOUNT_ID, List.of("deploy_bot_role_a"), false));

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        serviceAccountService.authenticateToken(newTokenFor(USER_MANAGED_ACCOUNT_ID), randomAlphaOfLengthBetween(3, 8), future);
        assertUnauthorized(future);
        verify(indexServiceAccountTokenStore, never()).authenticate(any(), any());
    }

    public void testUserManagedTokenIsRejectedWhereTheAccountStoreIsNotConfigured() {
        final ServiceAccountService service = newServiceAccountService(null);
        stubUserManagedAccount(enabledAccount());
        stubIndexTokenAuthentication(true);

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticateToken(newTokenFor(USER_MANAGED_ACCOUNT_ID), randomAlphaOfLengthBetween(3, 8), future);
        assertUnauthorized(future);
        // Even a token that would otherwise verify must not authenticate: with no account store there is no account.
        verify(indexServiceAccountTokenStore, never()).authenticate(any(), any());
    }

    /**
     * The check sits above the namespace fork, so it must hold for both kinds of account — asserted in one run rather
     * than one kind per run, since a check that had slipped into a single branch would otherwise pass half the time.
     */
    public void testAnUnderLengthSecretIsRejectedBeforeAnyLookup() {
        stubUserManagedAccount(enabledAccount());
        stubIndexTokenAuthentication(true);

        for (ServiceAccountId accountId : List.of(BUILT_IN_ACCOUNT_ID, USER_MANAGED_ACCOUNT_ID)) {
            final ServiceAccountToken token = newMockServiceAccountToken(
                accountId,
                randomAlphaOfLengthBetween(3, 8),
                new SecureString(randomAlphaOfLengthBetween(1, 9).toCharArray())
            );
            final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token, randomAlphaOfLengthBetween(3, 8), future);
            assertUnauthorized(future);
        }
        verify(userManagedServiceAccountStore, never()).getByPrincipal(any(), any());
        verify(indexServiceAccountTokenStore, never()).authenticate(any(), any());
    }

    public void testCreateBuiltInTokenWillDelegate() {
        final Authentication authentication = AuthenticationTestHelper.builder().serviceAccount().build();
        final CreateServiceAccountTokenRequest request = newCreateTokenRequest(BUILT_IN_ACCOUNT_ID);
        final ActionListener<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        serviceAccountService.createBuiltInToken(authentication, request, future);
        verify(indexServiceAccountTokenStore).createBuiltInToken(eq(authentication), eq(request), eq(future));
        verify(userManagedServiceAccountStore, never()).getByPrincipal(any(), any());
    }

    public void testDeleteBuiltInTokenWillDelegate() {
        final DeleteServiceAccountTokenRequest request = newDeleteTokenRequest(BUILT_IN_ACCOUNT_ID);
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteBuiltInToken(request, future);
        verify(indexServiceAccountTokenStore).deleteBuiltInToken(eq(request), eq(future));
    }

    public void testTheBuiltInTokenPathsDoNotReachAUserManagedAccount() {
        final CreateServiceAccountTokenRequest createRequest = newCreateTokenRequest(USER_MANAGED_ACCOUNT_ID);
        serviceAccountService.createBuiltInToken(
            AuthenticationTestHelper.builder().serviceAccount().build(),
            createRequest,
            new PlainActionFuture<>()
        );
        verify(indexServiceAccountTokenStore).createBuiltInToken(any(), eq(createRequest), any());
        verify(indexServiceAccountTokenStore, never()).createUserManagedToken(any(), any(), any());

        final DeleteServiceAccountTokenRequest deleteRequest = newDeleteTokenRequest(USER_MANAGED_ACCOUNT_ID);
        serviceAccountService.deleteBuiltInToken(deleteRequest, new PlainActionFuture<>());
        verify(indexServiceAccountTokenStore).deleteBuiltInToken(eq(deleteRequest), any());
        verify(indexServiceAccountTokenStore, never()).deleteUserManagedToken(any(), any());

        verify(userManagedServiceAccountStore, never()).getByPrincipal(any(), any());
    }

    /**
     * The account lookup here is the only thing standing between a caller and a working credential for an account that
     * does not exist: the token store writes whatever principal it is handed and cannot consult the account store.
     */
    public void testCreatingATokenRequiresTheUserManagedAccountToExist() {
        final Authentication authentication = AuthenticationTestHelper.builder().serviceAccount().build();
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        serviceAccountService.createUserManagedToken(authentication, newCreateTokenRequest(USER_MANAGED_ACCOUNT_ID), future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("service account [" + USER_MANAGED_ACCOUNT_ID + "] does not exist"));
        verify(indexServiceAccountTokenStore, never()).createUserManagedToken(any(), any(), any());
    }

    /**
     * A disabled account is suspended, not gone; a token created for it simply cannot authenticate until it is enabled
     * again, so there is nothing to be gained by refusing to prepare one.
     */
    public void testCreatingATokenIgnoresWhetherTheUserManagedAccountIsEnabled() {
        for (boolean enabled : List.of(true, false)) {
            clearInvocations(indexServiceAccountTokenStore);
            stubUserManagedAccount(new UserManagedServiceAccount(USER_MANAGED_ACCOUNT_ID, List.of("deploy_bot_role_a"), enabled));

            final Authentication authentication = AuthenticationTestHelper.builder().serviceAccount().build();
            final CreateServiceAccountTokenRequest request = newCreateTokenRequest(USER_MANAGED_ACCOUNT_ID);
            serviceAccountService.createUserManagedToken(authentication, request, new PlainActionFuture<>());
            verify(indexServiceAccountTokenStore).createUserManagedToken(eq(authentication), eq(request), any());
        }
    }

    /**
     * The create-token API accepts any namespace and has always answered a name no account carries with a request
     * fault. A node without an account store can hold no user-managed account, so it must keep giving that answer
     * rather than reporting the feature as unavailable: the caller can act on a name it got wrong, and turning a
     * client error into a server one would regress every caller that mistypes a namespace.
     */
    public void testCreatingATokenWhereTheAccountStoreIsNotConfiguredReportsTheAccountAsMissing() {
        final ServiceAccountService service = newServiceAccountService(null);
        final PlainActionFuture<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        service.createUserManagedToken(
            AuthenticationTestHelper.builder().serviceAccount().build(),
            newCreateTokenRequest(USER_MANAGED_ACCOUNT_ID),
            future
        );

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("service account [" + USER_MANAGED_ACCOUNT_ID + "] does not exist"));
        verify(indexServiceAccountTokenStore, never()).createUserManagedToken(any(), any(), any());
    }

    /**
     * Force-deleting an account leaves its tokens behind, and the credentials API still lists them. Requiring the
     * account to exist would make those leftovers permanently undeletable, so deleting a token resolves nothing.
     */
    public void testDeletingATokenDoesNotRequireTheUserManagedAccountToExist() {
        final DeleteServiceAccountTokenRequest request = newDeleteTokenRequest(USER_MANAGED_ACCOUNT_ID);
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteUserManagedToken(request, future);
        verify(indexServiceAccountTokenStore).deleteUserManagedToken(eq(request), eq(future));
        verify(userManagedServiceAccountStore, never()).getByPrincipal(any(), any());
    }

    public void testPutUserManagedAccountDelegatesToTheAccountStore() {
        final List<String> roles = randomList(1, 3, () -> randomAlphaOfLengthBetween(3, 8));
        final boolean enabled = randomBoolean();
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final UserManagedServiceAccountStore.PutResult result = randomFrom(UserManagedServiceAccountStore.PutResult.values());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UserManagedServiceAccountStore.PutResult> listener = (ActionListener<
                UserManagedServiceAccountStore.PutResult>) invocation.getArguments()[4];
            listener.onResponse(result);
            return null;
        }).when(userManagedServiceAccountStore).putAccount(any(), any(), anyBoolean(), any(), any());

        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        serviceAccountService.putUserManagedAccount(USER_MANAGED_ACCOUNT_ID, roles, enabled, refreshPolicy, future);
        assertThat(future.actionGet(), is(result));
        verify(userManagedServiceAccountStore).putAccount(eq(USER_MANAGED_ACCOUNT_ID), eq(roles), eq(enabled), eq(refreshPolicy), any());
    }

    public void testPutUserManagedAccountFailsWhereTheAccountStoreIsNotConfigured() {
        final ServiceAccountService service = newServiceAccountService(null);
        final PlainActionFuture<UserManagedServiceAccountStore.PutResult> future = new PlainActionFuture<>();
        service.putUserManagedAccount(USER_MANAGED_ACCOUNT_ID, List.of("a_role"), true, RefreshPolicy.NONE, future);

        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("user-managed service accounts are not available in this cluster configuration"));
        verify(userManagedServiceAccountStore, never()).putAccount(any(), any(), anyBoolean(), any(), any());
    }

    /**
     * Recreating an account of the same name would bring surviving tokens back to life, which is what the guard is
     * protecting against rather than any token being live in the meantime.
     */
    public void testDeletingAnAccountIsRefusedWhileItStillHasTokens() {
        stubHasTokensFor(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteUserManagedAccount(USER_MANAGED_ACCOUNT_ID, false, RefreshPolicy.NONE, future);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(
            e.getMessage(),
            equalTo(
                "cannot delete service account ["
                    + USER_MANAGED_ACCOUNT_ID
                    + "] because it has service tokens; delete the tokens first,"
                    + " or set force=true to delete the account and leave its tokens in place"
            )
        );
        verify(userManagedServiceAccountStore, never()).deleteAccount(any(), any(), any());
    }

    public void testDeletingAnAccountProceedsWhenItHasNoTokens() {
        stubHasTokensFor(false);
        final boolean found = randomBoolean();
        stubDeleteAccount(found);

        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteUserManagedAccount(USER_MANAGED_ACCOUNT_ID, false, refreshPolicy, future);
        assertThat(future.actionGet(), is(found));
        verify(userManagedServiceAccountStore).deleteAccount(eq(USER_MANAGED_ACCOUNT_ID), eq(refreshPolicy), any());
    }

    public void testForceDeletingAnAccountSkipsTheTokenCheck() {
        stubDeleteAccount(true);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteUserManagedAccount(USER_MANAGED_ACCOUNT_ID, true, RefreshPolicy.NONE, future);
        assertThat(future.actionGet(), is(true));
        verify(indexServiceAccountTokenStore, never()).hasTokensFor(any(), any());
    }

    public void testDeletingAnAccountFailsWhereTheAccountStoreIsNotConfigured() {
        final ServiceAccountService service = newServiceAccountService(null);
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        service.deleteUserManagedAccount(USER_MANAGED_ACCOUNT_ID, randomBoolean(), RefreshPolicy.NONE, future);

        final IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("user-managed service accounts are not available in this cluster configuration"));
        verify(indexServiceAccountTokenStore, never()).hasTokensFor(any(), any());
    }

    public void testGetUserManagedAccountInfosReportsWhatTheStoreHolds() {
        final UserManagedServiceAccount enabled = new UserManagedServiceAccount(USER_MANAGED_ACCOUNT_ID, List.of("role_a", "role_b"), true);
        final UserManagedServiceAccount disabled = new UserManagedServiceAccount(
            new ServiceAccountId("engineering", "audit_bot"),
            List.of(),
            false
        );
        stubListAccounts(List.of(enabled, disabled));

        final PlainActionFuture<List<ServiceAccountInfo>> future = new PlainActionFuture<>();
        serviceAccountService.getUserManagedAccountInfos("engineering", "deploy_bot", future);

        assertThat(
            future.actionGet(),
            contains(
                new ServiceAccountInfo.UserManaged("engineering/deploy_bot", List.of("role_a", "role_b"), true),
                new ServiceAccountInfo.UserManaged("engineering/audit_bot", List.of(), false)
            )
        );
        verify(userManagedServiceAccountStore).listAccounts(eq("engineering"), eq("deploy_bot"), any());
    }

    /**
     * Unlike writing an account, reading one is reachable through an API that also reports built-in accounts, so a
     * node that cannot hold user-managed accounts reports that it holds none rather than failing the whole read.
     */
    public void testGetUserManagedAccountInfosIsEmptyWhereTheAccountStoreIsNotConfigured() {
        final ServiceAccountService service = newServiceAccountService(null);
        final PlainActionFuture<List<ServiceAccountInfo>> future = new PlainActionFuture<>();
        service.getUserManagedAccountInfos(randomFrom("engineering", null), randomFrom("deploy_bot", null), future);
        assertThat(future.actionGet(), empty());
        verify(userManagedServiceAccountStore, never()).listAccounts(any(), any(), any());
    }

    public void testFindTokensFor() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountId accountId = new ServiceAccountId(namespace, serviceName);

        final List<TokenInfo> indexTokenInfos = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> TokenInfo.indexToken(ValidationTests.randomTokenName()))
            .sorted()
            .toList();

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<TokenInfo>> listener = (ActionListener<Collection<TokenInfo>>) args[1];
            listener.onResponse(indexTokenInfos);
            return null;
        }).when(indexServiceAccountTokenStore).findTokensFor(eq(accountId), any());

        final GetServiceAccountCredentialsNodesResponse fileTokensResponse = mock(GetServiceAccountCredentialsNodesResponse.class);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            @SuppressWarnings("unchecked")
            final ActionListener<GetServiceAccountCredentialsNodesResponse> listener = (ActionListener<
                GetServiceAccountCredentialsNodesResponse>) args[2];
            listener.onResponse(fileTokensResponse);
            return null;
        }).when(client)
            .execute(eq(GetServiceAccountNodesCredentialsAction.INSTANCE), any(GetServiceAccountCredentialsNodesRequest.class), any());

        final PlainActionFuture<GetServiceAccountCredentialsResponse> future = new PlainActionFuture<>();
        serviceAccountService.findTokensFor(new GetServiceAccountCredentialsRequest(namespace, serviceName), future);
        final GetServiceAccountCredentialsResponse response = future.actionGet();
        assertThat(response.getPrincipal(), equalTo(accountId.asPrincipal()));
        assertThat(response.getNodesResponse(), is(fileTokensResponse));
        assertThat(response.getIndexTokenInfos(), equalTo(indexTokenInfos));
    }

    private UserManagedServiceAccount enabledAccount() {
        return new UserManagedServiceAccount(USER_MANAGED_ACCOUNT_ID, List.of("deploy_bot_role_a"), true);
    }

    private ServiceAccountToken newTokenFor(ServiceAccountId accountId) {
        return newMockServiceAccountToken(
            accountId,
            randomAlphaOfLengthBetween(3, 8),
            new SecureString(randomAlphaOfLengthBetween(10, 20).toCharArray())
        );
    }

    private CreateServiceAccountTokenRequest newCreateTokenRequest(ServiceAccountId accountId) {
        return new CreateServiceAccountTokenRequest(accountId.namespace(), accountId.serviceName(), ValidationTests.randomTokenName());
    }

    private DeleteServiceAccountTokenRequest newDeleteTokenRequest(ServiceAccountId accountId) {
        return new DeleteServiceAccountTokenRequest(accountId.namespace(), accountId.serviceName(), ValidationTests.randomTokenName());
    }

    private void stubNoUserManagedAccounts() {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UserManagedServiceAccount> listener = (ActionListener<UserManagedServiceAccount>) invocation
                .getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(userManagedServiceAccountStore).getByPrincipal(any(), any());
    }

    private void stubUserManagedAccount(UserManagedServiceAccount account) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<UserManagedServiceAccount> listener = (ActionListener<UserManagedServiceAccount>) invocation
                .getArguments()[1];
            listener.onResponse(account);
            return null;
        }).when(userManagedServiceAccountStore).getByPrincipal(eq(account.id().asPrincipal()), any());
    }

    private void stubListAccounts(List<UserManagedServiceAccount> accounts) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<List<UserManagedServiceAccount>> listener = (ActionListener<List<UserManagedServiceAccount>>) invocation
                .getArguments()[2];
            listener.onResponse(accounts);
            return null;
        }).when(userManagedServiceAccountStore).listAccounts(any(), any(), any());
    }

    private void stubIndexTokenAuthentication(boolean success) {
        stubTokenAuthentication(indexServiceAccountTokenStore, success);
    }

    private void stubTokenAuthentication(CachingServiceAccountTokenStore store, boolean success) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener = (ActionListener<
                ServiceAccountTokenStore.StoreAuthenticationResult>) invocation.getArguments()[1];
            listener.onResponse(ServiceAccountTokenStore.StoreAuthenticationResult.fromBooleanResult(store.getTokenSource(), success));
            return null;
        }).when(store).authenticate(any(), any());
    }

    /**
     * The four token write paths answer their listener but are otherwise inert. No test asserts on what they return —
     * each verifies which of them was called — but leaving them unstubbed would turn a wrong dispatch into a hung
     * suite rather than a failed assertion.
     */
    private void stubTokenWrites() {
        final CreateServiceAccountTokenResponse created = CreateServiceAccountTokenResponse.created(
            ValidationTests.randomTokenName(),
            new SecureString(randomAlphaOfLength(20).toCharArray())
        );
        final Answer<Void> respondCreated = invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<CreateServiceAccountTokenResponse> listener = (ActionListener<
                CreateServiceAccountTokenResponse>) invocation.getArguments()[2];
            listener.onResponse(created);
            return null;
        };
        doAnswer(respondCreated).when(indexServiceAccountTokenStore).createBuiltInToken(any(), any(), any());
        doAnswer(respondCreated).when(indexServiceAccountTokenStore).createUserManagedToken(any(), any(), any());

        final Answer<Void> respondNotFound = invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(false);
            return null;
        };
        doAnswer(respondNotFound).when(indexServiceAccountTokenStore).deleteBuiltInToken(any(), any());
        doAnswer(respondNotFound).when(indexServiceAccountTokenStore).deleteUserManagedToken(any(), any());
    }

    private void stubHasTokensFor(boolean hasTokens) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(hasTokens);
            return null;
        }).when(indexServiceAccountTokenStore).hasTokensFor(any(), any());
    }

    private void stubDeleteAccount(boolean found) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(found);
            return null;
        }).when(userManagedServiceAccountStore).deleteAccount(any(), any(), any());
    }

    private void assertUnauthorized(PlainActionFuture<Authentication> future) {
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        assertThat(e.getMessage(), containsString("failed to authenticate service account ["));
    }

    private SecureString createBearerString(List<byte[]> bytesList) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (byte[] bytes : bytesList) {
                out.write(bytes);
            }
            return new SecureString(Base64.getEncoder().withoutPadding().encodeToString(out.toByteArray()).toCharArray());
        }
    }
}
