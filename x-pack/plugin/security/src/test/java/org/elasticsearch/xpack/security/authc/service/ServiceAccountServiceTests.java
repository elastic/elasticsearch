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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesResponse;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountServiceTests extends ESTestCase {

    private Client client;
    private ThreadPool threadPool;
    private FileServiceAccountTokenStore fileServiceAccountTokenStore;
    private IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private ServiceAccountService serviceAccountService;
    private Transport transport;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws UnknownHostException {
        threadPool = new TestThreadPool("service account service tests");
        fileServiceAccountTokenStore = mock(FileServiceAccountTokenStore.class);
        indexServiceAccountTokenStore = mock(IndexServiceAccountTokenStore.class);
        when(fileServiceAccountTokenStore.getTokenSource()).thenReturn(TokenInfo.TokenSource.FILE);
        when(indexServiceAccountTokenStore.getTokenSource()).thenReturn(TokenInfo.TokenSource.INDEX);
        final Settings.Builder builder = Settings.builder()
            .put("xpack.security.enabled", true);
        transport = mock(Transport.class);
        final TransportAddress transportAddress;
        if (randomBoolean()) {
            transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        } else {
            transportAddress = new TransportAddress(InetAddress.getLocalHost(), 9300);
        }
        if (randomBoolean()) {
            builder.put("xpack.security.http.ssl.enabled", true);
        } else {
            builder.put("discovery.type", "single-node");
        }
        when(transport.boundAddress()).thenReturn(
            new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        serviceAccountService = new ServiceAccountService(client,
            fileServiceAccountTokenStore, indexServiceAccountTokenStore,
            new HttpTlsRuntimeCheck(builder.build(), new SetOnce<>(transport)));
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testGetServiceAccountPrincipals() {
        assertThat(ServiceAccountService.getServiceAccountPrincipals(),
            containsInAnyOrder("elastic/fleet-server", "elastic/kibana-system"));
    }

    public void testTryParseToken() throws IOException, IllegalAccessException {
        // Null for null
        assertNull(ServiceAccountService.tryParseToken(null));

        final byte[] magicBytes = { 0, 1, 0, 1 };

        final Logger satLogger = LogManager.getLogger(ServiceAccountToken.class);
        Loggers.setLevel(satLogger, Level.TRACE);
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(satLogger, appender);
        Loggers.addAppender(sasLogger, appender);
        appender.start();

        try {
            // Less than 4 bytes
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "less than 4 bytes", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes")
            );
            final SecureString bearerString0 = createBearerString(List.of(Arrays.copyOfRange(magicBytes, 0, randomIntBetween(0, 3))));
            assertNull(ServiceAccountService.tryParseToken(bearerString0));
            appender.assertAllExpectationsMatched();

            // Prefix mismatch
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "prefix mismatch", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes"
            ));
            final SecureString bearerString1 = createBearerString(List.of(
                new byte[] { randomValueOtherThan((byte) 0, ESTestCase::randomByte) },
                randomByteArrayOfLength(randomIntBetween(30, 50))));
            assertNull(ServiceAccountService.tryParseToken(bearerString1));
            appender.assertAllExpectationsMatched();

            // No colon
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "no colon", ServiceAccountToken.class.getName(), Level.TRACE,
                "failed to extract qualified service token name and secret, missing ':'"
            ));
            final SecureString bearerString2 = createBearerString(List.of(
                magicBytes,
                randomAlphaOfLengthBetween(30, 50).getBytes(StandardCharsets.UTF_8)));
            assertNull(ServiceAccountService.tryParseToken(bearerString2));
            appender.assertAllExpectationsMatched();

            // Invalid delimiter for qualified name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid delimiter for qualified name", ServiceAccountToken.class.getName(), Level.TRACE,
                "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
            ));
            if (randomBoolean()) {
                final SecureString bearerString3 = createBearerString(List.of(
                    magicBytes,
                    (randomAlphaOfLengthBetween(10, 20) + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                ));
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            } else {
                final SecureString bearerString3 = createBearerString(List.of(
                    magicBytes,
                    (randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)
                        + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
                ));
                assertNull(ServiceAccountService.tryParseToken(bearerString3));
            }
            appender.assertAllExpectationsMatched();

            // Invalid token name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid token name", ServiceAccountService.class.getName(), Level.TRACE,
                "Cannot parse possible service account token"
            ));
            final SecureString bearerString4 = createBearerString(List.of(
                magicBytes,
                (randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8)
                    + "/" + randomValueOtherThanMany(n -> n.contains("/"), ValidationTests::randomInvalidTokenName)
                    + ":" + randomAlphaOfLengthBetween(10, 20)).getBytes(StandardCharsets.UTF_8)
            ));
            assertNull(ServiceAccountService.tryParseToken(bearerString4));
            appender.assertAllExpectationsMatched();

            // Everything is good
            final String namespace = randomAlphaOfLengthBetween(3, 8);
            final String serviceName = randomAlphaOfLengthBetween(3, 8);
            final String tokenName = ValidationTests.randomTokenName();
            final ServiceAccountId accountId = new ServiceAccountId(namespace, serviceName);
            final String secret = randomAlphaOfLengthBetween(10, 20);
            final SecureString bearerString5 = createBearerString(List.of(
                magicBytes,
                (namespace + "/" + serviceName + "/" + tokenName + ":" + secret).getBytes(StandardCharsets.UTF_8)
            ));
            final ServiceAccountToken serviceAccountToken1 = ServiceAccountService.tryParseToken(bearerString5);
            final ServiceAccountToken serviceAccountToken2 = new ServiceAccountToken(accountId, tokenName,
                new SecureString(secret.toCharArray()));
            assertThat(serviceAccountToken1, equalTo(serviceAccountToken2));

            // Serialise and de-serialise service account token
            final ServiceAccountToken parsedToken = ServiceAccountService.tryParseToken(serviceAccountToken2.asBearerString());
            assertThat(parsedToken, equalTo(serviceAccountToken2));

            // Invalid magic byte
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid magic byte again", ServiceAccountToken.class.getName(), Level.TRACE,
                "service account token expects the 4 leading bytes"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AQEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // No colon
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "no colon again", ServiceAccountToken.class.getName(), Level.TRACE,
                "failed to extract qualified service token name and secret, missing ':'"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xX3N1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // Invalid qualified name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid delimiter for qualified name again", ServiceAccountToken.class.getName(), Level.TRACE,
                "The qualified name of a service token should take format of 'namespace/service_name/token_name'"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXRfdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // Invalid token name
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid token name again", ServiceAccountService.class.getName(), Level.TRACE,
                "Cannot parse possible service account token"
            ));
            assertNull(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4hOnN1cGVyc2VjcmV0".toCharArray())));
            appender.assertAllExpectationsMatched();

            // everything is fine
            assertThat(ServiceAccountService.tryParseToken(
                new SecureString("AAEAAWVsYXN0aWMvZmxlZXQtc2VydmVyL3Rva2VuMTpzdXBlcnNlY3JldA".toCharArray())),
                equalTo(new ServiceAccountToken(new ServiceAccountId("elastic", "fleet-server"), "token1",
                    new SecureString("supersecret".toCharArray()))));
        } finally {
            appender.stop();
            Loggers.setLevel(satLogger, Level.INFO);
            Loggers.setLevel(sasLogger, Level.INFO);
            Loggers.removeAppender(satLogger, appender);
            Loggers.removeAppender(sasLogger, appender);
        }
    }

    public void testTryAuthenticateBearerToken() throws ExecutionException, InterruptedException {
        // Valid token
        final PlainActionFuture<Authentication> future5 = new PlainActionFuture<>();

        final CachingServiceAccountTokenStore authenticatingStore = randomFrom(fileServiceAccountTokenStore, indexServiceAccountTokenStore);
        Stream.of(fileServiceAccountTokenStore, indexServiceAccountTokenStore).forEach(store -> {
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener =
                    (ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(
                    new ServiceAccountTokenStore.StoreAuthenticationResult(store == authenticatingStore, store.getTokenSource()));
                return null;
            }).when(store).authenticate(any(), any());
        });

        final String nodeName = randomAlphaOfLengthBetween(3, 8);
        serviceAccountService.authenticateToken(
            new ServiceAccountToken(new ServiceAccountId("elastic", "fleet-server"), "token1",
                new SecureString("super-secret-value".toCharArray())),
            nodeName, future5);
        assertThat(future5.get(), equalTo(
            new Authentication(
                new User("elastic/fleet-server", Strings.EMPTY_ARRAY, "Service account - elastic/fleet-server", null,
                    Map.of("_elastic_service_account", true), true),
                new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, nodeName),
                null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
                Map.of("_token_name", "token1",
                    "_token_source", authenticatingStore.getTokenSource().name().toLowerCase(Locale.ROOT))
            )
        ));
    }

    public void testAuthenticateWithToken() throws ExecutionException, InterruptedException, IllegalAccessException {
        final Logger sasLogger = LogManager.getLogger(ServiceAccountService.class);
        Loggers.setLevel(sasLogger, Level.TRACE);

        final MockLogAppender appender = new MockLogAppender();
        Loggers.addAppender(sasLogger, appender);
        appender.start();

        try {
            // non-elastic service account
            final ServiceAccountId accountId1 = new ServiceAccountId(
                randomValueOtherThan(ElasticServiceAccounts.NAMESPACE, () -> randomAlphaOfLengthBetween(3, 8)),
                randomAlphaOfLengthBetween(3, 8));
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "non-elastic service account", ServiceAccountService.class.getName(), Level.DEBUG,
                "only [elastic] service accounts are supported, but received [" + accountId1.asPrincipal() + "]"
            ));
            final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
            final ServiceAccountToken token1 = new ServiceAccountToken(accountId1, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future1 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token1, randomAlphaOfLengthBetween(3, 8), future1);
            final ExecutionException e1 = expectThrows(ExecutionException.class, future1::get);
            assertThat(e1.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e1.getMessage(), containsString("failed to authenticate service account ["
                + token1.getAccountId().asPrincipal() + "] with token name [" + token1.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();

            // Unknown elastic service name
            final ServiceAccountId accountId2 = new ServiceAccountId(
                ElasticServiceAccounts.NAMESPACE,
                randomValueOtherThan("fleet-server", () -> randomAlphaOfLengthBetween(3, 8)));
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "unknown elastic service name", ServiceAccountService.class.getName(), Level.DEBUG,
                "the [" + accountId2.asPrincipal() + "] service account does not exist"
            ));
            final ServiceAccountToken token2 = new ServiceAccountToken(accountId2, randomAlphaOfLengthBetween(3, 8), secret);
            final PlainActionFuture<Authentication> future2 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token2, randomAlphaOfLengthBetween(3, 8), future2);
            final ExecutionException e2 = expectThrows(ExecutionException.class, future2::get);
            assertThat(e2.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e2.getMessage(), containsString("failed to authenticate service account ["
                + token2.getAccountId().asPrincipal() + "] with token name [" + token2.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();

            // Length of secret value is too short
            final ServiceAccountId accountId3 = new ServiceAccountId(ElasticServiceAccounts.NAMESPACE, "fleet-server");
            final SecureString secret3 = new SecureString(randomAlphaOfLengthBetween(1, 9).toCharArray());
            final ServiceAccountToken token3 = new ServiceAccountToken(accountId3, randomAlphaOfLengthBetween(3, 8), secret3);
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "secret value too short", ServiceAccountService.class.getName(), Level.DEBUG,
                "the provided credential has length [" + secret3.length()
                    + "] but a token's secret value must be at least [10] characters"
            ));
            final PlainActionFuture<Authentication> future3 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token3, randomAlphaOfLengthBetween(3, 8), future3);
            final ExecutionException e3 = expectThrows(ExecutionException.class, future3::get);
            assertThat(e3.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e3.getMessage(), containsString("failed to authenticate service account ["
                + token3.getAccountId().asPrincipal() + "] with token name [" + token3.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();

            final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
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
            final ServiceAccountToken token4 = new ServiceAccountToken(accountId4, randomAlphaOfLengthBetween(3, 8), secret);
            final ServiceAccountToken token5 = new ServiceAccountToken(accountId4, randomAlphaOfLengthBetween(3, 8),
                new SecureString(randomAlphaOfLength(20).toCharArray()));
            final String nodeName = randomAlphaOfLengthBetween(3, 8);
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener =
                    (ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(new ServiceAccountTokenStore.StoreAuthenticationResult(true, store.getTokenSource()));
                return null;
            }).when(store).authenticate(eq(token4), any());

            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener =
                    (ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(new ServiceAccountTokenStore.StoreAuthenticationResult(false, store.getTokenSource()));
                return null;
            }).when(store).authenticate(eq(token5), any());

            doAnswer(invocationOnMock -> {
                @SuppressWarnings("unchecked")
                final ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult> listener =
                    (ActionListener<ServiceAccountTokenStore.StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
                listener.onResponse(new ServiceAccountTokenStore.StoreAuthenticationResult(false, otherStore.getTokenSource()));
                return null;
            }).when(otherStore).authenticate(any(), any());

            final PlainActionFuture<Authentication> future4 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token4, nodeName, future4);
            final Authentication authentication = future4.get();
            assertThat(authentication, equalTo(new Authentication(
                new User("elastic/fleet-server", Strings.EMPTY_ARRAY,
                    "Service account - elastic/fleet-server", null,
                    Map.of("_elastic_service_account", true),
                    true),
                new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, nodeName),
                null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
                Map.of("_token_name", token4.getTokenName(), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT))
            )));

            appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                "invalid credential", ServiceAccountService.class.getName(), Level.DEBUG,
                "failed to authenticate service account [" + token5.getAccountId().asPrincipal()
                    + "] with token name [" + token5.getTokenName() + "]"
            ));
            final PlainActionFuture<Authentication> future5 = new PlainActionFuture<>();
            serviceAccountService.authenticateToken(token5, nodeName, future5);
            final ExecutionException e5 = expectThrows(ExecutionException.class, future5::get);
            assertThat(e5.getCause().getClass(), is(ElasticsearchSecurityException.class));
            assertThat(e5.getMessage(), containsString("failed to authenticate service account ["
                + token5.getAccountId().asPrincipal() + "] with token name [" + token5.getTokenName() + "]"));
            appender.assertAllExpectationsMatched();
        } finally {
            appender.stop();
            Loggers.setLevel(sasLogger, Level.INFO);
            Loggers.removeAppender(sasLogger, appender);
        }
    }

    public void testGetRoleDescriptor() throws ExecutionException, InterruptedException {
        final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
        final Authentication auth1 = new Authentication(
            new User("elastic/fleet-server",
                Strings.EMPTY_ARRAY,
                "Service account - elastic/fleet-server",
                null,
                Map.of("_elastic_service_account", true),
                true),
            new Authentication.RealmRef(
                ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT)));

        final PlainActionFuture<RoleDescriptor> future1 = new PlainActionFuture<>();
        serviceAccountService.getRoleDescriptor(auth1, future1);
        final RoleDescriptor roleDescriptor1 = future1.get();
        assertNotNull(roleDescriptor1);
        assertThat(roleDescriptor1.getName(), equalTo("elastic/fleet-server"));

        final String username =
            randomValueOtherThan("elastic/fleet-server", () -> randomAlphaOfLengthBetween(3, 8) + "/" + randomAlphaOfLengthBetween(3, 8));
        final Authentication auth2 = new Authentication(
            new User(username, Strings.EMPTY_ARRAY, "Service account - " + username, null,
                Map.of("_elastic_service_account", true), true),
            new Authentication.RealmRef(
                ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT)));
        final PlainActionFuture<RoleDescriptor> future2 = new PlainActionFuture<>();
        serviceAccountService.getRoleDescriptor(auth2, future2);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future2::actionGet);
        assertThat(e.getMessage(), containsString(
            "cannot load role for service account [" + username + "] - no such service account"));
    }

    public void testCreateIndexTokenWillDelegate() {
        final Authentication authentication = mock(Authentication.class);
        final CreateServiceAccountTokenRequest request = mock(CreateServiceAccountTokenRequest.class);
        final ActionListener<CreateServiceAccountTokenResponse> future = new PlainActionFuture<>();
        serviceAccountService.createIndexToken(authentication, request, future);
        verify(indexServiceAccountTokenStore).createToken(eq(authentication), eq(request), eq(future));
    }

    public void testDeleteIndexTokenWillDelegate() {
        final DeleteServiceAccountTokenRequest request = mock(DeleteServiceAccountTokenRequest.class);
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        serviceAccountService.deleteIndexToken(request, future);
        verify(indexServiceAccountTokenStore).deleteToken(eq(request), eq(future));
    }

    public void testFindTokensFor() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String serviceName = randomAlphaOfLengthBetween(3, 8);
        final ServiceAccountId accountId = new ServiceAccountId(namespace, serviceName);

        final List<TokenInfo> indexTokenInfos = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> TokenInfo.indexToken(ValidationTests.randomTokenName()))
            .sorted()
            .collect(Collectors.toUnmodifiableList());

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
            final ActionListener<GetServiceAccountCredentialsNodesResponse> listener =
                (ActionListener<GetServiceAccountCredentialsNodesResponse>) args[2];
            listener.onResponse(fileTokensResponse);
            return null;
        }).when(client).execute(eq(GetServiceAccountNodesCredentialsAction.INSTANCE),
            any(GetServiceAccountCredentialsNodesRequest.class), any());

        final PlainActionFuture<GetServiceAccountCredentialsResponse> future = new PlainActionFuture<>();
        serviceAccountService.findTokensFor(new GetServiceAccountCredentialsRequest(namespace, serviceName), future);
        final GetServiceAccountCredentialsResponse response = future.actionGet();
        assertThat(response.getPrincipal(), equalTo(accountId.asPrincipal()));
        assertThat(response.getNodesResponse(), is(fileTokensResponse));
        assertThat(response.getIndexTokenInfos(), equalTo(indexTokenInfos));
    }

    public void testTlsRequired() {
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", false)
            .build();
        final TransportAddress transportAddress = new TransportAddress(TransportAddress.META_ADDRESS, 9300);
        when(transport.boundAddress()).thenReturn(
            new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress));

        final ServiceAccountService service = new ServiceAccountService(client,
            fileServiceAccountTokenStore,indexServiceAccountTokenStore,
            new HttpTlsRuntimeCheck(settings, new SetOnce<>(transport)));

        final PlainActionFuture<Authentication> future1 = new PlainActionFuture<>();
        service.authenticateToken(mock(ServiceAccountToken.class), randomAlphaOfLengthBetween(3, 8), future1);
        final ElasticsearchException e1 = expectThrows(ElasticsearchException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("[service account authentication] requires TLS for the HTTP interface"));

        final PlainActionFuture<RoleDescriptor> future2 = new PlainActionFuture<>();
        final TokenInfo.TokenSource tokenSource = randomFrom(TokenInfo.TokenSource.values());
        final Authentication authentication = new Authentication(mock(User.class),
            new Authentication.RealmRef(
                ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE,
                randomAlphaOfLengthBetween(3, 8)),
            null,
            Version.CURRENT,
            Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", randomAlphaOfLengthBetween(3, 8), "_token_source", tokenSource.name().toLowerCase(Locale.ROOT)));
        service.getRoleDescriptor(authentication, future2);
        final ElasticsearchException e2 = expectThrows(ElasticsearchException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("[service account role descriptor resolving] requires TLS for the HTTP interface"));

        final PlainActionFuture<CreateServiceAccountTokenResponse> future3 = new PlainActionFuture<>();
        service.createIndexToken(authentication, mock(CreateServiceAccountTokenRequest.class), future3);
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);
        assertThat(e3.getMessage(), containsString("[create index-backed service token] requires TLS for the HTTP interface"));

        final PlainActionFuture<Boolean> future4 = new PlainActionFuture<>();
        service.deleteIndexToken(mock(DeleteServiceAccountTokenRequest.class), future4);
        final ElasticsearchException e4 = expectThrows(ElasticsearchException.class, future4::actionGet);
        assertThat(e4.getMessage(), containsString("[delete index-backed service token] requires TLS for the HTTP interface"));

        final PlainActionFuture<GetServiceAccountCredentialsResponse> future5 = new PlainActionFuture<>();
        service.findTokensFor(mock(GetServiceAccountCredentialsRequest.class), future5);
        final ElasticsearchException e5 = expectThrows(ElasticsearchException.class, future5::actionGet);
        assertThat(e5.getMessage(), containsString("[find service tokens] requires TLS for the HTTP interface"));
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
