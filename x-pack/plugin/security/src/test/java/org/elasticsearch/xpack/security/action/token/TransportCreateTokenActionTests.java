/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosAuthenticationToken;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCreateTokenActionTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder()
        .put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();

    private ThreadPool threadPool;
    private TransportService transportService;
    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private AtomicReference<IndexRequest> idxReqReference;
    private AuthenticationService authenticationService;
    private MockLicenseState license;
    private SecurityContext securityContext;
    private MockBytesRefRecycler bytesRefRecycler;

    @Before
    public void setupClient() {
        threadPool = new TestThreadPool(getTestName());
        transportService = mock(TransportService.class);
        client = mock(Client.class);
        idxReqReference = new AtomicReference<>();
        authenticationService = mock(AuthenticationService.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(SETTINGS);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client);
            builder.setIndex((String) invocationOnMock.getArguments()[0]).setId((String) invocationOnMock.getArguments()[1]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString());
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
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
        }).when(client).multiGet(any(MultiGetRequest.class), anyActionListener());
        when(client.prepareIndex(nullable(String.class))).thenReturn(new IndexRequestBuilder(client));
        when(client.prepareUpdate(any(String.class), any(String.class))).thenReturn(new UpdateRequestBuilder(client));
        doAnswer(invocationOnMock -> {
            idxReqReference.set((IndexRequest) invocationOnMock.getArguments()[1]);
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

        securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());

        // setup lifecycle service
        securityIndex = mock(SecurityIndexManager.class);
        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(projectIndex).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));

        doAnswer(invocationOnMock -> {
            AuthenticationToken authToken = (AuthenticationToken) invocationOnMock.getArguments()[2];
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> authListener = (ActionListener<Authentication>) invocationOnMock.getArguments()[3];
            User user = null;
            if (authToken instanceof ServiceAccountToken serviceAccountToken) {
                // Mimics ServiceAccountService: tokens in the reserved namespace authenticate as built-in accounts,
                // any other namespace authenticates as a user-managed service account.
                final Authentication serviceAccountAuthentication;
                if ("elastic".equals(serviceAccountToken.getAccountId().namespace())) {
                    serviceAccountAuthentication = AuthenticationTestHelper.builder()
                        .serviceAccount(new User(serviceAccountToken.getAccountId().asPrincipal()))
                        .build(false);
                } else {
                    serviceAccountAuthentication = AuthenticationTestHelper.builder()
                        .userManagedServiceAccount(serviceAccountToken.getAccountId().asPrincipal(), "role1")
                        .build(false);
                }
                serviceAccountAuthentication.writeToContext(threadPool.getThreadContext());
                authListener.onResponse(serviceAccountAuthentication);
                return Void.TYPE;
            }
            if (authToken instanceof UsernamePasswordToken) {
                UsernamePasswordToken token = (UsernamePasswordToken) invocationOnMock.getArguments()[2];
                user = new User(token.principal());
            } else if (authToken instanceof KerberosAuthenticationToken) {
                KerberosAuthenticationToken token = (KerberosAuthenticationToken) invocationOnMock.getArguments()[2];
                if (token.credentials() instanceof byte[]
                    && new String((byte[]) token.credentials(), StandardCharsets.UTF_8).equals("fail")) {
                    String errorMessage = "failed to authenticate user, gss context negotiation not complete";
                    ElasticsearchSecurityException ese = new ElasticsearchSecurityException(errorMessage, RestStatus.UNAUTHORIZED);
                    ese.addBodyHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE, "Negotiate FAIL");
                    authListener.onFailure(ese);
                    return Void.TYPE;
                }
                user = new User(token.principal());
                threadPool.getThreadContext().addResponseHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE, "Negotiate SUCCESS");
            }
            Authentication authentication = AuthenticationTestHelper.builder()
                .user(user)
                .realmRef(new Authentication.RealmRef("fake", "mock", "n1"))
                .build(false);
            authentication.writeToContext(threadPool.getThreadContext());
            authListener.onResponse(authentication);
            return Void.TYPE;
        }).when(authenticationService)
            .authenticate(eq(CreateTokenAction.NAME), any(CreateTokenRequest.class), any(AuthenticationToken.class), anyActionListener());

        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);

        this.license = mock(MockLicenseState.class);
        when(license.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);

        this.bytesRefRecycler = new MockBytesRefRecycler();
    }

    @After
    public void stopThreadPool() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    @After
    public void cleanupMocks() {
        Releasables.closeExpectNoException(bytesRefRecycler);
    }

    public void testClientCredentialsCreatesWithoutRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("client_credentials");

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
        assertNull(createTokenResponse.getRefreshToken());
        assertNotNull(createTokenResponse.getTokenString());

        assertNotNull(idxReqReference.get());
        Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
        assertNotNull(sourceMap);
        assertNotNull(sourceMap.get("access_token"));
        assertNull(sourceMap.get("refresh_token"));
    }

    public void testPasswordGrantTypeCreatesWithRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("password");
        createTokenRequest.setUsername("user");
        createTokenRequest.setPassword(new SecureString("password".toCharArray()));

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
        assertNotNull(createTokenResponse.getRefreshToken());
        assertNotNull(createTokenResponse.getTokenString());

        assertNotNull(idxReqReference.get());
        Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
        assertNotNull(sourceMap);
        assertNotNull(sourceMap.get("access_token"));
        assertNotNull(sourceMap.get("refresh_token"));
    }

    public void testKerberosGrantTypeCreatesWithRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("_kerberos");
        String failOrSuccess = randomBoolean() ? "fail" : "success";
        String kerbCredentialsBase64 = Base64.getEncoder().encodeToString(failOrSuccess.getBytes(StandardCharsets.UTF_8));
        createTokenRequest.setKerberosTicket(new SecureString(kerbCredentialsBase64.toCharArray()));

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        if (failOrSuccess.equals("fail")) {
            ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, () -> tokenResponseFuture.actionGet());
            assertNotNull(ese.getBodyHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE));
            assertThat(ese.getBodyHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE).size(), is(1));
            assertThat(ese.getBodyHeader(KerberosAuthenticationToken.WWW_AUTHENTICATE).get(0), is("Negotiate FAIL"));
        } else {
            CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
            assertNotNull(createTokenResponse.getRefreshToken());
            assertNotNull(createTokenResponse.getTokenString());
            assertNotNull(createTokenResponse.getKerberosAuthenticationResponseToken());
            assertThat(createTokenResponse.getKerberosAuthenticationResponseToken(), is("SUCCESS"));

            assertNotNull(idxReqReference.get());
            Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
            assertNotNull(sourceMap);
            assertNotNull(sourceMap.get("access_token"));
            assertNotNull(sourceMap.get("refresh_token"));
        }
    }

    public void testKerberosGrantTypeWillFailOnBase64DecodeError() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("_kerberos");
        final char[] invalidBase64Chars = "!\"#$%&\\'()*,.:;<>?@[]^_`{|}~\t\n\r".toCharArray();
        final String kerberosTicketValue = Strings.arrayToDelimitedString(
            randomArray(1, 10, Character[]::new, () -> invalidBase64Chars[randomIntBetween(0, invalidBase64Chars.length - 1)]),
            ""
        );
        createTokenRequest.setKerberosTicket(new SecureString(kerberosTicketValue.toCharArray()));

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, assertListenerIsOnlyCalledOnce(tokenResponseFuture));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> tokenResponseFuture.actionGet());
        assertThat(e.getMessage(), containsString("could not decode base64 kerberos ticket"));
        // The code flow should stop after above failure and never reach authenticationService
        Mockito.verifyNoMoreInteractions(authenticationService);
    }

    public void testServiceAccountCannotCreateOAuthToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        // Neither built-in nor user-managed service accounts may self-mint through client_credentials; a user-managed
        // account is only exchangeable through the dedicated grant by an independently authenticated manage_token caller.
        Authentication authentication = randomBoolean()
            ? AuthenticationTestHelper.builder().serviceAccount().build(false)
            : AuthenticationTestHelper.builder().userManagedServiceAccount("apps/worker1", "role1").build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("client_credentials");

        PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, future);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("OAuth2 token creation is not supported for service accounts"));
        assertThat(e.status(), is(RestStatus.BAD_REQUEST));
    }

    public void testUserManagedServiceAccountGrantCreatesTokenWithoutRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        // the caller (e.g. Kibana) is authenticated independently of the exchanged credential
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("kibana_system"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("_user_managed_service_account");
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(new ServiceAccountId("apps", "worker1"), "token1");
        createTokenRequest.setServiceAccountToken(serviceAccountToken.asBearerString());

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
        assertNull(createTokenResponse.getRefreshToken());
        assertNotNull(createTokenResponse.getTokenString());
        assertTrue(createTokenResponse.getAuthentication().isUserManagedServiceAccount());
        assertThat(createTokenResponse.getAuthentication().getEffectiveSubject().getUser().principal(), is("apps/worker1"));
        assertServiceAccountCredentialsCleared(createTokenRequest.getServiceAccountToken());

        assertNotNull(idxReqReference.get());
        Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
        assertNotNull(sourceMap);
        assertNotNull(sourceMap.get("access_token"));
        assertNull(sourceMap.get("refresh_token"));
    }

    public void testUserManagedServiceAccountGrantRejectsBuiltInServiceAccountToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("kibana_system"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("_user_managed_service_account");
        final ServiceAccountToken serviceAccountToken = ServiceAccountToken.newToken(new ServiceAccountId("elastic", "kibana"), "token1");
        createTokenRequest.setServiceAccountToken(serviceAccountToken.asBearerString());

        PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, future);
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("invalid_grant"));
        assertThat(e.getBodyHeader("error_description").size(), is(1));
        assertThat(
            e.getBodyHeader("error_description").get(0),
            containsString("service_account_token must belong to a user-managed service account")
        );
        // No token document may be created for a built-in service account
        assertNull(idxReqReference.get());
        assertServiceAccountCredentialsCleared(createTokenRequest.getServiceAccountToken());
    }

    public void testUserManagedServiceAccountGrantClearsCredentialsOnAuthenticationFailure() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        AuthenticationTestHelper.builder().build().writeToContext(threadPool.getThreadContext());
        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest request = new CreateTokenRequest();
        request.setGrantType("_user_managed_service_account");
        try (ServiceAccountToken token = ServiceAccountToken.newToken(new ServiceAccountId("apps", "worker1"), "token1")) {
            request.setServiceAccountToken(token.asBearerString());
        }

        final ElasticsearchSecurityException failure = new ElasticsearchSecurityException("invalid credential", RestStatus.UNAUTHORIZED);
        // The authentication service is mocked so the failure path can be exercised without a backing service account store.
        doAnswer(invocation -> {
            ActionListener<Authentication> authenticationListener = invocation.getArgument(3);
            authenticationListener.onFailure(failure);
            return null;
        }).when(authenticationService)
            .authenticate(eq(CreateTokenAction.NAME), any(CreateTokenRequest.class), any(AuthenticationToken.class), anyActionListener());

        PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, assertListenerIsOnlyCalledOnce(future));
        assertSame(failure, expectThrows(ElasticsearchSecurityException.class, future::actionGet));
        assertServiceAccountCredentialsCleared(request.getServiceAccountToken());
        assertNull(idxReqReference.get());
    }

    public void testUserManagedServiceAccountGrantRejectsMalformedToken() throws Exception {
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService,
            bytesRefRecycler
        );
        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("kibana_system"))
            .realmRef(new Authentication.RealmRef("realm", "type", "node"))
            .build(false);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(
            threadPool,
            transportService,
            ActionFilters.EMPTY,
            tokenService,
            authenticationService,
            securityContext
        );
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("_user_managed_service_account");
        createTokenRequest.setServiceAccountToken(new SecureString("not-a-service-account-token".toCharArray()));

        PlainActionFuture<CreateTokenResponse> future = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, assertListenerIsOnlyCalledOnce(future));
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("invalid_grant"));
        assertThat(e.getBodyHeader("error_description").size(), is(1));
        assertThat(e.getBodyHeader("error_description").get(0), containsString("not a valid service account token"));
        expectThrows(IllegalStateException.class, () -> createTokenRequest.getServiceAccountToken().getChars());
        // The code flow should stop after the parse failure and never reach authenticationService
        Mockito.verifyNoMoreInteractions(authenticationService);
    }

    /**
     * Asserts that both copies of a service account credential are cleared once authentication completes: the
     * {@code service_account_token} request field and the secret of the parsed {@link ServiceAccountToken} passed to
     * the authentication service, which is a separate buffer.
     */
    private void assertServiceAccountCredentialsCleared(SecureString requestCredential) {
        final ArgumentCaptor<AuthenticationToken> tokenCaptor = ArgumentCaptor.forClass(AuthenticationToken.class);
        Mockito.verify(authenticationService)
            .authenticate(eq(CreateTokenAction.NAME), any(CreateTokenRequest.class), tokenCaptor.capture(), anyActionListener());
        final Object credential = tokenCaptor.getValue().credentials();
        assertTrue(credential instanceof SecureString);
        expectThrows(IllegalStateException.class, () -> ((SecureString) credential).getChars());
        expectThrows(IllegalStateException.class, requestCredential::getChars);
    }

    private static <T> ActionListener<T> assertListenerIsOnlyCalledOnce(ActionListener<T> delegate) {
        final AtomicInteger callCount = new AtomicInteger(0);
        return ActionListener.runBefore(delegate, () -> {
            if (callCount.incrementAndGet() != 1) {
                fail("Listener was called twice");
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
