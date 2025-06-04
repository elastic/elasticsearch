/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportGrantApiKeyActionTests extends ESTestCase {

    private TransportGrantApiKeyAction action;
    private ApiKeyService apiKeyService;
    private ApiKeyUserRoleDescriptorResolver resolver;
    private AuthenticationService authenticationService;
    private ThreadPool threadPool;
    private AuthorizationService authorizationService;

    @Before
    public void setupMocks() throws Exception {
        apiKeyService = mock(ApiKeyService.class);
        resolver = mock(ApiKeyUserRoleDescriptorResolver.class);
        authenticationService = mock(AuthenticationService.class);
        authorizationService = mock(AuthorizationService.class);

        threadPool = new TestThreadPool("TP-" + getTestName());
        final ThreadContext threadContext = threadPool.getThreadContext();
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        action = new TransportGrantApiKeyAction(
            transportService,
            new ActionFilters(Set.of()),
            threadContext,
            authenticationService,
            authorizationService,
            apiKeyService,
            resolver
        );
    }

    @After
    public void cleanup() {
        threadPool.shutdown();
    }

    public void testGrantApiKeyWithUsernamePassword() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(username);
        request.getGrant().setPassword(password);

        final CreateApiKeyResponse response = mockResponse(request);

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), nullValue());
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], equalTo(GrantApiKeyAction.NAME));
            assertThat(args[1], sameInstance(request));
            assertThat(args[2], instanceOf(UsernamePasswordToken.class));
            UsernamePasswordToken token = (UsernamePasswordToken) args[2];
            assertThat(token.principal(), equalTo(username));
            assertThat(token.credentials(), equalTo(password));

            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) args[args.length - 1];
            listener.onResponse(authentication);

            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(UsernamePasswordToken.class), anyActionListener());

        setupApiKeyServiceWithRoleResolution(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verify(authorizationService, never()).authorize(any(), any(), any(), anyActionListener());
    }

    public void testClientAuthenticationForNonJWTFails() {
        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("access_token");
        request.getGrant().setAccessToken(new SecureString("obviously a non JWT token".toCharArray()));
        // only JWT tokens support client authentication
        request.getGrant().setClientAuthentication(new Grant.ClientAuthentication(new SecureString("whatever".toCharArray())));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        final ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(exception, throwableWithMessage("[client_authentication] not supported with the supplied access_token type"));

        verifyNoMoreInteractions(authenticationService);
        verifyNoMoreInteractions(authorizationService);
        verifyNoMoreInteractions(apiKeyService);
        verifyNoMoreInteractions(resolver);
    }

    public void testClientAuthenticationWithUsernamePasswordFails() {
        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(randomAlphaOfLengthBetween(4, 12));
        request.getGrant().setPassword(new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray()));
        // username & password does not support client authentication
        request.getGrant().setClientAuthentication(new Grant.ClientAuthentication(new SecureString("whatever".toCharArray())));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        final ActionRequestValidationException exception = expectThrows(ActionRequestValidationException.class, future::actionGet);
        assertThat(exception.getMessage(), containsString("[client_authentication] is not supported for grant_type [password]"));

        verifyNoMoreInteractions(authenticationService);
        verifyNoMoreInteractions(authorizationService);
        verifyNoMoreInteractions(apiKeyService);
        verifyNoMoreInteractions(resolver);
    }

    public void testUnsupportedClientAuthenticationScheme() {
        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("access_token");
        request.getGrant().setAccessToken(new SecureString("some token".toCharArray()));
        request.getGrant()
            .setClientAuthentication(new Grant.ClientAuthentication("wrong scheme", new SecureString("whatever".toCharArray())));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        final ActionRequestValidationException exception = expectThrows(ActionRequestValidationException.class, future::actionGet);
        assertThat(exception.getMessage(), containsString("[client_authentication.scheme] must be set to [SharedSecret]"));

        verifyNoMoreInteractions(authenticationService);
        verifyNoMoreInteractions(authorizationService);
        verifyNoMoreInteractions(apiKeyService);
        verifyNoMoreInteractions(resolver);
    }

    public void testGrantApiKeyWithAccessToken() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("access_token");
        final SecureString bearerString = new SecureString(randomAlphaOfLength(20).toCharArray());
        request.getGrant().setAccessToken(bearerString);

        final CreateApiKeyResponse response = mockResponse(request);

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), nullValue());
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], equalTo(GrantApiKeyAction.NAME));
            assertThat(args[1], sameInstance(request));
            assertThat(args[2], instanceOf(BearerToken.class));
            assertThat(((BearerToken) args[2]).credentials(), equalTo(bearerString));

            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) args[args.length - 1];
            listener.onResponse(authentication);

            return null;
        }).when(authenticationService).authenticate(eq(GrantApiKeyAction.NAME), same(request), any(BearerToken.class), anyActionListener());

        setupApiKeyServiceWithRoleResolution(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verify(authorizationService, never()).authorize(any(), any(), any(), anyActionListener());
    }

    public void testGrantApiKeyWithInvalidatedCredentials() {
        final GrantApiKeyRequest request = mockRequest();
        if (randomBoolean()) {
            request.getGrant().setType("password");
            final String username = randomAlphaOfLengthBetween(4, 12);
            final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
            request.getGrant().setUsername(username);
            request.getGrant().setPassword(password);
        } else {
            request.getGrant().setType("access_token");
            final SecureString bearerString = new SecureString(randomAlphaOfLength(20).toCharArray());
            request.getGrant().setAccessToken(bearerString);
        }

        final String username = randomAlphaOfLengthBetween(4, 12);
        final Authentication authentication = buildAuthentication(username);

        final CreateApiKeyResponse response = mockResponse(request);

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), nullValue());
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], equalTo(GrantApiKeyAction.NAME));
            assertThat(args[1], sameInstance(request));
            final GrantApiKeyRequest grantApiKeyRequest = (GrantApiKeyRequest) args[1];
            assertThat(request.getGrant().getType(), oneOf("password", "access_token"));
            if (grantApiKeyRequest.getGrant().getType().equals("password")) {
                assertThat(args[2], instanceOf(UsernamePasswordToken.class));
                UsernamePasswordToken token = (UsernamePasswordToken) args[2];
                assertThat(token.principal(), equalTo(request.getGrant().getUsername()));
                assertThat(token.credentials(), equalTo(request.getGrant().getPassword()));
            } else {
                assertThat(args[2], instanceOf(BearerToken.class));
                assertThat(((BearerToken) args[2]).credentials(), equalTo(grantApiKeyRequest.getGrant().getAccessToken()));
            }
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) args[args.length - 1];
            listener.onFailure(new ElasticsearchSecurityException("authentication failed for testing"));

            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(AuthenticationToken.class), anyActionListener());

        setupApiKeyServiceWithRoleResolution(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        final ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(exception, throwableWithMessage("authentication failed for testing"));

        verifyNoMoreInteractions(apiKeyService);
        verifyNoMoreInteractions(resolver);
        verify(authorizationService, never()).authorize(any(), any(), any(), anyActionListener());
    }

    public void testGrantWithRunAs() {
        final GrantApiKeyRequest request = mockRequest();
        if (randomBoolean()) {
            request.getGrant().setType("password");
            final String username = randomAlphaOfLengthBetween(4, 12);
            final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
            request.getGrant().setUsername(username);
            request.getGrant().setPassword(password);
        } else {
            request.getGrant().setType("access_token");
            final SecureString bearerString = new SecureString(randomAlphaOfLength(20).toCharArray());
            request.getGrant().setAccessToken(bearerString);
        }

        final String username = randomAlphaOfLengthBetween(4, 12);
        final String runAsUsername = randomValueOtherThan(username, () -> randomAlphaOfLengthBetween(4, 12));
        request.getGrant().setRunAsUsername(runAsUsername);

        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(username))
            .runAs()
            .user(new User(runAsUsername))
            .build();

        final CreateApiKeyResponse response = mockResponse(request);
        setupApiKeyServiceWithRoleResolution(authentication, request, response);

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), equalTo(runAsUsername));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) inv.getArguments()[3];
            listener.onResponse(authentication);
            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(AuthenticationToken.class), anyActionListener());

        doAnswer(invocation -> {
            final Object[] args = invocation.getArguments();
            assertThat(args[0], is(authentication));
            assertThat(args[1], is(AuthenticateAction.NAME));
            @SuppressWarnings("unchecked")
            final ActionListener<Void> listener = (ActionListener<Void>) args[3];
            listener.onResponse(null);
            return null;
        }).when(authorizationService)
            .authorize(eq(authentication), eq(AuthenticateAction.NAME), any(AuthenticateRequest.class), anyActionListener());

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verify(authorizationService).authorize(
            eq(authentication),
            eq(AuthenticateAction.NAME),
            any(AuthenticateRequest.class),
            anyActionListener()
        );

        // ThreadContext is restored afterwards
        assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), nullValue());
    }

    public void testGrantWithRunAsFailureDueToAuthorization() {
        final GrantApiKeyRequest request = mockRequest();
        if (randomBoolean()) {
            request.getGrant().setType("password");
            final String username = randomAlphaOfLengthBetween(4, 12);
            final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
            request.getGrant().setUsername(username);
            request.getGrant().setPassword(password);
        } else {
            request.getGrant().setType("access_token");
            final SecureString bearerString = new SecureString(randomAlphaOfLength(20).toCharArray());
            request.getGrant().setAccessToken(bearerString);
        }

        final String username = randomAlphaOfLengthBetween(4, 12);
        final String runAsUsername = randomValueOtherThan(username, () -> randomAlphaOfLengthBetween(4, 12));
        request.getGrant().setRunAsUsername(runAsUsername);

        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(username))
            .runAs()
            .user(new User(runAsUsername))
            .build();

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), equalTo(runAsUsername));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) inv.getArguments()[3];
            listener.onResponse(authentication);
            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(AuthenticationToken.class), anyActionListener());

        final ElasticsearchSecurityException e = new ElasticsearchSecurityException("unauthorized run-as");
        doAnswer(invocation -> {
            final Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            final ActionListener<Void> listener = (ActionListener<Void>) args[3];
            listener.onFailure(e);
            return null;
        }).when(authorizationService)
            .authorize(eq(authentication), eq(AuthenticateAction.NAME), any(AuthenticateRequest.class), anyActionListener());

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(expectThrows(ElasticsearchSecurityException.class, future::actionGet), sameInstance(e));
        verify(authorizationService).authorize(
            eq(authentication),
            eq(AuthenticateAction.NAME),
            any(AuthenticateRequest.class),
            anyActionListener()
        );
        // ThreadContext is restored afterwards
        assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), nullValue());
    }

    public void testGrantFailureDueToUnsupportedRunAs() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final Authentication authentication = AuthenticationTestHelper.builder().user(new User(username)).build();
        final String runAsUsername = randomValueOtherThan(username, () -> randomAlphaOfLengthBetween(4, 12));
        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(username);
        request.getGrant().setPassword(new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray()));
        request.getGrant().setRunAsUsername(runAsUsername);

        doAnswer(inv -> {
            assertThat(threadPool.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER), equalTo(runAsUsername));
            @SuppressWarnings("unchecked")
            ActionListener<Authentication> listener = (ActionListener<Authentication>) inv.getArguments()[3];
            listener.onResponse(authentication);
            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(AuthenticationToken.class), anyActionListener());

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        final ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("the provided grant credentials do not support run-as"));
        assertThat(e.status(), is(RestStatus.BAD_REQUEST));
    }

    private Authentication buildAuthentication(String username) {
        return AuthenticationTestHelper.builder()
            .user(new User(username))
            .realmRef(new Authentication.RealmRef("realm_name", "realm_type", "node_name"))
            .build(false);
    }

    private CreateApiKeyResponse mockResponse(GrantApiKeyRequest request) {
        return new CreateApiKeyResponse(
            request.getApiKeyRequest().getName(),
            randomAlphaOfLength(12),
            new SecureString(randomAlphaOfLength(18).toCharArray()),
            null
        );
    }

    private GrantApiKeyRequest mockRequest() {
        final String keyName = randomAlphaOfLengthBetween(6, 32);
        final GrantApiKeyRequest request = new GrantApiKeyRequest();
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(keyName, List.of(), null);
        createApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        request.setApiKeyRequest(createApiKeyRequest);
        return request;
    }

    private void setupApiKeyServiceWithRoleResolution(
        Authentication authentication,
        GrantApiKeyRequest request,
        CreateApiKeyResponse response
    ) {
        final Set<RoleDescriptor> roleDescriptors = Set.of();
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], equalTo(authentication));
            assertThat(args[1], sameInstance(request.getApiKeyRequest()));
            assertThat(args[2], sameInstance(roleDescriptors));

            @SuppressWarnings("unchecked")
            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[args.length - 1];
            listener.onResponse(response);

            return null;
        }).when(apiKeyService).createApiKey(any(Authentication.class), any(CreateApiKeyRequest.class), any(), anyActionListener());

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));
            assertThat(args[0], equalTo(authentication));

            @SuppressWarnings("unchecked")
            ActionListener<Set<RoleDescriptor>> listener = (ActionListener<Set<RoleDescriptor>>) args[args.length - 1];
            listener.onResponse(roleDescriptors);

            return null;
        }).when(resolver).resolveUserRoleDescriptors(any(Authentication.class), anyActionListener());
    }
}
