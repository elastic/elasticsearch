/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenServiceMock;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.test.SecurityMocks;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TransportGrantApiKeyActionTests extends ESTestCase {

    private TransportGrantApiKeyAction action;
    private ApiKeyGenerator apiKeyGenerator;
    private AuthenticationService authenticationService;
    private TokenServiceMock tokenServiceMock;
    private ThreadPool threadPool;

    @Before
    public void setupMocks() throws Exception {
        apiKeyGenerator = mock(ApiKeyGenerator.class);
        authenticationService = mock(AuthenticationService.class);

        threadPool = new TestThreadPool("TP-" + getTestName());
        tokenServiceMock = SecurityMocks.tokenService(true, threadPool);
        final ThreadContext threadContext = threadPool.getThreadContext();

        action = new TransportGrantApiKeyAction(mock(TransportService.class), mock(ActionFilters.class), threadContext,
            apiKeyGenerator, authenticationService, tokenServiceMock.tokenService);
    }

    @After
    public void cleanup() {
        threadPool.shutdown();
    }

    public void testGrantApiKeyWithUsernamePassword() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(username);
        request.getGrant().setPassword(password);

        final CreateApiKeyResponse response = mockResponse(request);

        doAnswer(inv -> {
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

        setupApiKeyGenerator(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
    }

    public void testGrantApiKeyWithInvalidUsernamePassword() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(username);
        request.getGrant().setPassword(password);

        final CreateApiKeyResponse response = mockResponse(request);

        doAnswer(inv -> {
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
            listener.onFailure(new ElasticsearchSecurityException("authentication failed for testing"));

            return null;
        }).when(authenticationService)
            .authenticate(eq(GrantApiKeyAction.NAME), same(request), any(UsernamePasswordToken.class), anyActionListener());

        setupApiKeyGenerator(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        final ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(exception, throwableWithMessage("authentication failed for testing"));

        verifyZeroInteractions(apiKeyGenerator);
    }

    public void testGrantApiKeyWithAccessToken() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final TokenServiceMock.MockToken token = tokenServiceMock.mockAccessToken();
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("access_token");
        request.getGrant().setAccessToken(token.encodedToken);

        final CreateApiKeyResponse response = mockResponse(request);

        tokenServiceMock.defineToken(token, authentication);
        setupApiKeyGenerator(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verifyZeroInteractions(authenticationService);
    }

    public void testGrantApiKeyWithInvalidatedAccessToken() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final TokenServiceMock.MockToken token = tokenServiceMock.mockAccessToken();
        final Authentication authentication = buildAuthentication(username);

        final GrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("access_token");
        request.getGrant().setAccessToken(token.encodedToken);

        final CreateApiKeyResponse response = mockResponse(request);

        tokenServiceMock.defineToken(token, authentication, false);
        setupApiKeyGenerator(authentication, request, response);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(null, request, future);

        final ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertThat(exception, throwableWithMessage("token expired"));

        verifyZeroInteractions(authenticationService);
        verifyZeroInteractions(apiKeyGenerator);
    }

    private Authentication buildAuthentication(String username) {
        return new Authentication(new User(username),
            new Authentication.RealmRef("realm_name", "realm_type", "node_name"), null);
    }

    private CreateApiKeyResponse mockResponse(GrantApiKeyRequest request) {
        return new CreateApiKeyResponse(request.getApiKeyRequest().getName(),
            randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(18).toCharArray()), null);
    }

    private GrantApiKeyRequest mockRequest() {
        final String keyName = randomAlphaOfLengthBetween(6, 32);
        final GrantApiKeyRequest request = new GrantApiKeyRequest();
        request.setApiKeyRequest(new CreateApiKeyRequest(keyName, Collections.emptyList(), null));
        return request;
    }

    private void setupApiKeyGenerator(Authentication authentication, GrantApiKeyRequest request, CreateApiKeyResponse response) {
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(3));

            assertThat(args[0], equalTo(authentication));
            assertThat(args[1], sameInstance(request.getApiKeyRequest()));

            @SuppressWarnings("unchecked")
            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[args.length - 1];
            listener.onResponse(response);

            return null;
        }).when(apiKeyGenerator).generateApiKey(any(Authentication.class), any(CreateApiKeyRequest.class), anyActionListener());
    }

}
