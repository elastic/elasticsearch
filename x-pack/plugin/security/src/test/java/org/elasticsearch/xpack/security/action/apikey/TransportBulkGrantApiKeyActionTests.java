/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.PluggableAuthenticatorChain;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportBulkGrantApiKeyActionTests extends ESTestCase {

    private TransportBulkGrantApiKeyAction action;
    private ApiKeyService apiKeyService;
    private ApiKeyUserRoleDescriptorResolver resolver;
    private AuthenticationService authenticationService;
    private ThreadPool threadPool;
    private AuthorizationService authorizationService;

    @Before
    public void setupMocks() {
        apiKeyService = mock(ApiKeyService.class);
        resolver = mock(ApiKeyUserRoleDescriptorResolver.class);
        authenticationService = mock(AuthenticationService.class);
        authorizationService = mock(AuthorizationService.class);

        threadPool = new TestThreadPool("TP-" + getTestName());
        final ThreadContext threadContext = threadPool.getThreadContext();
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        PluggableAuthenticatorChain pluggableAuthenticatorChain = mock(PluggableAuthenticatorChain.class);
        when(pluggableAuthenticatorChain.getCustomAuthenticators()).thenReturn(List.of());

        action = new TransportBulkGrantApiKeyAction(
            transportService,
            ActionFilters.EMPTY,
            threadContext,
            authenticationService,
            authorizationService,
            apiKeyService,
            resolver,
            pluggableAuthenticatorChain
        );
    }

    @After
    public void cleanup() {
        threadPool.shutdown();
    }

    public void testBulkGrantApiKeyWithUsernamePassword() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 24).toCharArray());
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User(username))
            .realmRef(new Authentication.RealmRef("realm_name", "realm_type", "node_name"))
            .build(false);

        final BulkGrantApiKeyRequest request = mockRequest();
        request.getGrant().setType("password");
        request.getGrant().setUsername(username);
        request.getGrant().setPassword(password);

        final BulkGrantApiKeyResponse response = new BulkGrantApiKeyResponse(
            List.of(
                new CreateApiKeyResponse(
                    request.getApiKeyRequests().get(0).getName(),
                    randomAlphaOfLength(12),
                    new SecureString(randomAlphaOfLength(18).toCharArray()),
                    null
                )
            ),
            Map.of()
        );

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));
            assertThat(args[0], equalTo(BulkGrantApiKeyAction.NAME));
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
            .authenticate(eq(BulkGrantApiKeyAction.NAME), same(request), any(UsernamePasswordToken.class), anyActionListener());

        final Set<RoleDescriptor> roleDescriptors = Set.of();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<RoleDescriptor>> listener = (ActionListener<Set<RoleDescriptor>>) inv.getArguments()[1];
            listener.onResponse(roleDescriptors);
            return null;
        }).when(resolver).resolveUserRoleDescriptors(any(Authentication.class), anyActionListener());

        doAnswer(inv -> {
            assertThat(inv.getArguments()[0], equalTo(authentication));
            assertThat(inv.getArguments()[1], sameInstance(request.getApiKeyRequests()));
            assertThat(inv.getArguments()[2], sameInstance(roleDescriptors));
            @SuppressWarnings("unchecked")
            ActionListener<BulkGrantApiKeyResponse> listener = (ActionListener<BulkGrantApiKeyResponse>) inv.getArguments()[3];
            listener.onResponse(response);
            return null;
        }).when(apiKeyService).bulkCreateApiKeys(any(Authentication.class), any(), any(), anyActionListener());

        final PlainActionFuture<BulkGrantApiKeyResponse> future = new PlainActionFuture<>();
        action.execute(null, request, future);

        assertThat(future.actionGet(), sameInstance(response));
        verify(authorizationService, never()).authorize(any(), any(), any(), anyActionListener());
    }

    private BulkGrantApiKeyRequest mockRequest() {
        final BulkGrantApiKeyRequest request = new BulkGrantApiKeyRequest();
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLengthBetween(6, 32), List.of(), null);
        createApiKeyRequest.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        request.setApiKeyRequests(List.of(createApiKeyRequest));
        return request;
    }
}
