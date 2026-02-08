/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CpsCredentialManagerTests extends ESTestCase {

    public void testGrantInternalApiKey_NoUiamToken() {
        Settings settings = Settings.builder().put(CpsCredentialManager.SHARED_SERVICE_SECRET_SETTING.getKey(), "test-secret").build();
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        CpsCredentialManager manager = new CpsCredentialManager(mock(Client.class), threadPool, mock(ClusterService.class), settings);

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.grantInternalApiKey("test-datafeed", ActionListener.wrap(result -> fail("Expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("no UIAM authenticating token"));
    }

    public void testGrantInternalApiKey_NoSharedSecret() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(CpsCredentialManager.AUTHENTICATING_TOKEN_HEADER, "test-uiam-token");
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        CpsCredentialManager manager = new CpsCredentialManager(mock(Client.class), threadPool, mock(ClusterService.class), settings);

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.grantInternalApiKey("test-datafeed", ActionListener.wrap(result -> fail("Expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("shared service secret is not configured"));
    }

    @SuppressWarnings("unchecked")
    public void testGrantInternalApiKey_Success() {
        Settings settings = Settings.builder().put(CpsCredentialManager.SHARED_SERVICE_SECRET_SETTING.getKey(), "test-secret").build();
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(CpsCredentialManager.AUTHENTICATING_TOKEN_HEADER, "test-uiam-token");
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class, org.mockito.Answers.RETURNS_DEEP_STUBS);
        when(clusterService.state()).thenReturn(clusterState);

        String apiKeyId = "generated-key-id";
        SecureString apiKeySecret = new SecureString("generated-key-secret".toCharArray());

        // Mock GrantApiKeyAction to return a successful response
        doAnswer(invocation -> {
            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) invocation.getArguments()[2];
            listener.onResponse(new CreateApiKeyResponse("_ml_datafeed_test-datafeed", apiKeyId, apiKeySecret, null));
            return null;
        }).when(client).execute(same(GrantApiKeyAction.INSTANCE), any(), any());

        // Mock AuthenticateAction to return a successful response
        doAnswer(invocation -> {
            ActionListener<AuthenticateResponse> listener = (ActionListener<AuthenticateResponse>) invocation.getArguments()[2];
            listener.onResponse(mock(AuthenticateResponse.class));
            return null;
        }).when(client).execute(same(AuthenticateAction.INSTANCE), any(), any());

        CpsCredentialManager manager = new CpsCredentialManager(client, threadPool, clusterService, settings);

        AtomicReference<CpsCredentialManager.InternalApiKeyResult> resultRef = new AtomicReference<>();
        manager.grantInternalApiKey("test-datafeed", ActionListener.wrap(resultRef::set, e -> fail("Expected success but got: " + e)));

        assertThat(resultRef.get(), notNullValue());
        assertThat(resultRef.get().apiKeyId(), is(apiKeyId));

        // Verify encoded credential is Base64(id:secret)
        String expectedEncoded = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));
        assertThat(resultRef.get().encodedCredential(), is(expectedEncoded));
        assertThat(resultRef.get().authHeaders(), notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testGrantInternalApiKey_GrantFails() {
        Settings settings = Settings.builder().put(CpsCredentialManager.SHARED_SERVICE_SECRET_SETTING.getKey(), "test-secret").build();
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(CpsCredentialManager.AUTHENTICATING_TOKEN_HEADER, "test-uiam-token");
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Client client = mock(Client.class);

        // Mock GrantApiKeyAction to fail
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("grant failed"));
            return null;
        }).when(client).execute(same(GrantApiKeyAction.INSTANCE), any(), any());

        CpsCredentialManager manager = new CpsCredentialManager(client, threadPool, mock(ClusterService.class), settings);

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.grantInternalApiKey("test-datafeed", ActionListener.wrap(result -> fail("Expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("grant failed"));
    }

    public void testHasUiamCredential() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        CpsCredentialManager manager = new CpsCredentialManager(mock(Client.class), threadPool, mock(ClusterService.class), settings);

        assertFalse(manager.hasUiamCredential());

        threadContext.putHeader(CpsCredentialManager.AUTHENTICATING_TOKEN_HEADER, "test-token");
        assertTrue(manager.hasUiamCredential());
    }

    public void testRevokeApiKey_NullApiKeyId() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        CpsCredentialManager manager = new CpsCredentialManager(mock(Client.class), threadPool, mock(ClusterService.class), settings);

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey(null, "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(false));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_Success() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        // Mock InvalidateApiKeyAction to return a successful invalidation
        doAnswer(invocation -> {
            ActionListener<InvalidateApiKeyResponse> listener = (ActionListener<InvalidateApiKeyResponse>) invocation.getArguments()[2];
            listener.onResponse(new InvalidateApiKeyResponse(List.of("test-key-id"), Collections.emptyList(), null));
            return null;
        }).when(client).execute(same(InvalidateApiKeyAction.INSTANCE), any(), any());

        CpsCredentialManager manager = new CpsCredentialManager(client, threadPool, mock(ClusterService.class), settings);

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey("test-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_KeyNotFound() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        // Mock InvalidateApiKeyAction to return empty (key already invalidated)
        doAnswer(invocation -> {
            ActionListener<InvalidateApiKeyResponse> listener = (ActionListener<InvalidateApiKeyResponse>) invocation.getArguments()[2];
            listener.onResponse(new InvalidateApiKeyResponse(Collections.emptyList(), Collections.emptyList(), null));
            return null;
        }).when(client).execute(same(InvalidateApiKeyAction.INSTANCE), any(), any());

        CpsCredentialManager manager = new CpsCredentialManager(client, threadPool, mock(ClusterService.class), settings);

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey("old-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(false));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_Failure() {
        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        // Mock InvalidateApiKeyAction to fail
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("revoke failed"));
            return null;
        }).when(client).execute(any(ActionType.class), any(), any());

        CpsCredentialManager manager = new CpsCredentialManager(client, threadPool, mock(ClusterService.class), settings);

        AtomicReference<Boolean> result = new AtomicReference<>();
        // Revocation failures should not fail the listener
        manager.revokeApiKey("test-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success, not failure")));

        assertThat(result.get(), is(false));
    }

    public void testHeadersForCpsSearch_NoStaleToken() {
        var headers = java.util.Map.of("Authorization", "Bearer test", "_security_serverless_authenticating_token", "some-token");
        var result = CpsCredentialManager.headersForCpsSearch(headers);
        // Original map returned as-is since no request_scoped_credential
        assertSame(headers, result);
    }

    public void testHeadersForCpsSearch_WithStaleToken() {
        var headers = new java.util.HashMap<String, String>();
        headers.put("_security_serverless_authenticating_token", "some-token");
        headers.put("_security_serverless_request_scoped_credential", "stale-token");
        headers.put("other-header", "value");

        var result = CpsCredentialManager.headersForCpsSearch(headers);

        assertNotSame(headers, result);
        assertThat(result.containsKey("_security_serverless_request_scoped_credential"), is(false));
        assertThat(result.get("_security_serverless_authenticating_token"), is("some-token"));
        assertThat(result.get("other-header"), is("value"));
    }
}
