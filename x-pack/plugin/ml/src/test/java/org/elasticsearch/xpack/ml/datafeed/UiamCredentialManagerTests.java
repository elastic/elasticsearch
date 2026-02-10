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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyResponse;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UiamCredentialManagerTests extends ESTestCase {

    public void testGrantInternalApiKey_ThrowsNotImplemented() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        UiamCredentialManager manager = new UiamCredentialManager(
            mock(Client.class),
            threadPool,
            mock(ClusterService.class),
            Settings.EMPTY
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        manager.grantInternalApiKey("test-datafeed", ActionListener.wrap(result -> fail("Expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get(), instanceOf(UnsupportedOperationException.class));
        assertThat(failure.get().getMessage(), containsString("GrantInternalUniversalApiKey is not yet implemented"));
        assertThat(failure.get().getMessage(), containsString("test-datafeed"));
        assertThat(failure.get().getMessage(), containsString("UIAM _grant API"));
    }

    public void testHasUiamCredential() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        UiamCredentialManager manager = new UiamCredentialManager(
            mock(Client.class),
            threadPool,
            mock(ClusterService.class),
            Settings.EMPTY
        );

        assertFalse(manager.hasUiamCredential());

        threadContext.putHeader(UiamCredentialManager.AUTHENTICATING_TOKEN_HEADER, "test-token");
        assertTrue(manager.hasUiamCredential());
    }

    public void testRevokeApiKey_NullApiKeyId() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        UiamCredentialManager manager = new UiamCredentialManager(
            mock(Client.class),
            threadPool,
            mock(ClusterService.class),
            Settings.EMPTY
        );

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey(null, "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(false));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_Success() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        doAnswer(invocation -> {
            ActionListener<InvalidateApiKeyResponse> listener = (ActionListener<InvalidateApiKeyResponse>) invocation.getArguments()[2];
            listener.onResponse(new InvalidateApiKeyResponse(List.of("test-key-id"), Collections.emptyList(), null));
            return null;
        }).when(client).execute(same(InvalidateApiKeyAction.INSTANCE), any(), any());

        UiamCredentialManager manager = new UiamCredentialManager(client, threadPool, mock(ClusterService.class), Settings.EMPTY);

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey("test-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_KeyNotFound() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        doAnswer(invocation -> {
            ActionListener<InvalidateApiKeyResponse> listener = (ActionListener<InvalidateApiKeyResponse>) invocation.getArguments()[2];
            listener.onResponse(new InvalidateApiKeyResponse(Collections.emptyList(), Collections.emptyList(), null));
            return null;
        }).when(client).execute(same(InvalidateApiKeyAction.INSTANCE), any(), any());

        UiamCredentialManager manager = new UiamCredentialManager(client, threadPool, mock(ClusterService.class), Settings.EMPTY);

        AtomicReference<Boolean> result = new AtomicReference<>();
        manager.revokeApiKey("old-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success")));

        assertThat(result.get(), is(false));
    }

    @SuppressWarnings("unchecked")
    public void testRevokeApiKey_Failure() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        Client client = mock(Client.class);

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("revoke failed"));
            return null;
        }).when(client).execute(any(ActionType.class), any(), any());

        UiamCredentialManager manager = new UiamCredentialManager(client, threadPool, mock(ClusterService.class), Settings.EMPTY);

        AtomicReference<Boolean> result = new AtomicReference<>();
        // Revocation failures should not fail the listener
        manager.revokeApiKey("test-key-id", "test-datafeed", ActionListener.wrap(result::set, e -> fail("Expected success, not failure")));

        assertThat(result.get(), is(false));
    }

    public void testHeadersForCpsSearch_NoStaleToken() {
        var headers = java.util.Map.of("Authorization", "Bearer test", "_security_serverless_authenticating_token", "some-token");
        var result = UiamCredentialManager.headersForCpsSearch(headers);
        // Original map returned as-is since no request_scoped_credential
        assertSame(headers, result);
    }

    public void testHeadersForCpsSearch_WithStaleToken() {
        var headers = new java.util.HashMap<String, String>();
        headers.put("_security_serverless_authenticating_token", "some-token");
        headers.put("_security_serverless_request_scoped_credential", "stale-token");
        headers.put("other-header", "value");

        var result = UiamCredentialManager.headersForCpsSearch(headers);

        assertNotSame(headers, result);
        assertThat(result.containsKey("_security_serverless_request_scoped_credential"), is(false));
        assertThat(result.get("_security_serverless_authenticating_token"), is("some-token"));
        assertThat(result.get("other-header"), is("value"));
    }
}
