/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.List;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountTokenStore.StoreAuthenticationResult;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CompositeServiceAccountTokenStoreTests extends ESTestCase {

    private ThreadContext threadContext;
    private ServiceAccountTokenStore store1;
    private ServiceAccountTokenStore store2;
    private ServiceAccountTokenStore store3;
    private CompositeServiceAccountTokenStore compositeStore;

    @Before
    public void init() {
        threadContext = new ThreadContext(Settings.EMPTY);
        store1 = mock(ServiceAccountTokenStore.class);
        store2 = mock(ServiceAccountTokenStore.class);
        store3 = mock(ServiceAccountTokenStore.class);
        compositeStore = new CompositeServiceAccountTokenStore(
            List.of(store1, store2, store3), threadContext);
    }

    public void testAuthenticate() throws ExecutionException, InterruptedException {
        Mockito.reset(store1, store2, store3);

        final ServiceAccountToken token = mock(ServiceAccountToken.class);
        final boolean store1Success = randomBoolean();
        final boolean store2Success = randomBoolean();
        final boolean store3Success = randomBoolean();
        final TokenSource tokenSource = randomFrom(TokenSource.values());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked") final ActionListener<StoreAuthenticationResult> listener =
                (ActionListener<StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(new StoreAuthenticationResult(store1Success, tokenSource));
            return null;
        }).when(store1).authenticate(eq(token), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<StoreAuthenticationResult> listener =
                (ActionListener<StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(new StoreAuthenticationResult(store2Success, tokenSource));
            return null;
        }).when(store2).authenticate(eq(token), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<StoreAuthenticationResult> listener =
                (ActionListener<StoreAuthenticationResult>) invocationOnMock.getArguments()[1];
            listener.onResponse(new StoreAuthenticationResult(store3Success, tokenSource));
            return null;
        }).when(store3).authenticate(eq(token), any());

        final PlainActionFuture<StoreAuthenticationResult> future = new PlainActionFuture<>();
        compositeStore.authenticate(token, future);
        if (store1Success || store2Success || store3Success) {
            assertThat(future.get().isSuccess(), is(true));
            assertThat(future.get().getTokenSource(), is(tokenSource));
            if (store1Success) {
                verify(store1).authenticate(eq(token), any());
                verifyZeroInteractions(store2);
                verifyZeroInteractions(store3);
            } else if (store2Success) {
                verify(store1).authenticate(eq(token), any());
                verify(store2).authenticate(eq(token), any());
                verifyZeroInteractions(store3);
            } else {
                verify(store1).authenticate(eq(token), any());
                verify(store2).authenticate(eq(token), any());
                verify(store3).authenticate(eq(token), any());
            }
        } else {
            assertThat(future.get().isSuccess(), is(false));
            assertThat(future.get().getTokenSource(), is(tokenSource));
            verify(store1).authenticate(eq(token), any());
            verify(store2).authenticate(eq(token), any());
            verify(store3).authenticate(eq(token), any());
        }
    }
}
