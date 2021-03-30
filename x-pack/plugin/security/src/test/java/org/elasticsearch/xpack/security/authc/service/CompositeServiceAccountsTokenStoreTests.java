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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CompositeServiceAccountsTokenStoreTests extends ESTestCase {

    private ThreadContext threadContext;

    @Before
    public void init() {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testAuthenticate() throws ExecutionException, InterruptedException {
        final ServiceAccountToken token = mock(ServiceAccountToken.class);

        final ServiceAccountsTokenStore store1 = mock(ServiceAccountsTokenStore.class);
        final ServiceAccountsTokenStore store2 = mock(ServiceAccountsTokenStore.class);
        final ServiceAccountsTokenStore store3 = mock(ServiceAccountsTokenStore.class);

        final boolean store1Success = randomBoolean();
        final boolean store2Success = randomBoolean();
        final boolean store3Success = randomBoolean();

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
            listener.onResponse(store1Success);
            return null;
        }).when(store1).authenticate(eq(token), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
            listener.onResponse(store2Success);
            return null;
        }).when(store2).authenticate(eq(token), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Boolean> listener = (ActionListener<Boolean>) invocationOnMock.getArguments()[1];
            listener.onResponse(store3Success);
            return null;
        }).when(store3).authenticate(eq(token), any());

        final ServiceAccountsTokenStore.CompositeServiceAccountsTokenStore compositeStore =
            new ServiceAccountsTokenStore.CompositeServiceAccountsTokenStore(List.of(store1, store2, store3), threadContext);

        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        compositeStore.authenticate(token, future);
        if (store1Success || store2Success || store3Success) {
            assertThat(future.get(), is(true));
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
            assertThat(future.get(), is(false));
            verify(store1).authenticate(eq(token), any());
            verify(store2).authenticate(eq(token), any());
            verify(store3).authenticate(eq(token), any());
        }
    }
}
