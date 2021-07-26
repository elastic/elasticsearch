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
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountTokenStore.StoreAuthenticationResult;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
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
        compositeStore = new CompositeServiceAccountTokenStore(List.of(store1, store2, store3), threadContext);
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

    public void testFindTokensFor() throws ExecutionException, InterruptedException {
        Mockito.reset(store1, store2, store3);

        final ServiceAccountId accountId1 = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final ServiceAccountId accountId2 = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final boolean store1Error = randomBoolean();
        final RuntimeException e = new RuntimeException("fail");
        final Set<TokenInfo> allTokenInfos = new HashSet<>();

        doAnswer(invocationOnMock -> {
            final ServiceAccountId accountId = (ServiceAccountId) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<TokenInfo>> listener =
                (ActionListener<Collection<TokenInfo>>) invocationOnMock.getArguments()[1];
            if (accountId == accountId1) {
                final Set<TokenInfo> tokenInfos = new HashSet<>();
                IntStream.range(0, randomIntBetween(0, 5)).forEach(i -> {
                    final TokenInfo tokenInfo = TokenInfo.fileToken(randomAlphaOfLengthBetween(3, 8));
                    tokenInfos.add(tokenInfo);
                });
                allTokenInfos.addAll(tokenInfos);
                listener.onResponse(tokenInfos);
            } else {
                if (store1Error) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(List.of());
                }
            }
            return null;
        }).when(store1).findTokensFor(any(), any());

        doAnswer(invocationOnMock -> {
            final ServiceAccountId accountId = (ServiceAccountId) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<TokenInfo>> listener =
                (ActionListener<Collection<TokenInfo>>) invocationOnMock.getArguments()[1];
            if (accountId == accountId1) {
                final Set<TokenInfo> tokenInfos = new HashSet<>();
                IntStream.range(0, randomIntBetween(0, 5)).forEach(i -> {
                    final TokenInfo tokenInfo = TokenInfo.indexToken(randomAlphaOfLengthBetween(3, 8));
                    tokenInfos.add(tokenInfo);
                });
                allTokenInfos.addAll(tokenInfos);
                listener.onResponse(tokenInfos);
            } else  {
                if (store1Error) {
                    listener.onResponse(List.of());
                } else {
                    listener.onFailure(e);
                }
            }
            return null;
        }).when(store2).findTokensFor(any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            final ActionListener<Collection<TokenInfo>> listener =
                (ActionListener<Collection<TokenInfo>>) invocationOnMock.getArguments()[1];
            listener.onResponse(List.of());
            return null;
        }).when(store3).findTokensFor(any(), any());

        final PlainActionFuture<Collection<TokenInfo>> future1 = new PlainActionFuture<>();
        compositeStore.findTokensFor(accountId1, future1);
        final Collection<TokenInfo> result = future1.get();
        assertThat(result.stream().collect(Collectors.toUnmodifiableSet()), equalTo(allTokenInfos));

        final PlainActionFuture<Collection<TokenInfo>> future2 = new PlainActionFuture<>();
        compositeStore.findTokensFor(accountId2, future2);
        final RuntimeException e2 = expectThrows(RuntimeException.class, future2::actionGet);
        assertThat(e2, is(e));
    }
}
