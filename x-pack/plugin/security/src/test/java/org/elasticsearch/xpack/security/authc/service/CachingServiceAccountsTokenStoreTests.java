/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class CachingServiceAccountsTokenStoreTests extends ESTestCase {

    private Settings globalSettings;
    private ThreadPool threadPool;

    @Before
    public void init() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        threadPool = new TestThreadPool("test");
    }

    @After
    public void stop() {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    public void testCache() throws ExecutionException, InterruptedException {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final SecureString validSecret = new SecureString("super-secret-value".toCharArray());
        final SecureString invalidSecret = new SecureString("some-fishy-value".toCharArray());
        final ServiceAccountToken token1Valid = new ServiceAccountToken(accountId, "token1", validSecret);
        final ServiceAccountToken token1Invalid = new ServiceAccountToken(accountId, "token1", invalidSecret);
        final ServiceAccountToken token2Valid = new ServiceAccountToken(accountId, "token2", validSecret);
        final ServiceAccountToken token2Invalid = new ServiceAccountToken(accountId, "token2", invalidSecret);
        final AtomicBoolean doAuthenticateInvoked = new AtomicBoolean(false);

        final CachingServiceAccountsTokenStore store = new CachingServiceAccountsTokenStore(globalSettings, threadPool) {
            @Override
            void doAuthenticate(ServiceAccountToken token, ActionListener<Boolean> listener) {
                doAuthenticateInvoked.set(true);
                listener.onResponse(validSecret.equals(token.getSecret()));
            }
        };

        final Cache<String, ListenableFuture<CachingServiceAccountsTokenStore.CachedResult>> cache = store.getCache();
        assertThat(cache.count(), equalTo(0));

        // 1st auth with the right token1
        final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
        store.authenticate(token1Valid, future1);
        assertThat(future1.get(), is(true));
        assertThat(doAuthenticateInvoked.get(), is(true));
        assertThat(cache.count(), equalTo(1));
        doAuthenticateInvoked.set(false); // reset

        // 2nd auth with the right token1 should use cache
        final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
        store.authenticate(token1Valid, future2);
        assertThat(future2.get(), is(true));
        assertThat(doAuthenticateInvoked.get(), is(false));

        // 3rd auth with the wrong token1 that has the same qualified name should use cache
        final PlainActionFuture<Boolean> future3 = new PlainActionFuture<>();
        store.authenticate(token1Invalid, future3);
        assertThat(future3.get(), is(false));
        assertThat(doAuthenticateInvoked.get(), is(false));

        // 4th auth with the wrong token2
        final PlainActionFuture<Boolean> future4 = new PlainActionFuture<>();
        store.authenticate(token2Invalid, future4);
        assertThat(future4.get(), is(false));
        assertThat(doAuthenticateInvoked.get(), is(true));
        assertThat(cache.count(), equalTo(2));
        doAuthenticateInvoked.set(false); // reset

        // 5th auth with the wrong token2 again should use cache
        final PlainActionFuture<Boolean> future5 = new PlainActionFuture<>();
        store.authenticate(token2Invalid, future5);
        assertThat(future5.get(), is(false));
        assertThat(doAuthenticateInvoked.get(), is(false));

        // 6th auth with the right token2
        final PlainActionFuture<Boolean> future6 = new PlainActionFuture<>();
        store.authenticate(token2Valid, future6);
        assertThat(future6.get(), is(true));
        assertThat(doAuthenticateInvoked.get(), is(true));
        assertThat(cache.count(), equalTo(2));
        doAuthenticateInvoked.set(false); // reset

        // Invalidate token1 in the cache
        store.invalidate(List.of(token1Valid.getQualifiedName()));
        assertThat(cache.count(), equalTo(1));

        // 7th auth with the right token1
        final PlainActionFuture<Boolean> future7 = new PlainActionFuture<>();
        store.authenticate(token1Valid, future7);
        assertThat(future7.get(), is(true));
        assertThat(doAuthenticateInvoked.get(), is(true));
        assertThat(cache.count(), equalTo(2));
        doAuthenticateInvoked.set(false); // reset

        // Invalidate all items in the cache
        store.invalidateAll();
        assertThat(cache.count(), equalTo(0));
    }

    public void testCacheCanBeDisabled() throws ExecutionException, InterruptedException {
        final Settings settings = Settings.builder()
            .put(globalSettings)
            .put(CachingServiceAccountsTokenStore.CACHE_TTL_SETTING.getKey(), "0")
            .build();

        final boolean success = randomBoolean();

        final CachingServiceAccountsTokenStore store = new CachingServiceAccountsTokenStore(settings, threadPool) {
            @Override
            void doAuthenticate(ServiceAccountToken token, ActionListener<Boolean> listener) {
                listener.onResponse(success);
            }
        };
        assertThat(store.getCache(), nullValue());
        // authenticate should still work
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        store.authenticate(mock(ServiceAccountToken.class), future);
        assertThat(future.get(), is(success));
    }
}
