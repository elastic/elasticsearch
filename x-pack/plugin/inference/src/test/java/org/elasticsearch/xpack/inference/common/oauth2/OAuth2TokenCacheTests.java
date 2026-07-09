/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.common.BroadcastMessageAction;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenCache.EXPIRY_SKEW;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenCache.OAUTH2_TOKEN_CACHE_ENABLED;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OAuth2TokenCacheTests extends ESTestCase {

    private static final String INFERENCE_ID = "inference-id";
    private static final String BEARER = "bearer-1";
    private static final Duration ONE_HOUR = Duration.ofHours(1);

    public void testCacheMiss_InvokesSupplierAndReturnsToken() {
        var cache = createEnabledCache();
        var token = new CachedToken(BEARER, Instant.now().plus(ONE_HOUR));
        var future = new TestPlainActionFuture<CachedToken>();
        var fetchCount = new AtomicInteger();

        cache.getToken(INFERENCE_ID, listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        }, future);

        assertThat(future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT), sameInstance(token));
        assertThat(fetchCount.get(), is(1));
    }

    public void testCacheHit_ReturnsCachedTokenWithoutInvokingSupplier() {
        var cache = createEnabledCache();
        var token = new CachedToken(BEARER, Instant.now().plus(ONE_HOUR));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future1);
        var cachedToken = future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
        assertThat(cachedToken, sameInstance(token));

        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future2);
        cachedToken = future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
        assertThat(cachedToken, sameInstance(token));
    }

    public void testExpiringSoon_TreatedAsMiss() {
        var cache = createEnabledCache();
        var expiringBearer = "expiring-bearer";
        var freshBearer = "fresh-bearer";
        // Token expires in less than EXPIRY_SKEW (60s) — considered expiring soon
        var expiringSoonToken = new CachedToken(expiringBearer, Instant.now().plus(EXPIRY_SKEW).minusSeconds(1));
        var freshToken = new CachedToken(freshBearer, Instant.now().plus(ONE_HOUR));

        var tokensToReturn = new ArrayDeque<>(List.of(expiringSoonToken, freshToken));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(tokensToReturn.poll());
        };

        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future1);
        assertThat(future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT).bearer(), is(expiringBearer));

        // Second call: cached token is expiring soon → must fetch again
        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future2);
        assertThat(future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT).bearer(), is(freshBearer));

        assertThat(fetchCount.get(), is(2));
    }

    public void testConcurrentCallers_CoalesceOntoOneFetch() throws InterruptedException {
        var cache = createEnabledCache();
        var token = new CachedToken("bearer-coalesced", Instant.now().plus(ONE_HOUR));

        var supplierStarted = new CountDownLatch(1);
        var supplierCanFinish = new CountDownLatch(1);
        var fetchCount = new AtomicInteger();

        OAuth2TokenSupplier blockingSupplier = listener -> {
            fetchCount.incrementAndGet();
            supplierStarted.countDown();
            try {
                assertTrue(supplierCanFinish.await(ESTestCase.TEST_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(e);
                return;
            }
            listener.onResponse(token);
        };

        var future1 = new TestPlainActionFuture<CachedToken>();
        var thread = new Thread(() -> cache.getToken(INFERENCE_ID, blockingSupplier, future1));
        thread.start();

        // Wait until the supplier is in-flight so the inflight map has the key
        assertTrue(supplierStarted.await(ESTestCase.TEST_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        // Second caller: supplier is still blocked, should attach to the existing in-flight future
        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, blockingSupplier, future2);

        // Release the supplier
        supplierCanFinish.countDown();

        assertThat(future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT), sameInstance(token));
        assertThat(future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT), sameInstance(token));
        assertThat(fetchCount.get(), is(1));

        thread.join(ESTestCase.TEST_REQUEST_TIMEOUT.millis());
    }

    public void testSupplierFailure_RemovesInflightEntry_AllowsRetry() {
        var cache = createEnabledCache();
        var failure = new RuntimeException("IdP unreachable");
        var retryBearer = "retry-bearer";
        var token = new CachedToken(retryBearer, Instant.now().plus(ONE_HOUR));
        var fetchCount = new AtomicInteger();

        // First call: supplier fails
        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, listener -> {
            fetchCount.incrementAndGet();
            listener.onFailure(failure);
        }, future1);
        expectThrows(RuntimeException.class, () -> future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
        assertThat(fetchCount.get(), is(1));

        // Second call: inflight entry should have been removed, supplier is invoked again
        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        }, future2);
        assertThat(future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT).bearer(), is(retryBearer));
        assertThat(fetchCount.get(), is(2));
    }

    public void testInvalidateOnlLocal_Node_RemovesEntry() {
        var cache = createEnabledCache();
        var token = new CachedToken(BEARER, Instant.now().plus(ONE_HOUR));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future1);
        future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));

        cache.invalidateLocal(new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT));

        // After invalidation the entry is gone, supplier is invoked again
        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future2);
        future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(2));
    }

    public void testInvalidateOnlLocal_Node_WithInferenceId_RemovesEntry() {
        var cache = createEnabledCache();
        var token = new CachedToken(BEARER, Instant.now().plus(ONE_HOUR));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future1);
        future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));

        cache.invalidateOnlLocalNode(INFERENCE_ID);

        // After invalidation the entry is gone, supplier is invoked again
        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(INFERENCE_ID, supplier, future2);
        future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(2));
    }

    @SuppressWarnings("unchecked")
    public void testInvalidate_BroadcastsToAllNodes() {
        var mockClient = mock(Client.class);
        var cache = createEnabledCache(Settings.EMPTY, mockClient);

        doAnswer(invocation -> {
            // The response value is ignored in the invalidate callback; passing null is safe
            var listener = (ActionListener<BroadcastMessageAction.Response>) invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(mockClient).execute(eq(ClearOAuth2TokenCacheAction.INSTANCE), any(), any());

        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        var future = new TestPlainActionFuture<Void>();
        cache.invalidate(key, future);
        future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        verify(mockClient).execute(eq(ClearOAuth2TokenCacheAction.INSTANCE), any(), any());
    }

    public void testInvalidate_NoOpWhenCacheDisabled() {
        var mockClient = mock(Client.class);
        var disabledSettings = Settings.builder().put(OAUTH2_TOKEN_CACHE_ENABLED.getKey(), false).build();
        var cache = createEnabledCache(disabledSettings, mockClient);

        var key = new InferenceIdAndProject(INFERENCE_ID, ProjectId.DEFAULT);
        var future = new TestPlainActionFuture<Void>();
        cache.invalidate(key, future);
        future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testStats_ReturnsEmptyWhenCacheDisabled() {
        var disabledSettings = Settings.builder().put(OAUTH2_TOKEN_CACHE_ENABLED.getKey(), false).build();
        var cache = createEnabledCache(disabledSettings, mock(Client.class));

        var stats = cache.stats();
        assertThat(stats.getHits(), is(0L));
        assertThat(stats.getMisses(), is(0L));
        assertThat(stats.getEvictions(), is(0L));
        assertThat(cache.cacheCount(), is(0));
    }

    public void testCacheEnabled_DynamicSettingToggle() {
        var clusterService = Utils.mockClusterService(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE))).thenReturn(true);
        var cache = new OAuth2TokenCache(
            clusterService,
            Settings.EMPTY,
            featureService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class)
        );
        cache.init();

        assertTrue(cache.cacheEnabled());

        // Apply the dynamic setting change through ClusterSettings
        clusterService.getClusterSettings().applySettings(Settings.builder().put(OAUTH2_TOKEN_CACHE_ENABLED.getKey(), false).build());

        assertFalse(cache.cacheEnabled());
    }

    public void testCacheEnabled_RequiresBothSettingAndFeatureGate() {
        var clusterService = Utils.mockClusterService(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var featureService = mock(FeatureService.class);
        // Feature gate returns false — cache must be disabled regardless of the setting
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE))).thenReturn(false);
        var cache = new OAuth2TokenCache(
            clusterService,
            Settings.EMPTY,
            featureService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class)
        );

        assertFalse(cache.cacheEnabled());
    }

    // --- helpers ---

    private static OAuth2TokenCache createEnabledCache() {
        return createEnabledCache(Settings.EMPTY, mock(Client.class));
    }

    public static OAuth2TokenCache createEnabledCache(Settings settings, Client client) {
        var clusterService = Utils.mockClusterService(settings);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE))).thenReturn(true);
        return new OAuth2TokenCache(clusterService, settings, featureService, TestProjectResolvers.DEFAULT_PROJECT_ONLY, client);
    }
}
