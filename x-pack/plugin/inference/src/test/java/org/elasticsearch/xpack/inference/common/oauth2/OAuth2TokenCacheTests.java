/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

// TODO: add an end-to-end integration test against a real OAuth2 IdP (mock-IdP + YAML REST) in a future PR.

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(10);

    public void testCacheMiss_invokesSupplierAndReturnsToken() {
        var cache = createEnabledCache();
        var token = new CachedToken("bearer-1", Instant.now().plusSeconds(3600));
        var future = new PlainActionFuture<CachedToken>();

        cache.getToken("inference-id", listener -> listener.onResponse(token), future);

        assertThat(future.actionGet(TIMEOUT), sameInstance(token));
    }

    public void testCacheHit_returnsCachedTokenWithoutInvokingSupplier() {
        var cache = createEnabledCache();
        var token = new CachedToken("bearer-1", Instant.now().plusSeconds(3600));
        var fetchCount = new AtomicInteger(0);
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var future1 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future1);
        future1.actionGet(TIMEOUT);

        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future2);
        future2.actionGet(TIMEOUT);

        assertThat(fetchCount.get(), is(1));
    }

    public void testExpiringSoon_treatedAsMiss() {
        var cache = createEnabledCache();
        // Token expires in less than EXPIRY_SKEW (60s) — considered expiring soon
        var expiringSoonToken = new CachedToken("expiring-bearer", Instant.now().plus(Duration.ofSeconds(30)));
        var freshToken = new CachedToken("fresh-bearer", Instant.now().plusSeconds(3600));

        var tokensToReturn = new ArrayDeque<>(List.of(expiringSoonToken, freshToken));
        var fetchCount = new AtomicInteger(0);
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(tokensToReturn.poll());
        };

        var future1 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future1);
        assertThat(future1.actionGet(TIMEOUT).bearer(), is("expiring-bearer"));

        // Second call: cached token is expiring soon → must fetch again
        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future2);
        assertThat(future2.actionGet(TIMEOUT).bearer(), is("fresh-bearer"));

        assertThat(fetchCount.get(), is(2));
    }

    public void testConcurrentCallers_coalesceOntoOneFetch() throws InterruptedException {
        var cache = createEnabledCache();
        var token = new CachedToken("bearer-coalesced", Instant.now().plusSeconds(3600));

        var supplierStarted = new CountDownLatch(1);
        var supplierCanFinish = new CountDownLatch(1);
        var fetchCount = new AtomicInteger(0);

        OAuth2TokenSupplier blockingSupplier = listener -> {
            fetchCount.incrementAndGet();
            supplierStarted.countDown();
            try {
                assertTrue(supplierCanFinish.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(e);
                return;
            }
            listener.onResponse(token);
        };

        var future1 = new PlainActionFuture<CachedToken>();
        var thread = new Thread(() -> cache.getToken("inference-id", blockingSupplier, future1));
        thread.start();

        // Wait until the supplier is in-flight so the inflight map has the key
        assertTrue(supplierStarted.await(10, TimeUnit.SECONDS));

        // Second caller: supplier is still blocked, should attach to the existing in-flight future
        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", blockingSupplier, future2);

        // Release the supplier
        supplierCanFinish.countDown();

        assertThat(future1.actionGet(TIMEOUT), sameInstance(token));
        assertThat(future2.actionGet(TIMEOUT), sameInstance(token));
        assertThat(fetchCount.get(), is(1));

        thread.join(TIMEOUT.millis());
    }

    public void testSupplierFailure_removesInflightEntry_allowsRetry() {
        var cache = createEnabledCache();
        var failure = new RuntimeException("IdP unreachable");
        var token = new CachedToken("retry-bearer", Instant.now().plusSeconds(3600));
        var fetchCount = new AtomicInteger(0);

        // First call: supplier fails
        var future1 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", listener -> {
            fetchCount.incrementAndGet();
            listener.onFailure(failure);
        }, future1);
        expectThrows(RuntimeException.class, () -> future1.actionGet(TIMEOUT));
        assertThat(fetchCount.get(), is(1));

        // Second call: inflight entry should have been removed, supplier is invoked again
        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        }, future2);
        assertThat(future2.actionGet(TIMEOUT).bearer(), is("retry-bearer"));
        assertThat(fetchCount.get(), is(2));
    }

    public void testInvalidateLocal_removesEntry() {
        var cache = createEnabledCache();
        var token = new CachedToken("bearer-1", Instant.now().plusSeconds(3600));
        var fetchCount = new AtomicInteger(0);
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var future1 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future1);
        future1.actionGet(TIMEOUT);
        assertThat(fetchCount.get(), is(1));

        cache.invalidateLocal(new InferenceIdAndProject("inference-id", ProjectId.DEFAULT));

        // After invalidation the entry is gone, supplier is invoked again
        var future2 = new PlainActionFuture<CachedToken>();
        cache.getToken("inference-id", supplier, future2);
        future2.actionGet(TIMEOUT);
        assertThat(fetchCount.get(), is(2));
    }

    @SuppressWarnings("unchecked")
    public void testInvalidate_broadcastsToAllNodes() {
        var mockClient = mock(org.elasticsearch.client.internal.Client.class);
        var cache = createEnabledCache(Settings.EMPTY, mockClient);

        doAnswer(invocation -> {
            // The response value is ignored in the invalidate callback; passing null is safe
            var listener = (ActionListener<BroadcastMessageAction.Response>) invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(mockClient).execute(eq(ClearOAuth2TokenCacheAction.INSTANCE), any(), any());

        var key = new InferenceIdAndProject("inference-id", ProjectId.DEFAULT);
        var future = new PlainActionFuture<Void>();
        cache.invalidate(key, future);
        future.actionGet(TIMEOUT);

        verify(mockClient).execute(eq(ClearOAuth2TokenCacheAction.INSTANCE), any(), any());
    }

    public void testInvalidate_noOpWhenCacheDisabled() {
        var mockClient = mock(org.elasticsearch.client.internal.Client.class);
        var disabledSettings = Settings.builder().put("xpack.inference.oauth2.token_cache.enabled", false).build();
        var cache = createEnabledCache(disabledSettings, mockClient);

        var key = new InferenceIdAndProject("inference-id", ProjectId.DEFAULT);
        var future = new PlainActionFuture<Void>();
        cache.invalidate(key, future);
        future.actionGet(TIMEOUT);

        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testStats_returnsEmptyWhenCacheDisabled() {
        var disabledSettings = Settings.builder().put("xpack.inference.oauth2.token_cache.enabled", false).build();
        var cache = createEnabledCache(disabledSettings, mock(org.elasticsearch.client.internal.Client.class));

        var stats = cache.stats();
        assertThat(stats.getHits(), is(0L));
        assertThat(stats.getMisses(), is(0L));
        assertThat(stats.getEvictions(), is(0L));
        assertThat(cache.cacheCount(), is(0));
    }

    public void testCacheEnabled_dynamicSettingToggle() {
        var clusterService = Utils.mockClusterService(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE))).thenReturn(true);
        var cache = new OAuth2TokenCache(
            clusterService,
            Settings.EMPTY,
            featureService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(org.elasticsearch.client.internal.Client.class)
        );

        assertTrue(cache.cacheEnabled());

        // Apply the dynamic setting change through ClusterSettings
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put("xpack.inference.oauth2.token_cache.enabled", false).build());

        assertFalse(cache.cacheEnabled());
    }

    public void testCacheEnabled_requiresBothSettingAndFeatureGate() {
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
            mock(org.elasticsearch.client.internal.Client.class)
        );

        assertFalse(cache.cacheEnabled());
    }

    // --- helpers ---

    private OAuth2TokenCache createEnabledCache() {
        return createEnabledCache(Settings.EMPTY, mock(org.elasticsearch.client.internal.Client.class));
    }

    private OAuth2TokenCache createEnabledCache(Settings settings, org.elasticsearch.client.internal.Client client) {
        var clusterService = Utils.mockClusterService(settings);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_OAUTH2_TOKEN_CACHE))).thenReturn(true);
        return new OAuth2TokenCache(clusterService, settings, featureService, TestProjectResolvers.DEFAULT_PROJECT_ONLY, client);
    }
}
