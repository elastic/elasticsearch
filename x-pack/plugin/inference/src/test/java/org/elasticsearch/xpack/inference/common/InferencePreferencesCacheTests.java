/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.regionpolicy.CspRegion;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.InferenceFeatures;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferencePreferencesCacheTests extends ESTestCase {

    public void testCacheMiss_FetchesAndCaches() {
        var regionPolicy = new RegionPolicy(null, List.of(new CspRegion("aws", "eu-west-1")));
        var fetchCount = new AtomicInteger();
        var cache = createCache(listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(regionPolicy);
        });

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        assertThat(future1.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));
        assertThat(fetchCount.get(), is(1));

        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        assertThat(future2.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));
        assertThat(fetchCount.get(), is(1)); // second call was a cache hit, no re-fetch
    }

    public void testCacheMiss_NoDocumentFound_CachesEmpty() {
        var fetchCount = new AtomicInteger();
        var cache = createCache(listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(null);
        });

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        assertThat(future1.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), nullValue());
        assertThat(fetchCount.get(), is(1));

        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        future2.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
    }

    public void testCacheMiss_ResourceNotFound_ReturnsEmptyWithoutFailing() {
        var cache = createCache(listener -> listener.onFailure(new ResourceNotFoundException("no region policy configured")));

        var future = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future);
        assertThat(future.actionGet(TEST_REQUEST_TIMEOUT), sameInstance(InferencePreferences.EMPTY));
    }

    public void testCacheMiss_UnexpectedFailure_PropagatesFailure() {
        var originalFailure = new RuntimeException("transient failure");
        var cache = createCache(listener -> listener.onFailure(originalFailure));

        var future = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getCause(), is(originalFailure));
    }

    public void testInvalidateLocal_ForcesRefetch() {
        var regionPolicy = new RegionPolicy(List.of("eu"), null);
        var fetchCount = new AtomicInteger();
        var cache = createCache(listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(regionPolicy);
        });

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        future1.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));

        cache.invalidateLocal();

        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        future2.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(2));
    }

    @SuppressWarnings("unchecked")
    public void testInvalidate_BroadcastsToCluster() {
        var mockClient = mock(Client.class);
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        var cache = new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mockClient,
            mockClusterService(),
            featureService(true),
            listener -> listener.onResponse(null)
        );

        doAnswer(invocation -> {
            var listener = (ActionListener<BroadcastMessageAction.Response>) invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(mockClient).execute(eq(ClearInferencePreferencesCacheAction.INSTANCE), any(), any());

        var future = new TestPlainActionFuture<Void>();
        cache.invalidate(future);
        future.actionGet(TEST_REQUEST_TIMEOUT);

        verify(mockClient).execute(eq(ClearInferencePreferencesCacheAction.INSTANCE), any(), any());
    }

    public void testCacheDisabled_AlwaysFetchesAndNeverCaches() {
        var regionPolicy = new RegionPolicy(List.of("eu"), null);
        var fetchCount = new AtomicInteger();
        var cache = new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class),
            mockClusterService(),
            featureService(false),
            listener -> {
                fetchCount.incrementAndGet();
                listener.onResponse(regionPolicy);
            }
        );

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        assertThat(future1.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));
        assertThat(fetchCount.get(), is(1));

        // With caching disabled, every get() re-fetches instead of reading a cached value.
        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        future2.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(2));
    }

    @SuppressWarnings("unchecked")
    public void testCacheDisabled_InvalidateDoesNotBroadcast() {
        var mockClient = mock(Client.class);
        var cache = new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mockClient,
            mockClusterService(),
            featureService(false),
            listener -> listener.onResponse(null)
        );

        var future = new TestPlainActionFuture<Void>();
        cache.invalidate(future);
        future.actionGet(TEST_REQUEST_TIMEOUT);

        verify(mockClient, never()).execute(eq(ClearInferencePreferencesCacheAction.INSTANCE), any(), any());
    }

    private InferencePreferencesCache createCache(InferencePreferencesCache.RegionPolicyFetcher fetcher) {
        return new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class),
            mockClusterService(),
            featureService(true),
            fetcher
        );
    }

    private static ClusterService mockClusterService() {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        return clusterService;
    }

    private static FeatureService featureService(boolean hasFeature) {
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_CLEAR_PREFERENCES_CACHE))).thenReturn(hasFeature);
        return featureService;
    }
}
