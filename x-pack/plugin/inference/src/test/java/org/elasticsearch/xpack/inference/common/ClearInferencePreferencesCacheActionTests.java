/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.common.ClearInferencePreferencesCacheAction.ClearInferencePreferencesMessage;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClearInferencePreferencesCacheActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testReceiveMessage_InvalidatesLocalCacheEntry() {
        var regionPolicy = new RegionPolicy(List.of("eu"), null);
        var callCount = new AtomicInteger();
        var clusterService = mockClusterService();
        var cache = new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class),
            clusterService,
            featureService(true),
            listener -> {
                callCount.incrementAndGet();
                listener.onResponse(regionPolicy);
            }
        );

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        assertThat(future1.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));

        var action = new ClearInferencePreferencesCacheAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            clusterService,
            mock(ActionFilters.class),
            cache
        );

        action.receiveMessage(ClearInferencePreferencesMessage.INSTANCE);

        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        assertThat(future2.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));

        assertThat(callCount.get(), is(2));
    }

    public void testGettingTwiceWithoutClearingDoesNotRefetch() {
        var regionPolicy = new RegionPolicy(List.of("eu"), null);
        var callCount = new AtomicInteger();
        var cache = new InferencePreferencesCache(
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            mock(Client.class),
            mockClusterService(),
            featureService(true),
            listener -> {
                callCount.incrementAndGet();
                listener.onResponse(regionPolicy);
            }
        );

        var future1 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future1);
        assertThat(future1.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));

        var future2 = new TestPlainActionFuture<InferencePreferences>();
        cache.get(future2);
        assertThat(future2.actionGet(TEST_REQUEST_TIMEOUT).regionPolicy(), is(regionPolicy));

        assertThat(callCount.get(), is(1));
    }

    private ClusterService mockClusterService() {
        var clusterService = Utils.mockClusterService(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.threadPool()).thenReturn(threadPool);
        return clusterService;
    }

    private static FeatureService featureService(boolean hasFeature) {
        var featureService = mock(FeatureService.class);
        when(featureService.clusterHasFeature(any(), eq(InferenceFeatures.INFERENCE_CLEAR_PREFERENCES_CACHE))).thenReturn(hasFeature);
        return featureService;
    }
}
