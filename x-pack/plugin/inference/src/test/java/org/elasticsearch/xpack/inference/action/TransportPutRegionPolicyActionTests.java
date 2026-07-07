/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.PutRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.RegionPolicyResponse;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportPutRegionPolicyActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_InvalidatesCache() {
        var cache = mock(InferencePreferencesCache.class);
        var invalidateCount = new AtomicInteger();
        doAnswer(invocation -> {
            invalidateCount.incrementAndGet();
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(invalidateCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_RefreshesAuthorizedEndpoints() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var refreshCount = new AtomicInteger();
        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    refreshCount.incrementAndGet();
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(refreshCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulPut_SwallowsRefreshFailure() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    var searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(SearchHits.EMPTY_WITHOUT_TOTAL_HITS);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof IndexRequest) {
                    listener.onResponse((Response) mock(IndexResponse.class));
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onFailure(new RuntimeException("refresh failed"));
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var action = new TransportPutRegionPolicyAction(
            Settings.EMPTY,
            mock(TransportService.class),
            threadPool,
            mock(ActionFilters.class),
            client,
            clusterService,
            mock(FeatureService.class),
            cache
        );

        var listener = new TestPlainActionFuture<RegionPolicyResponse>();
        action.doExecute(mock(Task.class), new PutRegionPolicyAction.Request(new RegionPolicy(List.of("eu"), null)), listener);

        // The put should still succeed even though the refresh failed
        var response = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy().allowedGeos(), is(List.of("eu")));
    }
}
