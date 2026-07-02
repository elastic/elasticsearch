/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.inference.action.PutRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.action.RegionPolicyResponse;
import org.elasticsearch.xpack.core.inference.regionpolicy.CspRegion;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicy;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutRegionPolicyActionTests extends ESTestCase {

    private static final String CSP = "aws";
    private static final String REGION = "us-east-1";
    private static final ShardId SHARD_ID = new ShardId(InferenceIndex.INDEX_NAME, "_na_", 0);
    private static final Settings SECURITY_DISABLED_SETTINGS = Settings.builder()
        .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
        .build();

    private Client mockClient;
    private ClusterService mockClusterService;
    private FeatureService mockFeatureService;

    @Before
    public void init() {
        mockClient = mock(Client.class);
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        mockFeatureService = mock(FeatureService.class);

        // No region policy exists yet, so the put is treated as a create.
        givenSearchFailsWith(new IndexNotFoundException(InferenceIndex.INDEX_NAME));
        givenIndexRespondsWith(createdResponse());
    }

    public void testPut_TriggersAuthorizationRefresh_AndReturnsPolicy() {
        givenRefreshRespondsWith(ActionResponse.Empty.INSTANCE);

        var future = new TestPlainActionFuture<RegionPolicyResponse>();
        createAction().doExecute(null, new PutRegionPolicyAction.Request(newRegionPolicy()), future);

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy(), is(newRegionPolicy()));
        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    public void testPut_SwallowsRefreshFailure_AndStillReturnsPolicy() {
        givenRefreshFailsWith(new RuntimeException("refresh failed"));

        var future = new TestPlainActionFuture<RegionPolicyResponse>();
        createAction().doExecute(null, new PutRegionPolicyAction.Request(newRegionPolicy()), future);

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(response.regionPolicy().regionPolicy(), is(newRegionPolicy()));
        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private static RegionPolicy newRegionPolicy() {
        return new RegionPolicy(null, List.of(new CspRegion(CSP, REGION)), null);
    }

    private static IndexResponse createdResponse() {
        return new IndexResponse(SHARD_ID, RegionPolicyDoc.DOCUMENT_ID, 1L, 1L, 1L, true);
    }

    private void givenSearchFailsWith(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<?> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(mockClient).execute(eq(TransportSearchAction.TYPE), any(), any());
    }

    private void givenIndexRespondsWith(IndexResponse response) {
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(TransportIndexAction.TYPE), any(), any());
    }

    private void givenRefreshRespondsWith(ActionResponse.Empty response) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private void givenRefreshFailsWith(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private TransportPutRegionPolicyAction createAction() {
        return new TransportPutRegionPolicyAction(
            SECURITY_DISABLED_SETTINGS,
            mock(TransportService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mockClient,
            mockClusterService,
            mockFeatureService
        );
    }
}
