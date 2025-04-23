/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AnalyticsInfoTransportActionTests extends ESTestCase {

    private Task task;
    private ClusterService clusterService;
    private ClusterName clusterName;
    private ClusterState clusterState;

    @Before
    public void init() {
        task = mock(Task.class);
        when(task.getId()).thenReturn(randomLong());
        clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(discoveryNode.getId()).thenReturn(randomAlphaOfLength(10));
        when(clusterService.localNode()).thenReturn(discoveryNode);
        clusterName = new ClusterName(randomAlphaOfLength(10));
        clusterState = mock(ClusterState.class);
        when(clusterState.getClusterName()).thenReturn(clusterName);
    }

    public void testAvailable() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));
        Client client = mockClient();
        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            client
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(task, null, clusterState, future);
        XPackFeatureUsage usage = future.get().getUsage();
        assertThat(usage.available(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureUsage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(true));
        verify(client, times(1)).execute(any(), any(), any());
        verifyNoMoreInteractions(client);
    }

    public void testEnabled() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
        assertTrue(featureSet.enabled());
        Client client = mockClient();
        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            client
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(task, null, clusterState, future);
        XPackFeatureUsage usage = future.get().getUsage();
        assertTrue(usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureUsage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertTrue(serializedUsage.enabled());
        verify(client, times(1)).execute(any(), any(), any());
        verifyNoMoreInteractions(client);
    }

    private Client mockClient() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AnalyticsStatsAction.Response> listener = (ActionListener<AnalyticsStatsAction.Response>) invocation
                .getArguments()[2];
            listener.onResponse(new AnalyticsStatsAction.Response(clusterName, Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(AnalyticsStatsAction.INSTANCE), any(), any());
        return client;
    }

}
