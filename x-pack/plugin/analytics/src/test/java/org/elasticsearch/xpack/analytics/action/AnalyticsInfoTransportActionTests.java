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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));
        Client client = mockClient();
        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(mock(TransportService.class), clusterService, null,
            mock(ActionFilters.class), null, client);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(task, null, clusterState, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(true));
        verify(client, times(1)).execute(any(), any(), any());
        verifyNoMoreInteractions(client);
    }

    public void testEnabled() throws Exception {
        AnalyticsInfoTransportAction featureSet = new AnalyticsInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
        assertTrue(featureSet.enabled());
        Client client = mockClient();
        AnalyticsUsageTransportAction usageAction = new AnalyticsUsageTransportAction(mock(TransportService.class),
            clusterService, null, mock(ActionFilters.class), null, client);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(task, null, clusterState, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertTrue(usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new AnalyticsFeatureSetUsage(out.bytes().streamInput());
        assertTrue(serializedUsage.enabled());
        verify(client, times(1)).execute(any(), any(), any());
        verifyNoMoreInteractions(client);
    }

    private Client mockClient() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AnalyticsStatsAction.Response> listener =
                (ActionListener<AnalyticsStatsAction.Response>) invocation.getArguments()[2];
            listener.onResponse(new AnalyticsStatsAction.Response(clusterName, Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(AnalyticsStatsAction.INSTANCE), any(), any());
        return client;
    }

}
