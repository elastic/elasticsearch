/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpatialInfoTransportActionTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void init() {
        clusterService = mock(ClusterService.class);

        DiscoveryNode discoveryNode = new DiscoveryNode("nodeId", buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        ClusterName clusterName = new ClusterName("cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterName);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterState.getClusterName()).thenReturn(clusterName);
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testAvailable() throws Exception {
        SpatialInfoTransportAction featureSet = new SpatialInfoTransportAction(mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));

        var usageAction = new SpatialUsageTransportAction(
            mock(TransportService.class),
            clusterService,
            null,
            mock(ActionFilters.class),
            null,
            mockClient()
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        Task task = new Task(1L, "_type", "_action", "_description", null, Collections.emptyMap());
        usageAction.masterOperation(task, null, clusterService.state(), future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(true));
    }

    public void testEnabled() throws Exception {
        SpatialInfoTransportAction featureSet = new SpatialInfoTransportAction(mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
        assertTrue(featureSet.enabled());

        SpatialUsageTransportAction usageAction = new SpatialUsageTransportAction(
            mock(TransportService.class),
            clusterService,
            null,
            mock(ActionFilters.class),
            null,
            mockClient()
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(mock(Task.class), null, clusterService.state(), future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertTrue(usage.enabled());

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SpatialFeatureSetUsage(out.bytes().streamInput());
        assertTrue(serializedUsage.enabled());
    }

    private Client mockClient() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<SpatialStatsAction.Response> listener = (ActionListener<SpatialStatsAction.Response>) invocation
                .getArguments()[2];
            listener.onResponse(
                new SpatialStatsAction.Response(clusterService.getClusterName(), Collections.emptyList(), Collections.emptyList())
            );
            return null;
        }).when(client).execute(eq(SpatialStatsAction.INSTANCE), any(), any());
        return client;
    }
}
