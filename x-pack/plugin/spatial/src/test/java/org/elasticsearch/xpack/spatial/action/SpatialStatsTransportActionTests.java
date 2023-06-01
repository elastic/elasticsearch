/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;
import org.elasticsearch.xpack.spatial.SpatialUsage;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpatialStatsTransportActionTests extends ESTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void mockServices() {
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);

        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("nodeId");
        when(clusterService.localNode()).thenReturn(discoveryNode);
        ClusterName clusterName = new ClusterName("cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterName);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testUsage() throws IOException {
        for (SpatialStatsAction.Item item : SpatialStatsAction.Item.values()) {
            SpatialUsage usage = new SpatialUsage();
            ContextParser<Void, Void> parser = usage.track(item, (p, c) -> c);
            int count = between(1, 10000);
            for (int i = 0; i < count; i++) {
                assertNull(parser.parse(null, null));
            }
            ObjectPath used = buildSpatialStatsResponse(usage);
            assertThat(item.name(), used.evaluate("stats." + item.name().toLowerCase(Locale.ROOT) + "_usage"), equalTo(count));
        }
    }

    private SpatialStatsTransportAction toAction(SpatialUsage nodeUsage) {
        return new SpatialStatsTransportAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            nodeUsage
        );
    }

    private ObjectPath buildSpatialStatsResponse(SpatialUsage... nodeUsages) throws IOException {
        SpatialStatsAction.Request request = new SpatialStatsAction.Request();
        List<SpatialStatsAction.NodeResponse> nodeResponses = Arrays.stream(nodeUsages)
            .map(usage -> toAction(usage).nodeOperation(new SpatialStatsAction.NodeRequest(request), null))
            .collect(Collectors.toList());
        SpatialStatsAction.Response response = new SpatialStatsAction.Response(new ClusterName("cluster_name"), nodeResponses, emptyList());
        try (XContentBuilder builder = jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        }
    }
}
