/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAnalyticsStatsActionTests extends ESTestCase {

    private TransportAnalyticsStatsAction action;

    @Before
    public void setupTransportAction() {
        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = new DiscoveryNode("nodeId", buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(discoveryNode);

        ClusterName clusterName = new ClusterName("cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterName);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetaData()).thenReturn(MetaData.EMPTY_META_DATA);
        when(clusterService.state()).thenReturn(clusterState);


        action = new TransportAnalyticsStatsAction(transportService, clusterService, threadPool, new
            ActionFilters(Collections.emptySet()));
    }

    public void testCumulativeCardStats() throws Exception {
        AnalyticsStatsAction.Request request = new AnalyticsStatsAction.Request();
        AnalyticsStatsAction.NodeResponse nodeResponse1 = action.nodeOperation(new AnalyticsStatsAction.NodeRequest(request), null);
        AnalyticsStatsAction.NodeResponse nodeResponse2 = action.nodeOperation(new AnalyticsStatsAction.NodeRequest(request), null);

        AnalyticsStatsAction.Response response = action.newResponse(request,
            Arrays.asList(nodeResponse1, nodeResponse2), Collections.emptyList());

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            ObjectPath objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            assertThat(objectPath.evaluate("stats.0.cumulative_cardinality_usage"), equalTo(0));
            assertThat(objectPath.evaluate("stats.1.cumulative_cardinality_usage"), equalTo(0));
        }
    }
}
