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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.analytics.AnalyticsUsage;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAnalyticsStatsActionTests extends ESTestCase {
    public TransportAnalyticsStatsAction action(AnalyticsUsage usage) {
        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);

        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = new DiscoveryNode("nodeId", buildNewFakeTransportAddress(), Version.CURRENT);
        when(clusterService.localNode()).thenReturn(discoveryNode);
        ClusterName clusterName = new ClusterName("cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterName);


        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterService.state()).thenReturn(clusterState);

        return new TransportAnalyticsStatsAction(transportService, clusterService, threadPool,
                new ActionFilters(Collections.emptySet()), usage);
    }

    public void test() throws IOException {
        for (AnalyticsStatsAction.Item item : AnalyticsStatsAction.Item.values()) {
            AnalyticsUsage realUsage = new AnalyticsUsage();
            AnalyticsUsage emptyUsage = new AnalyticsUsage();
            ContextParser<Void, Void> parser = realUsage.track(item, (p, c) -> c);
            ObjectPath unused = run(realUsage, emptyUsage);
            assertThat(unused.evaluate("stats." + item.name().toLowerCase(Locale.ROOT) + "_usage"), equalTo(0));
            int count = between(1, 10000);
            for (int i = 0; i < count; i++) {
                assertNull(parser.parse(null, null));
            }
            ObjectPath used = run(realUsage, emptyUsage);
            assertThat(item.name(), used.evaluate("stats." + item.name().toLowerCase(Locale.ROOT) + "_usage"), equalTo(count));
        }
    }

    private ObjectPath run(AnalyticsUsage... nodeUsages) throws IOException {
        AnalyticsStatsAction.Request request = new AnalyticsStatsAction.Request();
        List<AnalyticsStatsAction.NodeResponse> nodeResponses = Arrays.stream(nodeUsages)
                .map(usage -> action(usage).nodeOperation(new AnalyticsStatsAction.NodeRequest(request), null))
                .collect(toList());
        AnalyticsStatsAction.Response response = new AnalyticsStatsAction.Response(
                new ClusterName("cluster_name"), nodeResponses, emptyList());

        AnalyticsFeatureSetUsage usage = new AnalyticsFeatureSetUsage(true, true, response);
        try (XContentBuilder builder = jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        }
    }
}
