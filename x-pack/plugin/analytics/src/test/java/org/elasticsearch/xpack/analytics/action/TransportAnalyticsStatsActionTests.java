/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.action;

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
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.analytics.AnalyticsUsage;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAnalyticsStatsActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("TransportAnalyticsStatsActionTests");
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public TransportAnalyticsStatsAction action(AnalyticsUsage usage) {
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("nodeId");
        when(clusterService.localNode()).thenReturn(discoveryNode);
        ClusterName clusterName = new ClusterName("cluster_name");
        when(clusterService.getClusterName()).thenReturn(clusterName);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterService.state()).thenReturn(clusterState);

        return new TransportAnalyticsStatsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            usage
        );
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
            .map(usage -> action(usage).nodeOperation(new AnalyticsStatsAction.NodeRequest(), null))
            .collect(toList());
        AnalyticsStatsAction.Response response = new AnalyticsStatsAction.Response(
            new ClusterName("cluster_name"),
            nodeResponses,
            emptyList()
        );

        AnalyticsFeatureSetUsage usage = new AnalyticsFeatureSetUsage(true, true, response);
        try (XContentBuilder builder = jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        }
    }
}
