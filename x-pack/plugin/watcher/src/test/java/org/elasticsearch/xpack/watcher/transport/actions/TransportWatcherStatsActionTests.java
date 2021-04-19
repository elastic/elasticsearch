/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
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
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportWatcherStatsActionTests extends ESTestCase {

    private TransportWatcherStatsAction action;

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
        when(clusterState.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterService.state()).thenReturn(clusterState);

        WatcherLifeCycleService watcherLifeCycleService = mock(WatcherLifeCycleService.class);
        when(watcherLifeCycleService.getState()).thenReturn(() -> WatcherState.STARTED);

        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.executionThreadPoolQueueSize()).thenReturn(100L);
        when(executionService.executionThreadPoolMaxSize()).thenReturn(5L);
        Counters firstExecutionCounters = new Counters();
        firstExecutionCounters.inc("spam.eggs", 1);
        Counters secondExecutionCounters = new Counters();
        secondExecutionCounters.inc("whatever", 1);
        secondExecutionCounters.inc("foo.bar.baz", 123);
        when(executionService.executionTimes()).thenReturn(firstExecutionCounters, secondExecutionCounters);

        TriggerService triggerService = mock(TriggerService.class);
        when(triggerService.count()).thenReturn(10L, 30L);
        Counters firstTriggerServiceStats = new Counters();
        firstTriggerServiceStats.inc("foo.bar.baz", 1024);
        Counters secondTriggerServiceStats = new Counters();
        secondTriggerServiceStats.inc("foo.bar.baz", 1024);
        when(triggerService.stats()).thenReturn(firstTriggerServiceStats, secondTriggerServiceStats);

        action = new TransportWatcherStatsAction(transportService, clusterService, threadPool, new
            ActionFilters(Collections.emptySet()), watcherLifeCycleService, executionService, triggerService);
    }

    public void testWatcherStats() throws Exception {
        WatcherStatsRequest request = new WatcherStatsRequest();
        request.includeStats(true);
        WatcherStatsResponse.Node nodeResponse1 = action.nodeOperation(new WatcherStatsRequest.Node(request), null);
        WatcherStatsResponse.Node nodeResponse2 = action.nodeOperation(new WatcherStatsRequest.Node(request), null);

        WatcherStatsResponse response = action.newResponse(request,
            Arrays.asList(nodeResponse1, nodeResponse2), Collections.emptyList());
        assertThat(response.getWatchesCount(), is(40L));

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            ObjectPath objectPath = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            assertThat(objectPath.evaluate("stats.0.stats.foo.bar.baz"), is(1024));
            assertThat(objectPath.evaluate("stats.1.stats.foo.bar.baz"), is(1147));
            assertThat(objectPath.evaluate("stats.0.stats.spam.eggs"), is(1));
            assertThat(objectPath.evaluate("stats.1.stats.whatever"), is(1));
        }
    }
}
