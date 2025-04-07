/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.esql.EsqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsAction;
import org.elasticsearch.xpack.esql.plugin.EsqlStatsResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlInfoTransportActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private TransportService transportService;
    private Client client;

    @Before
    public void init() {
        threadPool = new TestThreadPool(getTestName());
        transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
    }

    @After
    public void shutdown() {
        threadPool.shutdown();
    }

    public void testAvailable() {
        EsqlInfoTransportAction featureSet = new EsqlInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabled() {
        EsqlInfoTransportAction featureSet = new EsqlInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        doAnswer(mock -> {
            ActionListener<EsqlStatsResponse> listener = (ActionListener<EsqlStatsResponse>) mock.getArguments()[2];

            List<EsqlStatsResponse.NodeStatsResponse> nodes = new ArrayList<>();
            DiscoveryNode first = DiscoveryNodeUtils.create("first");
            EsqlStatsResponse.NodeStatsResponse firstNode = new EsqlStatsResponse.NodeStatsResponse(first);
            Counters firstCounters = new Counters();
            firstCounters.inc("foo.foo", 1);
            firstCounters.inc("foo.bar.baz", 1);
            firstNode.setStats(firstCounters);
            nodes.add(firstNode);

            DiscoveryNode second = DiscoveryNodeUtils.create("second");
            EsqlStatsResponse.NodeStatsResponse secondNode = new EsqlStatsResponse.NodeStatsResponse(second);
            Counters secondCounters = new Counters();
            secondCounters.inc("spam", 1);
            secondCounters.inc("foo.bar.baz", 4);
            secondNode.setStats(secondCounters);
            nodes.add(secondNode);

            listener.onResponse(new EsqlStatsResponse(new ClusterName("whatever"), nodes, Collections.emptyList()));
            return null;
        }).when(client).execute(eq(EsqlStatsAction.INSTANCE), any(), any());
        ClusterService clusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(clusterService.localNode()).thenReturn(mockNode);

        var usageAction = new EsqlUsageTransportAction(transportService, clusterService, threadPool, mock(ActionFilters.class), client);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(mock(Task.class), null, null, future);
        EsqlFeatureSetUsage esqlUsage = (EsqlFeatureSetUsage) future.get().getUsage();

        long fooBarBaz = ObjectPath.eval("foo.bar.baz", esqlUsage.stats());
        long fooFoo = ObjectPath.eval("foo.foo", esqlUsage.stats());
        long spam = ObjectPath.eval("spam", esqlUsage.stats());

        assertThat(esqlUsage.stats().keySet(), containsInAnyOrder("foo", "spam"));
        assertThat(fooBarBaz, is(5L));
        assertThat(fooFoo, is(1L));
        assertThat(spam, is(1L));
    }
}
