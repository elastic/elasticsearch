/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.plugin.EqlStatsAction;
import org.elasticsearch.xpack.eql.plugin.EqlStatsResponse;
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

public class EqlInfoTransportActionTests extends ESTestCase {

    private Client client;

    @Before
    public void init() throws Exception {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testAvailable() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        EqlInfoTransportAction featureSet = new EqlInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabled() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        EqlInfoTransportAction featureSet = new EqlInfoTransportAction(transportService, mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        doAnswer(mock -> {
            ActionListener<EqlStatsResponse> listener = (ActionListener<EqlStatsResponse>) mock.getArguments()[2];

            List<EqlStatsResponse.NodeStatsResponse> nodes = new ArrayList<>();
            DiscoveryNode first = DiscoveryNodeUtils.create("first");
            EqlStatsResponse.NodeStatsResponse firstNode = new EqlStatsResponse.NodeStatsResponse(first);
            Counters firstCounters = new Counters();
            firstCounters.inc("foo.foo", 1);
            firstCounters.inc("foo.bar.baz", 1);
            firstNode.setStats(firstCounters);
            nodes.add(firstNode);

            DiscoveryNode second = DiscoveryNodeUtils.create("second");
            EqlStatsResponse.NodeStatsResponse secondNode = new EqlStatsResponse.NodeStatsResponse(second);
            Counters secondCounters = new Counters();
            secondCounters.inc("spam", 1);
            secondCounters.inc("foo.bar.baz", 4);
            secondNode.setStats(secondCounters);
            nodes.add(secondNode);

            listener.onResponse(new EqlStatsResponse(new ClusterName("whatever"), nodes, Collections.emptyList()));
            return null;
        }).when(client).execute(eq(EqlStatsAction.INSTANCE), any(), any());
        ClusterService clusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(clusterService.localNode()).thenReturn(mockNode);

        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        var usageAction = new EqlUsageTransportAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            null,
            client
        );
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(mock(Task.class), null, null, future);
        EqlFeatureSetUsage eqlUsage = (EqlFeatureSetUsage) future.get().getUsage();

        long fooBarBaz = ObjectPath.eval("foo.bar.baz", eqlUsage.stats());
        long fooFoo = ObjectPath.eval("foo.foo", eqlUsage.stats());
        long spam = ObjectPath.eval("spam", eqlUsage.stats());

        assertThat(eqlUsage.stats().keySet(), containsInAnyOrder("foo", "spam"));
        assertThat(fooBarBaz, is(5L));
        assertThat(fooFoo, is(1L));
        assertThat(spam, is(1L));
    }
}
