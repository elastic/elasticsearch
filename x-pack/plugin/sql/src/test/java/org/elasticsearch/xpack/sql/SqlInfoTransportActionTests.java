/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.sql.SqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.sql.plugin.SqlStatsAction;
import org.elasticsearch.xpack.sql.plugin.SqlStatsResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlInfoTransportActionTests extends ESTestCase {

    private XPackLicenseState licenseState;
    private Client client;

    @Before
    public void init() throws Exception {
        licenseState = mock(XPackLicenseState.class);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);
    }

    public void testAvailable() {
        SqlInfoTransportAction featureSet = new SqlInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.SQL)).thenReturn(available);
        assertThat(featureSet.available(), is(available));
    }

    @SuppressWarnings("unchecked")
    public void testUsageStats() throws Exception {
        doAnswer(mock -> {
            ActionListener<SqlStatsResponse> listener =
                    (ActionListener<SqlStatsResponse>) mock.getArguments()[2];

            List<SqlStatsResponse.NodeStatsResponse> nodes = new ArrayList<>();
            DiscoveryNode first = new DiscoveryNode("first", buildNewFakeTransportAddress(), Version.CURRENT);
            SqlStatsResponse.NodeStatsResponse firstNode = new SqlStatsResponse.NodeStatsResponse(first);
            Counters firstCounters = new Counters();
            firstCounters.inc("foo.foo", 1);
            firstCounters.inc("foo.bar.baz", 1);
            firstNode.setStats(firstCounters);
            nodes.add(firstNode);

            DiscoveryNode second = new DiscoveryNode("second", buildNewFakeTransportAddress(), Version.CURRENT);
            SqlStatsResponse.NodeStatsResponse secondNode = new SqlStatsResponse.NodeStatsResponse(second);
            Counters secondCounters = new Counters();
            secondCounters.inc("spam", 1);
            secondCounters.inc("foo.bar.baz", 4);
            secondNode.setStats(secondCounters);
            nodes.add(secondNode);

            listener.onResponse(new SqlStatsResponse(new ClusterName("whatever"), nodes, Collections.emptyList()));
            return null;
        }).when(client).execute(eq(SqlStatsAction.INSTANCE), any(), any());
        ClusterService clusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("mocknode");
        when(clusterService.localNode()).thenReturn(mockNode);

        var usageAction = new SqlUsageTransportAction(mock(TransportService.class), clusterService, null,
            mock(ActionFilters.class), null, licenseState, client);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(mock(Task.class), null, null, future);
        SqlFeatureSetUsage sqlUsage = (SqlFeatureSetUsage) future.get().getUsage();

        long fooBarBaz = ObjectPath.eval("foo.bar.baz", sqlUsage.stats());
        long fooFoo = ObjectPath.eval("foo.foo", sqlUsage.stats());
        long spam = ObjectPath.eval("spam", sqlUsage.stats());

        assertThat(sqlUsage.stats().keySet(), containsInAnyOrder("foo", "spam"));
        assertThat(fooBarBaz, is(5L));
        assertThat(fooFoo, is(1L));
        assertThat(spam, is(1L));
    }
}
