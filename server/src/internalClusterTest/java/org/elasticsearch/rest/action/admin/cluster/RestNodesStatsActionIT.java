/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.TransportGetAllocationStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestNodesStatsActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testSendOnlyNecessaryElectedMasterNodeStatsRequest() {
        var node = internalCluster().startDataOnlyNode();

        var getAllocationStatsActions = new AtomicInteger(0);
        MockTransportService.getInstance(node).addSendBehavior((connection, requestId, action, request, options) -> {
            if (Objects.equals(action, TransportGetAllocationStatsAction.TYPE.name())) {
                getAllocationStatsActions.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var metrics = randomSubsetOf(Metric.values().length, Metric.values());
        client(node).admin().cluster().nodesStats(new NodesStatsRequest().addMetrics(metrics)).actionGet();

        var shouldSendGetAllocationStatsRequest = metrics.contains(Metric.ALLOCATIONS) || metrics.contains(Metric.FS);
        assertThat(getAllocationStatsActions.get(), equalTo(shouldSendGetAllocationStatsRequest ? 1 : 0));
    }

    public void testGetAllocationStatsRequestIsScopedToTargetedNodes() {
        var node = internalCluster().startDataOnlyNode();
        var localNodeId = client(node).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getLocalNodeId();

        var captured = new AtomicReference<TransportGetAllocationStatsAction.Request>();
        MockTransportService.getInstance(node).addSendBehavior((connection, requestId, action, request, options) -> {
            if (Objects.equals(action, TransportGetAllocationStatsAction.TYPE.name())) {
                captured.set((TransportGetAllocationStatsAction.Request) MasterNodeRequestHelper.unwrapTermOverride(request));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Request without any node selector — coordinator passes null (= all nodes) to keep the cache fast path on the master.
        client(node).admin().cluster().nodesStats(new NodesStatsRequest().addMetric(Metric.ALLOCATIONS)).actionGet();
        assertThat(captured.get().nodeIds(), nullValue());

        // Request scoped to the local node — coordinator resolves _local and ships only that node ID to the master.
        captured.set(null);
        client(node).admin().cluster().nodesStats(new NodesStatsRequest("_local").addMetric(Metric.ALLOCATIONS)).actionGet();
        assertThat(captured.get().nodeIds(), equalTo(Set.of(localNodeId)));
    }
}
