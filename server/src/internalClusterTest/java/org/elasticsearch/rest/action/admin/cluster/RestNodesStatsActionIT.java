/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.allocation.TransportGetAllocationStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

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
}
