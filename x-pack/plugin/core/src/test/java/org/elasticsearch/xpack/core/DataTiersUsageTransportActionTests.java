/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataTiersUsageTransportActionTests extends ESTestCase {

    public void testCalculateMAD() {
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(new TDigestState(10)), equalTo(0L));

        TDigestState sketch = new TDigestState(randomDoubleBetween(1, 1000, false));
        sketch.add(1);
        sketch.add(1);
        sketch.add(2);
        sketch.add(2);
        sketch.add(4);
        sketch.add(6);
        sketch.add(9);
        assertThat(DataTiersUsageTransportAction.computeMedianAbsoluteDeviation(sketch), equalTo(1L));
    }

    public void testSeparateTiers() {
        NodeStats hotStats = fakeStats(DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        NodeStats coldStats = fakeStats(DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        NodeStats warmStats = fakeStats(DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        NodeStats warmStats2 = fakeStats(DiscoveryNodeRole.DATA_WARM_NODE_ROLE);

        NodesStatsResponse nodesStats = new NodesStatsResponse(new ClusterName("cluster"),
            Arrays.asList(hotStats, coldStats, warmStats, warmStats2), Collections.emptyList());

        Map<String, List<NodeStats>> tiers = DataTiersUsageTransportAction.separateTiers(nodesStats);
        assertThat(tiers.keySet(), equalTo(DataTier.ALL_DATA_TIERS));
        assertThat(tiers.get(DataTier.DATA_CONTENT), empty());
        assertThat(tiers.get(DataTier.DATA_HOT), containsInAnyOrder(hotStats));
        assertThat(tiers.get(DataTier.DATA_WARM), containsInAnyOrder(warmStats, warmStats2));
        assertThat(tiers.get(DataTier.DATA_COLD), containsInAnyOrder(coldStats));
    }

    private static NodeStats fakeStats(DiscoveryNodeRole role) {
        NodeStats stats = mock(NodeStats.class);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getRoles()).thenReturn(Collections.singleton(role));
        when(stats.getNode()).thenReturn(node);
        return stats;
    }
}
