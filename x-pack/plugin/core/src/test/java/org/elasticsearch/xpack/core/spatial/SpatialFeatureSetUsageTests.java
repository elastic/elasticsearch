/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.spatial;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.common.stats.EnumCounters;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;

import java.net.InetAddress;
import java.util.List;

import static java.util.Collections.emptyList;

public class SpatialFeatureSetUsageTests extends AbstractWireSerializingTestCase<SpatialFeatureSetUsage> {

    @Override
    protected SpatialFeatureSetUsage createTestInstance() {
        SpatialStatsAction.Response statsResponse = randomStatsResponse();
        return new SpatialFeatureSetUsage(statsResponse);
    }

    @Override
    protected SpatialFeatureSetUsage mutateInstance(SpatialFeatureSetUsage instance) {
        return null; // no mutations
    }

    @Override
    protected Writeable.Reader<SpatialFeatureSetUsage> instanceReader() {
        return SpatialFeatureSetUsage::new;
    }

    private SpatialStatsAction.Response randomStatsResponse() {
        DiscoveryNode node = DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300));
        EnumCounters<SpatialStatsAction.Item> counters = new EnumCounters<>(SpatialStatsAction.Item.class);
        SpatialStatsAction.NodeResponse nodeResponse = new SpatialStatsAction.NodeResponse(node, counters);
        return new SpatialStatsAction.Response(new ClusterName("cluster_name"), List.of(nodeResponse), emptyList());
    }
}
