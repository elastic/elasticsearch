/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for shards.
 * <p>
 * This collector runs on the master node only and collects the {@link ShardMonitoringDoc} documents
 * for every index shard.
 */
public class ShardsCollector extends Collector {

    public ShardsCollector(final ClusterService clusterService, final XPackLicenseState licenseState) {
        super(ShardMonitoringDoc.TYPE, clusterService, null, licenseState);
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return isElectedMaster && super.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node, final long interval, final ClusterState clusterState)
        throws Exception {
        final List<MonitoringDoc> results = new ArrayList<>(1);
        if (clusterState != null) {
            RoutingTable routingTable = clusterState.routingTable();
            if (routingTable != null) {
                List<ShardRouting> shards = routingTable.allShards();
                if (shards != null) {
                    final String clusterUuid = clusterUuid(clusterState);
                    final String stateUUID = clusterState.stateUUID();
                    final long timestamp = timestamp();

                    final String[] indices = getCollectionIndices();
                    final boolean isAllIndices = IndexNameExpressionResolver.isAllIndices(Arrays.asList(indices));

                    for (ShardRouting shard : shards) {
                        if (isAllIndices || Regex.simpleMatch(indices, shard.getIndexName())) {
                            MonitoringDoc.Node shardNode = null;
                            if (shard.assignedToNode()) {
                                // If the shard is assigned to a node, the shard monitoring document refers to this node
                                shardNode = convertNode(node.getTimestamp(), clusterState.getNodes().get(shard.currentNodeId()));
                            }
                            results.add(new ShardMonitoringDoc(clusterUuid, timestamp, interval, shardNode, shard, stateUUID));
                        }
                    }
                }
            }
        }
        return Collections.unmodifiableCollection(results);
    }
}
