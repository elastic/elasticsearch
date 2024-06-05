/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                final String clusterUuid = clusterUuid(clusterState);
                final String stateUUID = clusterState.stateUUID();
                final long timestamp = timestamp();

                final String[] indicesToMonitor = getCollectionIndices();
                final boolean isAllIndices = IndexNameExpressionResolver.isAllIndices(Arrays.asList(indicesToMonitor));
                final String[] indices = isAllIndices
                    ? routingTable.indicesRouting().keySet().toArray(new String[0])
                    : expandIndexPattern(indicesToMonitor, routingTable.indicesRouting().keySet().toArray(new String[0]));

                for (String index : indices) {
                    IndexRoutingTable indexRoutingTable = routingTable.index(index);
                    if (indexRoutingTable != null) {
                        final int shardCount = indexRoutingTable.size();
                        for (int i = 0; i < shardCount; i++) {
                            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);

                            ShardRouting primary = shardRoutingTable.primaryShard();
                            MonitoringDoc.Node primaryShardNode = null;
                            if (primary.assignedToNode()) {
                                // If the shard is assigned to a node, the shard monitoring document refers to this node
                                primaryShardNode = convertNode(node.getTimestamp(), clusterState.getNodes().get(primary.currentNodeId()));
                            }
                            results.add(new ShardMonitoringDoc(clusterUuid, timestamp, interval, primaryShardNode, primary, stateUUID, 0));

                            List<ShardRouting> replicas = shardRoutingTable.replicaShards();
                            for (int j = 0; j < replicas.size(); j++) {
                                ShardRouting replica = replicas.get(j);

                                MonitoringDoc.Node replicaShardNode = null;
                                if (replica.assignedToNode()) {
                                    replicaShardNode = convertNode(
                                        node.getTimestamp(),
                                        clusterState.getNodes().get(replica.currentNodeId())
                                    );
                                }
                                results.add(
                                    new ShardMonitoringDoc(clusterUuid, timestamp, interval, replicaShardNode, replica, stateUUID, j + 1)
                                );
                            }
                        }
                    }
                }
            }
        }
        return Collections.unmodifiableCollection(results);
    }

    private static String[] expandIndexPattern(String[] indicesToMonitor, String[] indices) {
        final Set<String> expandedIndices = new HashSet<>();

        for (String indexOrPattern : indicesToMonitor) {
            if (indexOrPattern.contains("*")) {
                for (String index : indices) {
                    if (Regex.simpleMatch(indexOrPattern, index)) {
                        expandedIndices.add(index);
                    }
                }
            } else {
                expandedIndices.add(indexOrPattern);
            }
        }

        return expandedIndices.toArray(new String[0]);
    }
}
