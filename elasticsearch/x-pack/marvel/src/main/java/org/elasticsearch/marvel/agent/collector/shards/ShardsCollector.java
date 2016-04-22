/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.shards;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;

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
public class ShardsCollector extends AbstractCollector<ShardsCollector> {

    public static final String NAME = "shards-collector";

    @Inject
    public ShardsCollector(Settings settings, ClusterService clusterService,
                           MonitoringSettings monitoringSettings, MonitoringLicensee licensee) {
        super(settings, NAME, clusterService, monitoringSettings, licensee);
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        List<MonitoringDoc> results = new ArrayList<>(1);

        ClusterState clusterState = clusterService.state();
        if (clusterState != null) {
            RoutingTable routingTable = clusterState.routingTable();
            if (routingTable != null) {
                List<ShardRouting> shards = routingTable.allShards();
                if (shards != null) {
                    String clusterUUID = clusterUUID();
                    String stateUUID = clusterState.stateUUID();
                    long timestamp = System.currentTimeMillis();

                    for (ShardRouting shard : shards) {
                        if (match(shard.getIndexName())) {
                            ShardMonitoringDoc shardDoc = new ShardMonitoringDoc(monitoringId(), monitoringVersion());
                            shardDoc.setClusterUUID(clusterUUID);
                            shardDoc.setTimestamp(timestamp);
                            if (shard.assignedToNode()) {
                                // If the shard is assigned to a node, the shard monitoring document
                                // refers to this node
                                shardDoc.setSourceNode(clusterState.getNodes().get(shard.currentNodeId()));
                            }
                            shardDoc.setShardRouting(shard);
                            shardDoc.setClusterStateUUID(stateUUID);
                            results.add(shardDoc);
                        }
                    }
                }
            }
        }

        return Collections.unmodifiableCollection(results);
    }

    private boolean match(String indexName) {
        String[] indices = monitoringSettings.indices();
        return IndexNameExpressionResolver
                .isAllIndices(Arrays.asList(monitoringSettings.indices())) || Regex.simpleMatch(indices, indexName);
    }
}
