/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.monitor.exporter;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.component.LifecycleComponent;

import java.util.Map;

public interface StatsExporter<T> extends LifecycleComponent<T> {

    String name();

    void exportNodeStats(NodeStats nodeStats);

    void exportShardStats(ShardStats[] shardStatsArray);

    void exportIndicesStats(IndicesStatsResponse indicesStats);

}
