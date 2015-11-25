/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterStatsMarvelDoc extends MarvelDoc {

    private final ClusterStatsResponse clusterStats;

    public ClusterStatsMarvelDoc(String clusterUUID, String type, long timestamp, ClusterStatsResponse clusterStats) {
        super(clusterUUID, type, timestamp);
        this.clusterStats = clusterStats;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }
}
