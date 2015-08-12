/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterStatsMarvelDoc extends MarvelDoc<ClusterStatsMarvelDoc.Payload> {

    private final Payload payload;

    public ClusterStatsMarvelDoc(String clusterName, String type, long timestamp, Payload payload) {
        super(clusterName, type, timestamp);
        this.payload = payload;
    }

    @Override
    public ClusterStatsMarvelDoc.Payload payload() {
        return payload;
    }

    public static ClusterStatsMarvelDoc createMarvelDoc(String clusterName, String type, long timestamp, ClusterStatsResponse clusterStats) {
        return new ClusterStatsMarvelDoc(clusterName, type, timestamp, new Payload(clusterStats));
    }

    public static class Payload {

        private final ClusterStatsResponse clusterStats;

        Payload(ClusterStatsResponse clusterStats) {
            this.clusterStats = clusterStats;
        }

        public ClusterStatsResponse getClusterStats() {
            return clusterStats;
        }
    }
}
