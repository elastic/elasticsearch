/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterInfoMarvelDoc extends MarvelDoc {

    private final String clusterName;
    private final String version;
    private final License license;
    private final ClusterStatsResponse clusterStats;

    ClusterInfoMarvelDoc(String index, String type, String id, String clusterUUID, long timestamp,
                         String clusterName, String version, License license, ClusterStatsResponse clusterStats) {
        super(index, type, id, clusterUUID, timestamp);
        this.clusterName = clusterName;
        this.version = version;
        this.license = license;
        this.clusterStats = clusterStats;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getVersion() {
        return version;
    }

    public License getLicense() {
        return license;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }
}
