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

    private String clusterName;
    private String version;
    private License license;
    private ClusterStatsResponse clusterStats;

    public ClusterInfoMarvelDoc(String index, String type, String id) {
        super(index, type, id);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public License getLicense() {
        return license;
    }

    public void setLicense(License license) {
        this.license = license;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }

    public void setClusterStats(ClusterStatsResponse clusterStats) {
        this.clusterStats = clusterStats;
    }
}
