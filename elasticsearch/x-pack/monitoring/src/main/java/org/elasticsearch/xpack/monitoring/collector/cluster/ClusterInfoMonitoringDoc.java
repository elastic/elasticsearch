/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.List;

public class ClusterInfoMonitoringDoc extends MonitoringDoc {

    private String clusterName;
    private String version;
    private License license;
    private List<XPackFeatureSet.Usage> usage;
    private ClusterStatsResponse clusterStats;

    public ClusterInfoMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
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

    public List<XPackFeatureSet.Usage> getUsage() {
        return usage;
    }

    public void setUsage(List<XPackFeatureSet.Usage> usage) {
        this.usage = usage;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }

    public void setClusterStats(ClusterStatsResponse clusterStats) {
        this.clusterStats = clusterStats;
    }
}
