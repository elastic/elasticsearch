/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

public class IndexStatsMonitoringDoc extends MonitoringDoc {

    private IndexStats indexStats;

    public IndexStatsMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public IndexStats getIndexStats() {
        return indexStats;
    }

    public void setIndexStats(IndexStats indexStats) {
        this.indexStats = indexStats;
    }
}
