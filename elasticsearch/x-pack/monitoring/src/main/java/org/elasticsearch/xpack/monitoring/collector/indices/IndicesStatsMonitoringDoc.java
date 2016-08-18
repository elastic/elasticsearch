/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

public class IndicesStatsMonitoringDoc extends MonitoringDoc {

    private IndicesStatsResponse indicesStats;

    public IndicesStatsMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public IndicesStatsResponse getIndicesStats() {
        return indicesStats;
    }

    public void setIndicesStats(IndicesStatsResponse indicesStats) {
        this.indicesStats = indicesStats;
    }
}
