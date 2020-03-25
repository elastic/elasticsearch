/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.CoordinatorStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;

public final class EnrichCoordinatorDoc extends MonitoringDoc {

    public static final String TYPE = "enrich_coordinator_stats";

    private final CoordinatorStats coordinatorStats;

    public EnrichCoordinatorDoc(String cluster,
                                long timestamp,
                                long intervalMillis,
                                MonitoringDoc.Node node,
                                CoordinatorStats coordinatorStats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.coordinatorStats = Objects.requireNonNull(coordinatorStats, "stats");
    }

    public CoordinatorStats getCoordinatorStats() {
        return coordinatorStats;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        {
            coordinatorStats.toXContent(builder, params);
        }
        builder.endObject();
    }
}
