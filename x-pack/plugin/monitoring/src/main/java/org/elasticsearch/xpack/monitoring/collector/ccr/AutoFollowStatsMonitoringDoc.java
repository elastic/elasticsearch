/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;

public class AutoFollowStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "ccr_auto_follow_stats";

    private final AutoFollowStats stats;

    public AutoFollowStats stats() {
        return stats;
    }

    public AutoFollowStatsMonitoringDoc(
            final String cluster,
            final long timestamp,
            final long intervalMillis,
            final Node node,
            final AutoFollowStats stats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.stats = Objects.requireNonNull(stats, "stats");
    }


    @Override
    protected void innerToXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(TYPE);
        {
            stats.toXContentFragment(builder, params);
        }
        builder.endObject();
    }

}
