/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.enrich;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction.Response.ExecutingPolicy;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;

public final class ExecutingPolicyDoc extends MonitoringDoc {

    public static final String TYPE = "enrich_executing_policy_stats";

    private final ExecutingPolicy executingPolicy;

    public ExecutingPolicyDoc(String cluster, long timestamp, long intervalMillis, Node node, ExecutingPolicy coordinatorStats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.executingPolicy = Objects.requireNonNull(coordinatorStats, "stats");
    }

    public ExecutingPolicy getExecutingPolicy() {
        return executingPolicy;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        {
            executingPolicy.toXContent(builder, params);
        }
        builder.endObject();
    }
}
