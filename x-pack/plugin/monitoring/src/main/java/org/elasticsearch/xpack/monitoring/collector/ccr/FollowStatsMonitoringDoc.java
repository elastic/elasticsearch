/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.collector.ccr;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;

public class FollowStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "ccr_stats";

    private final ShardFollowNodeTaskStatus status;

    public ShardFollowNodeTaskStatus status() {
        return status;
    }

    public FollowStatsMonitoringDoc(
            final String cluster,
            final long timestamp,
            final long intervalMillis,
            final MonitoringDoc.Node node,
            final ShardFollowNodeTaskStatus status) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.status = Objects.requireNonNull(status, "status");
    }


    @Override
    protected void innerToXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(TYPE);
        {
            status.toXContentFragment(builder, params);
        }
        builder.endObject();
    }

}
