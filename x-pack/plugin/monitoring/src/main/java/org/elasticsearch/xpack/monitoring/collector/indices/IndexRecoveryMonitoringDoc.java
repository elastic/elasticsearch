/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Monitoring document collected by {@link IndexRecoveryCollector}
 */
public class IndexRecoveryMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "index_recovery";

    private final RecoveryResponse recoveryResponse;

    public IndexRecoveryMonitoringDoc(final String cluster,
                                      final long timestamp,
                                      final long intervalMillis,
                                      final MonitoringDoc.Node node,
                                      final RecoveryResponse recoveryResponse) {

        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.recoveryResponse = Objects.requireNonNull(recoveryResponse);
    }

    RecoveryResponse getRecoveryResponse() {
        return recoveryResponse;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        {
            if (recoveryResponse != null) {
                builder.startArray("shards");
                {
                    Map<String, List<RecoveryState>> shards = recoveryResponse.shardRecoveryStates();
                    if (shards != null) {
                        for (Map.Entry<String, List<RecoveryState>> shard : shards.entrySet()) {
                            List<RecoveryState> indexShards = shard.getValue();
                            if (indexShards != null) {
                                for (RecoveryState indexShard : indexShards) {
                                    builder.startObject();
                                    {
                                        builder.field("index_name", shard.getKey());
                                        indexShard.toXContent(builder, params);
                                    }
                                    builder.endObject();
                                }
                            }
                        }
                    }
                }
                builder.endArray();
            }
        }
        builder.endObject();
    }
}
