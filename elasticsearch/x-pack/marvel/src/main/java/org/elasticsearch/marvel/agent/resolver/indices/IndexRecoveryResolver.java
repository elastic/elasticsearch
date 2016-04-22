/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexRecoveryResolver extends MonitoringIndexNameResolver.Timestamped<IndexRecoveryMonitoringDoc> {

    public static final String TYPE = "index_recovery";

    public IndexRecoveryResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public String type(IndexRecoveryMonitoringDoc document) {
        return TYPE;
    }

    @Override
    protected void buildXContent(IndexRecoveryMonitoringDoc document,
                                 XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEX_RECOVERY);

        RecoveryResponse recovery = document.getRecoveryResponse();
        if (recovery != null) {
            builder.startArray(Fields.SHARDS);
            Map<String, List<RecoveryState>> shards = recovery.shardRecoveryStates();
            if (shards != null) {
                for (Map.Entry<String, List<RecoveryState>> shard : shards.entrySet()) {

                    List<RecoveryState> indexShards = shard.getValue();
                    if (indexShards != null) {
                        for (RecoveryState indexShard : indexShards) {
                            builder.startObject();
                            builder.field(Fields.INDEX_NAME, shard.getKey());
                            indexShard.toXContent(builder, params);
                            builder.endObject();
                        }
                    }
                }
            }
            builder.endArray();
        }

        builder.endObject();
    }

    static final class Fields {
        static final String INDEX_RECOVERY = TYPE;
        static final String SHARDS = "shards";
        static final String INDEX_NAME = "index_name";
    }
}
