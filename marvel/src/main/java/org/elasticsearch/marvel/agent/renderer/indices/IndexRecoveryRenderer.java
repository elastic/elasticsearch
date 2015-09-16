/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexRecoveryRenderer extends AbstractRenderer<IndexRecoveryMarvelDoc> {

    public IndexRecoveryRenderer() {
        super(null, false);
    }

    @Override
    protected void doRender(IndexRecoveryMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEX_RECOVERY);

        RecoveryResponse recovery = marvelDoc.getRecoveryResponse();
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
        static final XContentBuilderString INDEX_RECOVERY = new XContentBuilderString("index_recovery");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString INDEX_NAME = new XContentBuilderString("index_name");
    }
}
