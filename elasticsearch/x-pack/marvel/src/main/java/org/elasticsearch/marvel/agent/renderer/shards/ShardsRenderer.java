/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.shards.ShardMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;

public class ShardsRenderer extends AbstractRenderer<ShardMarvelDoc> {

    public static final String[] FILTERS = {
            "state_uuid",
            "shard.state",
            "shard.primary",
            "shard.node",
            "shard.relocating_node",
            "shard.shard",
            "shard.index",
    };

    public ShardsRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(ShardMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(Fields.STATE_UUID, marvelDoc.getClusterStateUUID());

        ShardRouting shardRouting = marvelDoc.getShardRouting();
        if (shardRouting != null) {
            // ShardRouting is rendered inside a startObject() / endObject() but without a name,
            // so we must use XContentBuilder.field(String, ToXContent, ToXContent.Params) here
            builder.field(Fields.SHARD.underscore().toString(), shardRouting, params);
        }
    }

    static final class Fields {
        static final XContentBuilderString SHARD = new XContentBuilderString("shard");
        static final XContentBuilderString STATE_UUID = new XContentBuilderString("state_uuid");
    }
}
