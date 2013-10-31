/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.monitor.annotation;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardEventAnnotation extends Annotation {

    private final ShardRouting shardRouting;
    private final ShardId shardId;
    private EventType event;

    public enum EventType {
        CREATED,
        STARTED,
        CLOSED
    }


    public ShardEventAnnotation(long timestamp, EventType event, ShardId shardId, ShardRouting shardRouting) {
        super(timestamp);
        this.event = event;
        this.shardId = shardId;
        this.shardRouting = shardRouting;
    }

    @Override
    public String type() {
        return "shard_event";
    }

    @Override
    String conciseDescription() {
        return "[" + event + "]" + (shardRouting != null ? shardRouting : shardId);
    }

    @Override
    public XContentBuilder addXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.addXContentBody(builder, params);
        builder.field("event", event);
        builder.field("index", shardId.index());
        builder.field("shard_id", shardId.id());
        if (shardRouting != null) {
            builder.field("routing");
            shardRouting.toXContent(builder, params);
        }

        return builder;
    }
}
