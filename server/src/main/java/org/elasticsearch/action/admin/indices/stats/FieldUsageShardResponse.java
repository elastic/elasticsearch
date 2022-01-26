/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FieldUsageShardResponse implements Writeable, ToXContentObject {

    final String trackingId;
    final ShardRouting shardRouting;
    final long trackingStartTime;
    final FieldUsageStats stats;

    FieldUsageShardResponse(StreamInput in) throws IOException {
        trackingId = in.readString();
        shardRouting = new ShardRouting(in);
        trackingStartTime = in.readVLong();
        stats = new FieldUsageStats(in);
    }

    FieldUsageShardResponse(String trackingId, ShardRouting shardRouting, long trackingStartTime, FieldUsageStats stats) {
        this.trackingId = Objects.requireNonNull(trackingId, "trackingId must be non null");
        this.shardRouting = Objects.requireNonNull(shardRouting, "routing must be non null");
        this.trackingStartTime = trackingStartTime;
        this.stats = Objects.requireNonNull(stats, "stats must be non null");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(trackingId);
        shardRouting.writeTo(out);
        out.writeVLong(trackingStartTime);
        stats.writeTo(out);
    }

    public String getTrackingId() {
        return trackingId;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public long getTrackingStartTime() {
        return trackingStartTime;
    }

    public FieldUsageStats getStats() {
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.TRACKING_ID, trackingId);
        builder.timeField(Fields.TRACKING_STARTED_AT_MILLIS, Fields.TRACKING_STARTED_AT, trackingStartTime);
        builder.startObject(Fields.ROUTING)
            .field(Fields.STATE, shardRouting.state())
            .field(Fields.PRIMARY, shardRouting.primary())
            .field(Fields.NODE, shardRouting.currentNodeId())
            .field(Fields.RELOCATING_NODE, shardRouting.relocatingNodeId())
            .endObject();
        builder.field(Fields.STATS, stats, params);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String TRACKING_ID = "tracking_id";
        static final String TRACKING_STARTED_AT_MILLIS = "tracking_started_at_millis";
        static final String TRACKING_STARTED_AT = "tracking_started_at";
        static final String STATS = "stats";
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";
    }
}
