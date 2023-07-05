/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;

import java.io.IOException;

public record RollupShardTaskParams(
    DownsampleConfig downsampleConfig,
    String rollupIndex,
    long indexStartTimeMillis,
    long indexEndTimeMillis,
    ShardId shardId,
    String[] metrics,
    String[] labels
) implements PersistentTaskParams {

    public static final String NAME = RollupShardTask.TASK_NAME;
    private static final ParseField DOWNSAMPLE_CONFIG = new ParseField("downsample_config");
    private static final ParseField ROLLUP_INDEX = new ParseField("rollup_index");
    private static final ParseField INDEX_START_TIME_MILLIS = new ParseField("index_start_time_millis");
    private static final ParseField INDEX_END_TIME_MILLIS = new ParseField("index_end_time_millis");
    private static final ParseField SHARD_ID = new ParseField("shard_id");
    private static final ParseField METRICS = new ParseField("metrics");
    private static final ParseField LABELS = new ParseField("labels");
    private static final ConstructingObjectParser<RollupShardTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        (args) -> new RollupShardTaskParams(
            (DownsampleConfig) args[0],
            (String) args[1],
            (Long) args[2],
            (Long) args[3],
            (ShardId) args[4],
            (String[]) args[5],
            (String[]) args[6]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DownsampleConfig.fromXContent(p), DOWNSAMPLE_CONFIG);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ROLLUP_INDEX);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_START_TIME_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_END_TIME_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ShardId.fromString(p.text()), SHARD_ID);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), METRICS);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LABELS);
    }

    RollupShardTaskParams(final StreamInput in) throws IOException {
        this(
            new DownsampleConfig(in),
            in.readString(),
            in.readVLong(),
            in.readVLong(),
            new ShardId(in),
            in.readStringArray(),
            in.readStringArray()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DOWNSAMPLE_CONFIG.getPreferredName(), downsampleConfig);
        builder.field(ROLLUP_INDEX.getPreferredName(), rollupIndex);
        builder.field(INDEX_START_TIME_MILLIS.getPreferredName(), indexStartTimeMillis);
        builder.field(INDEX_END_TIME_MILLIS.getPreferredName(), indexEndTimeMillis);
        builder.field(SHARD_ID.getPreferredName(), shardId);
        builder.array(METRICS.getPreferredName(), metrics);
        builder.array(LABELS.getPreferredName(), labels);
        return builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.MINIMUM_COMPATIBLE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        downsampleConfig.writeTo(out);
        out.writeString(rollupIndex);
        out.writeVLong(indexStartTimeMillis);
        out.writeLong(indexEndTimeMillis);
        shardId.writeTo(out);
        out.writeStringArray(metrics);
        out.writeStringArray(labels);
    }
}
