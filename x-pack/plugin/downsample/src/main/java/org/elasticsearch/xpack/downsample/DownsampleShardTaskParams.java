/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public record DownsampleShardTaskParams(
    DownsampleConfig downsampleConfig,
    String downsampleIndex,
    long indexStartTimeMillis,
    long indexEndTimeMillis,
    ShardId shardId,
    String[] metrics,
    String[] labels
) implements PersistentTaskParams {

    public static final String NAME = DownsampleShardTask.TASK_NAME;
    private static final ParseField DOWNSAMPLE_CONFIG = new ParseField("downsample_config");
    private static final ParseField DOWNSAMPLE_INDEX = new ParseField("rollup_index");
    private static final ParseField INDEX_START_TIME_MILLIS = new ParseField("index_start_time_millis");
    private static final ParseField INDEX_END_TIME_MILLIS = new ParseField("index_end_time_millis");
    private static final ParseField SHARD_ID = new ParseField("shard_id");
    private static final ParseField METRICS = new ParseField("metrics");
    private static final ParseField LABELS = new ParseField("labels");
    public static final ObjectParser<DownsampleShardTaskParams.Builder, Void> PARSER = new ObjectParser<>(NAME);

    static {
        PARSER.declareObject(
            DownsampleShardTaskParams.Builder::downsampleConfig,
            (p, c) -> DownsampleConfig.fromXContent(p),
            DOWNSAMPLE_CONFIG
        );
        PARSER.declareString(DownsampleShardTaskParams.Builder::downsampleIndex, DOWNSAMPLE_INDEX);
        PARSER.declareLong(DownsampleShardTaskParams.Builder::indexStartTimeMillis, INDEX_START_TIME_MILLIS);
        PARSER.declareLong(DownsampleShardTaskParams.Builder::indexEndTimeMillis, INDEX_END_TIME_MILLIS);
        PARSER.declareString(DownsampleShardTaskParams.Builder::shardId, SHARD_ID);
        PARSER.declareStringArray(DownsampleShardTaskParams.Builder::metrics, METRICS);
        PARSER.declareStringArray(DownsampleShardTaskParams.Builder::labels, LABELS);
    }

    DownsampleShardTaskParams(final StreamInput in) throws IOException {
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
        builder.field(DOWNSAMPLE_INDEX.getPreferredName(), downsampleIndex);
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
        return TransportVersions.V_8_500_054;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        downsampleConfig.writeTo(out);
        out.writeString(downsampleIndex);
        out.writeVLong(indexStartTimeMillis);
        out.writeVLong(indexEndTimeMillis);
        shardId.writeTo(out);
        out.writeStringArray(metrics);
        out.writeStringArray(labels);
    }

    public static DownsampleShardTaskParams fromXContent(XContentParser parser) throws IOException {
        final DownsampleShardTaskParams.Builder builder = new DownsampleShardTaskParams.Builder();
        PARSER.parse(parser, builder, null);
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DownsampleShardTaskParams that = (DownsampleShardTaskParams) o;
        return indexStartTimeMillis == that.indexStartTimeMillis
            && indexEndTimeMillis == that.indexEndTimeMillis
            && Objects.equals(downsampleConfig, that.downsampleConfig)
            && Objects.equals(downsampleIndex, that.downsampleIndex)
            && Objects.equals(shardId.id(), that.shardId.id())
            && Objects.equals(shardId.getIndexName(), that.shardId.getIndexName())
            && Arrays.equals(metrics, that.metrics)
            && Arrays.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            downsampleConfig,
            downsampleIndex,
            indexStartTimeMillis,
            indexEndTimeMillis,
            shardId.id(),
            shardId.getIndexName()
        );
        result = 31 * result + Arrays.hashCode(metrics);
        result = 31 * result + Arrays.hashCode(labels);
        return result;
    }

    public static class Builder {
        DownsampleConfig downsampleConfig;
        String downsampleIndex;
        long indexStartTimeMillis;
        long indexEndTimeMillis;
        ShardId shardId;
        String[] metrics;
        String[] labels;

        public Builder downsampleConfig(final DownsampleConfig downsampleConfig) {
            this.downsampleConfig = downsampleConfig;
            return this;
        }

        public Builder downsampleIndex(final String downsampleIndex) {
            this.downsampleIndex = downsampleIndex;
            return this;
        }

        public Builder indexStartTimeMillis(final Long indexStartTimeMillis) {
            this.indexStartTimeMillis = indexStartTimeMillis;
            return this;
        }

        public Builder indexEndTimeMillis(final Long indexEndTimeMillis) {
            this.indexEndTimeMillis = indexEndTimeMillis;
            return this;
        }

        public Builder shardId(final String shardId) {
            this.shardId = ShardId.fromString(shardId);
            return this;
        }

        public Builder metrics(final List<String> metrics) {
            this.metrics = metrics.toArray(String[]::new);
            return this;
        }

        public Builder labels(final List<String> labels) {
            this.labels = labels.toArray(String[]::new);
            return this;
        }

        public DownsampleShardTaskParams build() {
            return new DownsampleShardTaskParams(
                downsampleConfig,
                downsampleIndex,
                indexStartTimeMillis,
                indexEndTimeMillis,
                shardId,
                metrics,
                labels
            );
        }
    }
}
