/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * @param downsampleShardIndexerStatus An instance of {@link DownsampleShardIndexerStatus} with the downsampleShardIndexerStatus of
 *                                     the downsample task
 * @param tsid The latest successfully processed tsid component of a tuple (tsid, timestamp)
 */
public record DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus downsampleShardIndexerStatus, BytesRef tsid)
    implements
        PersistentTaskState {

    public static final String NAME = DownsampleShardTask.TASK_NAME;
    private static final ParseField ROLLUP_SHARD_INDEXER_STATUS = new ParseField("status");
    private static final ParseField TSID = new ParseField("tsid");

    public static final ObjectParser<DownsampleShardPersistentTaskState.Builder, Void> PARSER = new ObjectParser<>(NAME);

    static {
        PARSER.declareField(
            DownsampleShardPersistentTaskState.Builder::status,
            (p, c) -> DownsampleShardIndexerStatus.valueOf(p.textOrNull()),
            ROLLUP_SHARD_INDEXER_STATUS,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            DownsampleShardPersistentTaskState.Builder::tsid,
            (p, c) -> new BytesRef(p.textOrNull()),
            TSID,
            ObjectParser.ValueType.STRING
        );
    }

    public DownsampleShardPersistentTaskState(final StreamInput in) throws IOException {
        this(DownsampleShardIndexerStatus.readFromStream(in), in.readBytesRef());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ROLLUP_SHARD_INDEXER_STATUS.getPreferredName(), downsampleShardIndexerStatus);
        if (tsid != null) {
            builder.field(TSID.getPreferredName(), tsid.utf8ToString());
        }
        return builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return DownsampleShardTask.TASK_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        downsampleShardIndexerStatus.writeTo(out);
        out.writeBytesRef(tsid);
    }

    public DownsampleShardIndexerStatus downsampleShardIndexerStatus() {
        return downsampleShardIndexerStatus;
    }

    public boolean done() {
        return DownsampleShardIndexerStatus.COMPLETED.equals(downsampleShardIndexerStatus)
            || DownsampleShardIndexerStatus.CANCELLED.equals(downsampleShardIndexerStatus)
            || DownsampleShardIndexerStatus.FAILED.equals(downsampleShardIndexerStatus);
    }

    public boolean started() {
        return DownsampleShardIndexerStatus.STARTED.equals(downsampleShardIndexerStatus);
    }

    public boolean cancelled() {
        return DownsampleShardIndexerStatus.CANCELLED.equals(downsampleShardIndexerStatus);
    }

    public boolean failed() {
        return DownsampleShardIndexerStatus.FAILED.equals(downsampleShardIndexerStatus);
    }

    public static DownsampleShardPersistentTaskState readFromStream(final StreamInput in) throws IOException {
        return new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.readFromStream(in), in.readBytesRef());
    }

    public static DownsampleShardPersistentTaskState fromXContent(final XContentParser parser) throws IOException {
        final DownsampleShardPersistentTaskState.Builder builder = new DownsampleShardPersistentTaskState.Builder();
        PARSER.parse(parser, builder, null);
        return builder.build();
    }

    public static class Builder {
        private DownsampleShardIndexerStatus status;
        private BytesRef tsid;

        public Builder status(final DownsampleShardIndexerStatus status) {
            this.status = status;
            return this;
        }

        public Builder tsid(final BytesRef tsid) {
            this.tsid = tsid;
            return this;
        }

        public DownsampleShardPersistentTaskState build() {
            return new DownsampleShardPersistentTaskState(status, tsid);
        }
    }
}
