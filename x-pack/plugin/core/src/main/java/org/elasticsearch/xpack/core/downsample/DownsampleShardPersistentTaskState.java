/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * @param downsampleShardIndexerStatus An instance of {@link DownsampleShardIndexerStatus} with the downsampleShardIndexerStatus of
 *                                     the downsample task
 * @param tsid The {@code _tsid} to resume the shard task from, or {@code null} if nothing is known to be indexed yet. While the
 *             task runs it is the watermark below which every bucket is known to be in the target index.
 * @param targetIndexUuid The UUID of the target index the {@code tsid} refers to, or {@code null} if unknown. A watermark is only
 *                        meaningful for the index it was reached on: if the target was deleted and re-created under the same
 *                        name, resuming from it would skip buckets the new index never received.
 */
public record DownsampleShardPersistentTaskState(
    DownsampleShardIndexerStatus downsampleShardIndexerStatus,
    @Nullable BytesRef tsid,
    @Nullable String targetIndexUuid
) implements PersistentTaskState {

    /**
     * Since this version the state carries the target index UUID. Nodes before it drop the UUID when they serialize the state, so a
     * watermark published while the cluster has such nodes could not be resumed from; none is published until every node supports
     * this version.
     */
    public static final TransportVersion DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID = TransportVersion.fromName(
        "downsample_resume_watermark_target_uuid"
    );

    public static final String NAME = DownsampleShardTask.TASK_NAME;
    private static final ParseField ROLLUP_SHARD_INDEXER_STATUS = new ParseField("status");
    /** The tsid in the encoding of the {@code _tsid} field; the former {@code tsid} field held it as UTF-8 text, see the parser. */
    private static final ParseField ENCODED_TSID = new ParseField("encoded_tsid");
    private static final ParseField TSID = new ParseField("tsid");
    private static final ParseField TARGET_INDEX_UUID = new ParseField("target_index_uuid");

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
            (p, c) -> DocValueFormat.TIME_SERIES_ID.parseBytesRef(p.text()),
            ENCODED_TSID,
            ObjectParser.ValueType.STRING
        );
        // The tsid used to be written as UTF-8 text under this field, which does not survive a round trip for a hash. It is still
        // accepted, so that a node can read the cluster state it persisted before an upgrade, but it carries no usable resume point
        // and is dropped.
        PARSER.declareString((builder, legacyTsid) -> {}, TSID);
        PARSER.declareString(DownsampleShardPersistentTaskState.Builder::targetIndexUuid, TARGET_INDEX_UUID);
    }

    /**
     * A state whose {@code tsid} is not bound to a target index. Such a watermark is never resumed from, so this is for states that
     * carry no watermark.
     */
    public DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus downsampleShardIndexerStatus, @Nullable BytesRef tsid) {
        this(downsampleShardIndexerStatus, tsid, null);
    }

    public DownsampleShardPersistentTaskState(final StreamInput in) throws IOException {
        this(
            DownsampleShardIndexerStatus.readFromStream(in),
            readTsid(in),
            in.getTransportVersion().supports(DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID) ? in.readOptionalString() : null
        );
    }

    /**
     * A {@code null} tsid is written as an empty one, read it back as {@code null} so that "nothing known" survives a round trip.
     */
    private static BytesRef readTsid(final StreamInput in) throws IOException {
        BytesRef tsid = in.readBytesRef();
        return tsid.length == 0 ? null : tsid;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ROLLUP_SHARD_INDEXER_STATUS.getPreferredName(), downsampleShardIndexerStatus);
        if (tsid != null) {
            // The tsid is a hash, not text, so it is encoded the same way the _tsid field is formatted in search hits
            builder.field(ENCODED_TSID.getPreferredName(), TimeSeriesIdFieldMapper.encodeTsid(tsid));
        }
        if (targetIndexUuid != null) {
            builder.field(TARGET_INDEX_UUID.getPreferredName(), targetIndexUuid);
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
        if (out.getTransportVersion().supports(DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID)) {
            out.writeOptionalString(targetIndexUuid);
        }
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
        return new DownsampleShardPersistentTaskState(in);
    }

    public static DownsampleShardPersistentTaskState fromXContent(final XContentParser parser) throws IOException {
        final DownsampleShardPersistentTaskState.Builder builder = new DownsampleShardPersistentTaskState.Builder();
        PARSER.parse(parser, builder, null);
        return builder.build();
    }

    public static class Builder {
        private DownsampleShardIndexerStatus status;
        private BytesRef tsid;
        private String targetIndexUuid;

        public Builder status(final DownsampleShardIndexerStatus status) {
            this.status = status;
            return this;
        }

        public Builder tsid(final BytesRef tsid) {
            this.tsid = tsid;
            return this;
        }

        public Builder targetIndexUuid(final String targetIndexUuid) {
            this.targetIndexUuid = targetIndexUuid;
            return this;
        }

        public DownsampleShardPersistentTaskState build() {
            return new DownsampleShardPersistentTaskState(status, tsid, targetIndexUuid);
        }
    }
}
