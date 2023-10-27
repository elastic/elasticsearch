/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class DownsampleShardStatus implements Task.Status {
    public static final String NAME = "rollup-index-shard";
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField START_TIME_FIELD = new ParseField("start_time");
    private static final ParseField IN_NUM_DOCS_RECEIVED_FIELD = new ParseField("in_num_docs_received");
    private static final ParseField OUT_NUM_DOCS_SENT_FIELD = new ParseField("out_num_docs_sent");
    private static final ParseField OUT_NUM_DOCS_INDEXED_FIELD = new ParseField("out_num_docs_indexed");
    private static final ParseField OUT_NUM_DOCS_FAILED_FIELD = new ParseField("out_num_docs_failed");
    private static final ParseField TOTAL_SHARD_DOC_COUNT = new ParseField("total_shard_doc_count");
    private static final ParseField LAST_SOURCE_TIMESTAMP = new ParseField("last_source_timestamp");
    private static final ParseField LAST_TARGET_TIMESTAMP = new ParseField("last_target_timestamp");
    private static final ParseField LAST_INDEXING_TIMESTAMP = new ParseField("last_indexing_timestamp");
    private static final ParseField DOCS_PROCESSED = new ParseField("docs_processed");
    private static final ParseField INDEX_START_TIME_MILLIS = new ParseField("index_start_time");
    private static final ParseField INDEX_END_TIME_MILLIS = new ParseField("index_end_time");
    private static final ParseField DOCS_PROCESSED_PERCENTAGE = new ParseField("docs_processed_percentage");
    private static final ParseField ROLLUP_BULK_INFO = new ParseField("rollup_bulk_info");
    private static final ParseField ROLLUP_BEFORE_BULK_INFO = new ParseField("rollup_before_bulk_info");
    private static final ParseField ROLLUP_AFTER_BULK_INFO = new ParseField("rollup_after_bulk_info");
    private static final ParseField ROLLUP_SHARD_INDEXER_STATUS = new ParseField("rollup_shard_indexer_status");

    private final ShardId shardId;
    private final long rollupStart;
    private final long numReceived;
    private final long numSent;
    private final long numIndexed;
    private final long numFailed;
    private final long totalShardDocCount;
    private final long lastSourceTimestamp;
    private final long lastTargetTimestamp;
    private final long lastIndexingTimestamp;
    private final long indexStartTimeMillis;
    private final long indexEndTimeMillis;
    private final long docsProcessed;
    private final float docsProcessedPercentage;
    private final DownsampleBulkInfo downsampleBulkInfo;
    private final DownsampleBeforeBulkInfo downsampleBeforeBulkInfo;
    private final DownsampleAfterBulkInfo downsampleAfterBulkInfo;
    private final DownsampleShardIndexerStatus downsampleShardIndexerStatus;

    private static final ConstructingObjectParser<DownsampleShardStatus, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, args -> {
            final ShardId _shardId = ShardId.fromString((String) args[0]);
            long _rollupStart = Instant.parse((String) args[1]).toEpochMilli();
            final Long _numReceived = (Long) args[2];
            final Long _numSent = (Long) args[3];
            final Long _numIndexed = (Long) args[4];
            final Long _numFailed = (Long) args[5];
            final Long _totalShardDocCount = (Long) args[6];
            final Long _lastSourceTimestamp = (Long) args[7];
            final Long _lastTargetTimestamp = (Long) args[8];
            final Long _lastIndexingTimestamp = (Long) args[9];
            final Long _indexStartTimeMillis = (Long) args[10];
            final Long _indexEndTimeMillis = (Long) args[11];
            final Long _docsProcessed = (Long) args[12];
            final Float _docsProcessedPercentage = (Float) args[13];
            final DownsampleBulkInfo _downsampleBulkInfo = (DownsampleBulkInfo) args[14];
            final DownsampleBeforeBulkInfo _downsampleBeforeBulkInfo = (DownsampleBeforeBulkInfo) args[15];
            final DownsampleAfterBulkInfo _downsampleAfterBulkInfo = (DownsampleAfterBulkInfo) args[16];
            final DownsampleShardIndexerStatus _downsampleShardIndexerStatus = DownsampleShardIndexerStatus.valueOf((String) args[17]);
            return new DownsampleShardStatus(
                _shardId,
                _rollupStart,
                _numReceived,
                _numSent,
                _numIndexed,
                _numFailed,
                _totalShardDocCount,
                _lastSourceTimestamp,
                _lastTargetTimestamp,
                _lastIndexingTimestamp,
                _indexStartTimeMillis,
                _indexEndTimeMillis,
                _docsProcessed,
                _docsProcessedPercentage,
                _downsampleBulkInfo,
                _downsampleBeforeBulkInfo,
                _downsampleAfterBulkInfo,
                _downsampleShardIndexerStatus
            );
        });

        PARSER.declareString(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), START_TIME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), IN_NUM_DOCS_RECEIVED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_SENT_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_INDEXED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_FAILED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_SHARD_DOC_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_SOURCE_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_TARGET_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_INDEXING_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_START_TIME_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INDEX_END_TIME_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), DOCS_PROCESSED);
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), DOCS_PROCESSED_PERCENTAGE);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DownsampleBulkInfo.fromXContext(p),
            ROLLUP_BULK_INFO
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DownsampleBeforeBulkInfo.fromXContent(p),
            ROLLUP_BEFORE_BULK_INFO
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DownsampleAfterBulkInfo.fromXContent(p),
            ROLLUP_AFTER_BULK_INFO
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ROLLUP_SHARD_INDEXER_STATUS);
    }

    public DownsampleShardStatus(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        rollupStart = in.readLong();
        numReceived = in.readLong();
        numSent = in.readLong();
        numIndexed = in.readLong();
        numFailed = in.readLong();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_040) && in.readBoolean()) {
            totalShardDocCount = in.readVLong();
            lastSourceTimestamp = in.readVLong();
            lastTargetTimestamp = in.readVLong();
            lastIndexingTimestamp = in.readVLong();
            indexStartTimeMillis = in.readVLong();
            indexEndTimeMillis = in.readVLong();
            docsProcessed = in.readVLong();
            docsProcessedPercentage = in.readFloat();
            downsampleBulkInfo = new DownsampleBulkInfo(in);
            downsampleBeforeBulkInfo = new DownsampleBeforeBulkInfo(in);
            downsampleAfterBulkInfo = new DownsampleAfterBulkInfo(in);
            downsampleShardIndexerStatus = in.readEnum(DownsampleShardIndexerStatus.class);
        } else {
            totalShardDocCount = -1;
            lastSourceTimestamp = -1;
            lastTargetTimestamp = -1;
            lastIndexingTimestamp = -1;
            indexStartTimeMillis = -1;
            indexEndTimeMillis = -1;
            docsProcessed = 0;
            docsProcessedPercentage = 0;
            downsampleBulkInfo = null;
            downsampleBeforeBulkInfo = null;
            downsampleAfterBulkInfo = null;
            downsampleShardIndexerStatus = null;
        }
    }

    public DownsampleShardStatus(
        ShardId shardId,
        long rollupStart,
        long numReceived,
        long numSent,
        long numIndexed,
        long numFailed,
        long totalShardDocCount,
        long lastSourceTimestamp,
        long lastTargetTimestamp,
        long lastIndexingTimestamp,
        long indexStartTimeMillis,
        long indexEndTimeMillis,
        long docsProcessed,
        float docsProcessedPercentage,
        final DownsampleBulkInfo downsampleBulkInfo,
        final DownsampleBeforeBulkInfo downsampleBeforeBulkInfo,
        final DownsampleAfterBulkInfo downsampleAfterBulkInfo,
        final DownsampleShardIndexerStatus downsampleShardIndexerStatus
    ) {
        this.shardId = shardId;
        this.rollupStart = rollupStart;
        this.numReceived = numReceived;
        this.numSent = numSent;
        this.numIndexed = numIndexed;
        this.numFailed = numFailed;
        this.totalShardDocCount = totalShardDocCount;
        this.lastSourceTimestamp = lastSourceTimestamp;
        this.lastTargetTimestamp = lastTargetTimestamp;
        this.lastIndexingTimestamp = lastIndexingTimestamp;
        this.indexStartTimeMillis = indexStartTimeMillis;
        this.indexEndTimeMillis = indexEndTimeMillis;
        this.docsProcessed = docsProcessed;
        this.docsProcessedPercentage = docsProcessedPercentage;
        this.downsampleBulkInfo = downsampleBulkInfo;
        this.downsampleBeforeBulkInfo = downsampleBeforeBulkInfo;
        this.downsampleAfterBulkInfo = downsampleAfterBulkInfo;
        this.downsampleShardIndexerStatus = downsampleShardIndexerStatus;
    }

    public static DownsampleShardStatus fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD_FIELD.getPreferredName(), shardId);
        builder.field(START_TIME_FIELD.getPreferredName(), Instant.ofEpochMilli(rollupStart).toString());
        builder.field(IN_NUM_DOCS_RECEIVED_FIELD.getPreferredName(), numReceived);
        builder.field(OUT_NUM_DOCS_SENT_FIELD.getPreferredName(), numSent);
        builder.field(OUT_NUM_DOCS_INDEXED_FIELD.getPreferredName(), numIndexed);
        builder.field(OUT_NUM_DOCS_FAILED_FIELD.getPreferredName(), numFailed);
        builder.field(TOTAL_SHARD_DOC_COUNT.getPreferredName(), totalShardDocCount);
        builder.field(LAST_SOURCE_TIMESTAMP.getPreferredName(), lastSourceTimestamp);
        builder.field(LAST_TARGET_TIMESTAMP.getPreferredName(), lastTargetTimestamp);
        builder.field(LAST_INDEXING_TIMESTAMP.getPreferredName(), lastIndexingTimestamp);
        builder.field(INDEX_START_TIME_MILLIS.getPreferredName(), indexStartTimeMillis);
        builder.field(INDEX_END_TIME_MILLIS.getPreferredName(), indexEndTimeMillis);
        builder.field(DOCS_PROCESSED.getPreferredName(), docsProcessed);
        builder.field(DOCS_PROCESSED_PERCENTAGE.getPreferredName(), docsProcessedPercentage);
        downsampleBulkInfo.toXContent(builder, params);
        downsampleBeforeBulkInfo.toXContent(builder, params);
        downsampleAfterBulkInfo.toXContent(builder, params);
        builder.field(ROLLUP_SHARD_INDEXER_STATUS.getPreferredName(), downsampleShardIndexerStatus);
        return builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(rollupStart);
        out.writeLong(numReceived);
        out.writeLong(numSent);
        out.writeLong(numIndexed);
        out.writeLong(numFailed);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_040)) {
            out.writeBoolean(true);
            out.writeVLong(totalShardDocCount);
            out.writeVLong(lastSourceTimestamp);
            out.writeVLong(lastTargetTimestamp);
            out.writeVLong(lastIndexingTimestamp);
            out.writeVLong(indexStartTimeMillis);
            out.writeVLong(indexEndTimeMillis);
            out.writeVLong(docsProcessed);
            out.writeFloat(docsProcessedPercentage);
            downsampleBulkInfo.writeTo(out);
            downsampleBeforeBulkInfo.writeTo(out);
            downsampleAfterBulkInfo.writeTo(out);
            out.writeEnum(downsampleShardIndexerStatus);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DownsampleShardStatus that = (DownsampleShardStatus) o;
        return rollupStart == that.rollupStart
            && numReceived == that.numReceived
            && numSent == that.numSent
            && numIndexed == that.numIndexed
            && numFailed == that.numFailed
            && totalShardDocCount == that.totalShardDocCount
            && lastSourceTimestamp == that.lastSourceTimestamp
            && lastTargetTimestamp == that.lastTargetTimestamp
            && lastIndexingTimestamp == that.lastIndexingTimestamp
            && indexStartTimeMillis == that.indexStartTimeMillis
            && indexEndTimeMillis == that.indexEndTimeMillis
            && docsProcessed == that.docsProcessed
            && docsProcessedPercentage == that.docsProcessedPercentage
            && Objects.equals(shardId.getIndexName(), that.shardId.getIndexName())
            && Objects.equals(shardId.id(), that.shardId.id())
            && Objects.equals(downsampleBulkInfo, that.downsampleBulkInfo)
            && Objects.equals(downsampleBeforeBulkInfo, that.downsampleBeforeBulkInfo)
            && Objects.equals(downsampleAfterBulkInfo, that.downsampleAfterBulkInfo)
            && Objects.equals(downsampleShardIndexerStatus, that.downsampleShardIndexerStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            shardId.getIndexName(),
            shardId.id(),
            rollupStart,
            numReceived,
            numSent,
            numIndexed,
            numFailed,
            totalShardDocCount,
            lastSourceTimestamp,
            lastTargetTimestamp,
            lastIndexingTimestamp,
            indexStartTimeMillis,
            indexEndTimeMillis,
            docsProcessed,
            docsProcessedPercentage,
            downsampleBulkInfo,
            downsampleBeforeBulkInfo,
            downsampleAfterBulkInfo,
            downsampleShardIndexerStatus
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
