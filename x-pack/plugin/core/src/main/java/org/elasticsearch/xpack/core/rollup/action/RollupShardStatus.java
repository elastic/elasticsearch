/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.TransportVersion;
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

public class RollupShardStatus implements Task.Status {
    public static final String NAME = "rollup-index-shard";
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField START_TIME_FIELD = new ParseField("start_time");
    private static final ParseField IN_NUM_DOCS_RECEIVED_FIELD = new ParseField("in_num_docs_received");
    private static final ParseField OUT_NUM_DOCS_SENT_FIELD = new ParseField("out_num_docs_sent");
    private static final ParseField OUT_NUM_DOCS_INDEXED_FIELD = new ParseField("out_num_docs_indexed");
    private static final ParseField OUT_NUM_DOCS_FAILED_FIELD = new ParseField("out_num_docs_failed");
    private static final ParseField TOTAL_DOC_COUNT = new ParseField("total_doc_count");
    private static final ParseField TOTAL_SHARD_DOC_COUNT = new ParseField("total_shard_doc_count");
    private static final ParseField LAST_SOURCE_TIMESTAMP = new ParseField("last_source_timestamp");
    private static final ParseField LAST_TARGET_TIMESTAMP = new ParseField("last_target_timestamp");
    private static final ParseField LAST_INDEXING_TIMESTAMP = new ParseField("last_indexing_timestamp");
    private static final ParseField BEFORE_BULK_INFO = new ParseField("rollup_before_bulk");
    private static final ParseField AFTER_BULK_INFO = new ParseField("rollup_after_bulk");
    private static final ParseField ROLLUP_SHARD_INDEXER_STATUS = new ParseField("rollup_shard_indexer_status");

    private final ShardId shardId;
    private final long rollupStart;
    private final long numReceived;
    private final long numSent;
    private final long numIndexed;
    private final long numFailed;
    private final long totalDocCount;
    private final long totalShardDocCount;
    private final long lastSourceTimestamp;
    private final long lastTargetTimestamp;
    private final long lastIndexingTimestamp;
    private final RollupBeforeBulkInfo rollupBeforeBulkInfo;
    private final RollupAfterBulkInfo rollupAfterBulkInfo;

    private final RollupShardIndexerStatus rollupShardIndexerStatus;

    private static final ConstructingObjectParser<RollupShardStatus, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new RollupShardStatus(
                ShardId.fromString((String) args[0]),
                Instant.parse((String) args[1]).toEpochMilli(),
                (Long) args[2],
                (Long) args[3],
                (Long) args[4],
                (Long) args[5],
                (Long) args[6],
                (Long) args[7],
                (Long) args[8],
                (Long) args[9],
                (Long) args[10],
                (RollupBeforeBulkInfo) args[11],
                (RollupAfterBulkInfo) args[12],
                RollupShardIndexerStatus.valueOf((String) args[13])
            )
        );

        PARSER.declareString(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), START_TIME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), IN_NUM_DOCS_RECEIVED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_SENT_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_INDEXED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), OUT_NUM_DOCS_FAILED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_DOC_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_SHARD_DOC_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_SOURCE_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_TARGET_TIMESTAMP);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_INDEXING_TIMESTAMP);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> RollupBeforeBulkInfo.fromXContent(p),
            BEFORE_BULK_INFO
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> RollupAfterBulkInfo.fromXContent(p),
            AFTER_BULK_INFO
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ROLLUP_SHARD_INDEXER_STATUS);
    }

    public RollupShardStatus(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        rollupStart = in.readLong();
        numReceived = in.readLong();
        numSent = in.readLong();
        numIndexed = in.readLong();
        numFailed = in.readLong();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_017) && in.readBoolean()) {
            totalDocCount = in.readLong();
            totalShardDocCount = in.readLong();
            lastSourceTimestamp = in.readLong();
            lastTargetTimestamp = in.readLong();
            lastIndexingTimestamp = in.readLong();
            rollupBeforeBulkInfo = in.readNamedWriteable(RollupBeforeBulkInfo.class, RollupBeforeBulkInfo.NAME);
            rollupAfterBulkInfo = in.readNamedWriteable(RollupAfterBulkInfo.class, RollupAfterBulkInfo.NAME);
            rollupShardIndexerStatus = in.readEnum(RollupShardIndexerStatus.class);
        } else {
            totalDocCount = -1;
            totalShardDocCount = -1;
            lastSourceTimestamp = -1;
            lastTargetTimestamp = -1;
            lastIndexingTimestamp = -1;
            rollupBeforeBulkInfo = null;
            rollupAfterBulkInfo = null;
            rollupShardIndexerStatus = null;
        }
    }

    public RollupShardStatus(
        ShardId shardId,
        long rollupStart,
        long numReceived,
        long numSent,
        long numIndexed,
        long numFailed,
        long totalDocCount,
        long totalShardDocCount,
        long lastSourceTimestamp,
        long lastTargetTimestamp,
        long lastIndexingTimestamp,
        final RollupBeforeBulkInfo rollupBeforeBulkInfo,
        final RollupAfterBulkInfo rollupAfterBulkInfo,
        final RollupShardIndexerStatus rollupShardIndexerStatus
    ) {
        this.shardId = shardId;
        this.rollupStart = rollupStart;
        this.numReceived = numReceived;
        this.numSent = numSent;
        this.numIndexed = numIndexed;
        this.numFailed = numFailed;
        this.totalDocCount = totalDocCount;
        this.totalShardDocCount = totalShardDocCount;
        this.lastSourceTimestamp = lastSourceTimestamp;
        this.lastTargetTimestamp = lastTargetTimestamp;
        this.lastIndexingTimestamp = lastIndexingTimestamp;
        this.rollupBeforeBulkInfo = rollupBeforeBulkInfo;
        this.rollupAfterBulkInfo = rollupAfterBulkInfo;
        this.rollupShardIndexerStatus = rollupShardIndexerStatus;
    }

    public static RollupShardStatus fromXContent(XContentParser parser) throws IOException {
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
        builder.field(TOTAL_DOC_COUNT.getPreferredName(), totalDocCount);
        builder.field(TOTAL_SHARD_DOC_COUNT.getPreferredName(), totalShardDocCount);
        builder.field(LAST_SOURCE_TIMESTAMP.getPreferredName(), lastSourceTimestamp);
        builder.field(LAST_TARGET_TIMESTAMP.getPreferredName(), lastTargetTimestamp);
        builder.field(LAST_INDEXING_TIMESTAMP.getPreferredName(), lastIndexingTimestamp);
        rollupBeforeBulkInfo.toXContent(builder, params);
        rollupAfterBulkInfo.toXContent(builder, params);
        builder.field(ROLLUP_SHARD_INDEXER_STATUS.getPreferredName(), rollupShardIndexerStatus);
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_017)) {
            out.writeBoolean(true);
            out.writeLong(totalDocCount);
            out.writeLong(totalShardDocCount);
            out.writeLong(lastSourceTimestamp);
            out.writeLong(lastTargetTimestamp);
            out.writeLong(lastIndexingTimestamp);
            rollupBeforeBulkInfo.writeTo(out);
            rollupAfterBulkInfo.writeTo(out);
            out.writeEnum(rollupShardIndexerStatus);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupShardStatus that = (RollupShardStatus) o;
        return rollupStart == that.rollupStart
            && numReceived == that.numReceived
            && numSent == that.numSent
            && numIndexed == that.numIndexed
            && numFailed == that.numFailed
            && totalDocCount == that.totalDocCount
            && totalShardDocCount == that.totalShardDocCount
            && lastSourceTimestamp == that.lastSourceTimestamp
            && lastTargetTimestamp == that.lastTargetTimestamp
            && lastIndexingTimestamp == that.lastIndexingTimestamp
            && Objects.equals(shardId.getIndexName(), that.shardId.getIndexName())
            && Objects.equals(shardId.id(), that.shardId.id())
            && Objects.equals(rollupBeforeBulkInfo, that.rollupBeforeBulkInfo)
            && Objects.equals(rollupAfterBulkInfo, that.rollupAfterBulkInfo)
            && Objects.equals(rollupShardIndexerStatus, that.rollupShardIndexerStatus);
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
            totalDocCount,
            totalShardDocCount,
            lastSourceTimestamp,
            lastTargetTimestamp,
            lastIndexingTimestamp,
            rollupBeforeBulkInfo,
            rollupAfterBulkInfo,
            rollupShardIndexerStatus
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
