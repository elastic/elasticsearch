/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.action;

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
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class RollupShardStatus implements Task.Status {
    public static final String NAME = "rollup-index-shard";
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField START_TIME_FIELD = new ParseField("start_time");
    private static final ParseField IN_NUM_DOCS_RECEIVED_FIELD = new ParseField("in_num_docs_received");
    private static final ParseField OUT_NUM_DOCS_SENT_FIELD = new ParseField("out_num_docs_sent");
    private static final ParseField OUT_NUM_DOCS_INDEXED_FIELD = new ParseField("out_num_docs_indexed");
    private static final ParseField OUT_NUM_DOCS_FAILED_FIELD = new ParseField("out_num_docs_failed");

    private final ShardId shardId;
    private final long rollupStart;
    private final AtomicLong numReceived;
    private final AtomicLong numSent;
    private final AtomicLong numIndexed;
    private final AtomicLong numFailed;

    private static final ConstructingObjectParser<RollupShardStatus, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            args -> new RollupShardStatus(
                ShardId.fromString((String) args[0]),
                Instant.parse((String) args[2]).toEpochMilli(),
                new AtomicLong((Long) args[3]),
                new AtomicLong((Long) args[4]),
                new AtomicLong((Long) args[5]),
                new AtomicLong((Long) args[6])
            )
        );

        PARSER.declareString(constructorArg(), SHARD_FIELD);
        PARSER.declareString(constructorArg(), START_TIME_FIELD);
        PARSER.declareLong(constructorArg(), IN_NUM_DOCS_RECEIVED_FIELD);
        PARSER.declareLong(constructorArg(), OUT_NUM_DOCS_SENT_FIELD);
        PARSER.declareLong(constructorArg(), OUT_NUM_DOCS_INDEXED_FIELD);
        PARSER.declareLong(constructorArg(), OUT_NUM_DOCS_FAILED_FIELD);
    }

    public RollupShardStatus(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        rollupStart = in.readLong();
        numReceived = new AtomicLong(in.readLong());
        numSent = new AtomicLong(in.readLong());
        numIndexed = new AtomicLong(in.readLong());
        numFailed = new AtomicLong(in.readLong());
    }

    public RollupShardStatus(
        ShardId shardId,
        long rollupStart,
        AtomicLong numReceived,
        AtomicLong numSent,
        AtomicLong numIndexed,
        AtomicLong numFailed
    ) {
        this.shardId = shardId;
        this.rollupStart = rollupStart;
        this.numReceived = numReceived;
        this.numSent = numSent;
        this.numIndexed = numIndexed;
        this.numFailed = numFailed;
    }

    public static RollupShardStatus fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD_FIELD.getPreferredName(), shardId);
        builder.field(START_TIME_FIELD.getPreferredName(), Instant.ofEpochMilli(rollupStart).toString());
        builder.field(IN_NUM_DOCS_RECEIVED_FIELD.getPreferredName(), numReceived.get());
        builder.field(OUT_NUM_DOCS_SENT_FIELD.getPreferredName(), numSent.get());
        builder.field(OUT_NUM_DOCS_INDEXED_FIELD.getPreferredName(), numIndexed.get());
        builder.field(OUT_NUM_DOCS_FAILED_FIELD.getPreferredName(), numFailed.get());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(rollupStart);
        out.writeLong(numReceived.get());
        out.writeLong(numSent.get());
        out.writeLong(numIndexed.get());
        out.writeLong(numFailed.get());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RollupShardStatus that = (RollupShardStatus) o;
        return rollupStart == that.rollupStart
            && Objects.equals(shardId.getIndexName(), that.shardId.getIndexName())
            && Objects.equals(shardId.id(), that.shardId.id())
            && Objects.equals(numReceived.get(), that.numReceived.get())
            && Objects.equals(numSent.get(), that.numSent.get())
            && Objects.equals(numIndexed.get(), that.numIndexed.get())
            && Objects.equals(numFailed.get(), that.numFailed.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            shardId.getIndexName(),
            shardId.id(),
            rollupStart,
            numReceived.get(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get()
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
