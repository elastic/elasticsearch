/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record ShardWriteLoadHistogramSnapshot(
    String dataStream,
    ShardId shardId,
    boolean primary,
    WriteLoadHistogramSnapshot writeLoadHistogramSnapshot
) implements Writeable, ToXContentObject {

    private static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField PRIMARY_FIELD = new ParseField("primary");

    public static final ConstructingObjectParser<ShardWriteLoadHistogramSnapshot, Void> PARSER = new ConstructingObjectParser<>(
        "shard_write_load_histogram_snapshot",
        false,
        (args, name) -> new ShardWriteLoadHistogramSnapshot(
            (String) args[0],
            new ShardId(new Index((String) args[1], (String) args[2]), (int) args[3]),
            (boolean) args[4],
            new WriteLoadHistogramSnapshot(
                (long) args[5],
                (HistogramSnapshot) args[6],
                (HistogramSnapshot) args[7],
                (HistogramSnapshot) args[8]
            )
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DATA_STREAM_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), PRIMARY_FIELD);
        WriteLoadHistogramSnapshot.configureParser(PARSER);
    }

    public ShardWriteLoadHistogramSnapshot(StreamInput in) throws IOException {
        this(in.readString(), new ShardId(in), in.readBoolean(), new WriteLoadHistogramSnapshot(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(dataStream);
        shardId.writeTo(out);
        out.writeBoolean(primary);
        writeLoadHistogramSnapshot.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DATA_STREAM_FIELD.getPreferredName(), dataStream);
        builder.field(INDEX_NAME_FIELD.getPreferredName(), shardId.getIndex().getName());
        builder.field(INDEX_UUID_FIELD.getPreferredName(), shardId.getIndex().getUUID());
        builder.field(SHARD_FIELD.getPreferredName(), shardId.getId());
        builder.field(PRIMARY_FIELD.getPreferredName(), primary);
        writeLoadHistogramSnapshot.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    public static ShardWriteLoadHistogramSnapshot fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public long timestamp() {
        return writeLoadHistogramSnapshot.timestamp();
    }

    public HistogramSnapshot indexLoadHistogramSnapshot() {
        return writeLoadHistogramSnapshot.indexLoadHistogramSnapshot();
    }

    public HistogramSnapshot mergeLoadHistogramSnapshot() {
        return writeLoadHistogramSnapshot.mergeLoadHistogramSnapshot();
    }

    public HistogramSnapshot refreshLoadHistogramSnapshot() {
        return writeLoadHistogramSnapshot.refreshLoadHistogramSnapshot();
    }
}
