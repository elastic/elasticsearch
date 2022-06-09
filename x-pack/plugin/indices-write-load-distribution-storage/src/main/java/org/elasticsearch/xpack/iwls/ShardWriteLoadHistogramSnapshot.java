/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.iwls;

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
    long timestamp,
    String dataStream,
    ShardId shardId,
    boolean primary,
    HistogramSnapshot indexingHistogramSnapshot,
    HistogramSnapshot mergingHistogramSnapshot,
    HistogramSnapshot refreshHistogramSnapshot
) implements Writeable, ToXContentObject {

    private static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    private static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField PRIMARY_FIELD = new ParseField("primary");

    private static final ParseField INDEXING_LOAD_DISTRIBUTION_FIELD = new ParseField("indexing_load_distribution");
    private static final ParseField MERGING_LOAD_DISTRIBUTION_FIELD = new ParseField("merge_load_distribution");
    private static final ParseField REFRESH_LOAD_DISTRIBUTION_FIELD = new ParseField("refresh_load_distribution");

    public static final ConstructingObjectParser<ShardWriteLoadHistogramSnapshot, String> PARSER = new ConstructingObjectParser<>(
        "shard_write_load_distribution",
        false,
        (args, name) -> new ShardWriteLoadHistogramSnapshot(
            (long) args[0],
            (String) args[1],
            new ShardId(new Index((String) args[2], (String) args[3]), (int) args[4]),
            (boolean) args[5],
            (HistogramSnapshot) args[6],
            (HistogramSnapshot) args[7],
            (HistogramSnapshot) args[8]
        )
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DATA_STREAM_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), PRIMARY_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            INDEXING_LOAD_DISTRIBUTION_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            MERGING_LOAD_DISTRIBUTION_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> HistogramSnapshot.fromXContent(p),
            REFRESH_LOAD_DISTRIBUTION_FIELD
        );
    }

    public ShardWriteLoadHistogramSnapshot(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readString(),
            new ShardId(in),
            in.readBoolean(),
            new HistogramSnapshot(in),
            new HistogramSnapshot(in),
            new HistogramSnapshot(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeString(dataStream);
        shardId.writeTo(out);
        out.writeBoolean(primary);
        indexingHistogramSnapshot.writeTo(out);
        mergingHistogramSnapshot.writeTo(out);
        refreshHistogramSnapshot.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp);
        builder.field(DATA_STREAM_FIELD.getPreferredName(), dataStream);
        builder.field(INDEX_NAME_FIELD.getPreferredName(), shardId.getIndex().getName());
        builder.field(INDEX_UUID_FIELD.getPreferredName(), shardId.getIndex().getUUID());
        builder.field(SHARD_FIELD.getPreferredName(), shardId.getId());
        builder.field(PRIMARY_FIELD.getPreferredName(), primary);
        builder.field(INDEXING_LOAD_DISTRIBUTION_FIELD.getPreferredName(), indexingHistogramSnapshot);
        builder.field(MERGING_LOAD_DISTRIBUTION_FIELD.getPreferredName(), mergingHistogramSnapshot);
        builder.field(REFRESH_LOAD_DISTRIBUTION_FIELD.getPreferredName(), refreshHistogramSnapshot);
        builder.endObject();

        return builder;
    }

    public static ShardWriteLoadHistogramSnapshot fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
