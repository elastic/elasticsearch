/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

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

public record ShardWriteLoadDistribution(
    long timestamp,
    String dataStream,
    ShardId shardId,
    boolean primary,
    double indexingP50,
    double indexingP90,
    double indexingMax,
    double mergesP50,
    double mergesP90,
    double mergesMax,
    double refreshP50,
    double refreshP90,
    double refreshMax
) implements Writeable, ToXContentObject {

    private static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    private static final ParseField DATA_STREAM_FIELD = new ParseField("data_stream");
    private static final ParseField INDEX_NAME_FIELD = new ParseField("index_name");
    private static final ParseField INDEX_UUID_FIELD = new ParseField("index_uuid");
    private static final ParseField SHARD_FIELD = new ParseField("shard");
    private static final ParseField PRIMARY_FIELD = new ParseField("primary");

    private static final ParseField INDEXING_P50_FIELD = new ParseField("indexing_p50");
    private static final ParseField INDEXING_P90_FIELD = new ParseField("indexing_p90");
    private static final ParseField INDEXING_MAX_FIELD = new ParseField("indexing_max");

    private static final ParseField MERGE_P50_FIELD = new ParseField("merge_p50");
    private static final ParseField MERGE_P90_FIELD = new ParseField("merge_p90");
    private static final ParseField MERGE_MAX_FIELD = new ParseField("merge_max");

    private static final ParseField REFRESH_P50_FIELD = new ParseField("refresh_p50");
    private static final ParseField REFRESH_P90_FIELD = new ParseField("refresh_p90");
    private static final ParseField REFRESH_MAX_FIELD = new ParseField("refresh_max");

    public static final ConstructingObjectParser<ShardWriteLoadDistribution, String> PARSER = new ConstructingObjectParser<>(
        "shard_write_load_distribution",
        false,
        (args, name) -> new ShardWriteLoadDistribution(
            (long) args[0],
            (String) args[1],
            new ShardId(new Index((String) args[2], (String) args[3]), (int) args[4]),
            (boolean) args[5],
            // Indexing
            (double) args[6],
            (double) args[7],
            (double) args[8],
            // Merge
            (double) args[9],
            (double) args[10],
            (double) args[11],
            // Refresh
            (double) args[12],
            (double) args[13],
            (double) args[14]
        )
    );

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DATA_STREAM_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_UUID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), PRIMARY_FIELD);

        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), INDEXING_P50_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), INDEXING_P90_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), INDEXING_MAX_FIELD);

        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MERGE_P50_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MERGE_P90_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MERGE_MAX_FIELD);

        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), REFRESH_P50_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), REFRESH_P90_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), REFRESH_MAX_FIELD);
    }

    ShardWriteLoadDistribution(StreamInput in) throws IOException {
        this(
            in.readLong(),
            in.readString(),
            new ShardId(in),
            in.readBoolean(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble(),
            in.readDouble()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeString(dataStream);
        shardId.writeTo(out);
        out.writeBoolean(primary);

        out.writeDouble(indexingP50);
        out.writeDouble(indexingP90);
        out.writeDouble(indexingMax);

        out.writeDouble(mergesP50);
        out.writeDouble(mergesP90);
        out.writeDouble(mergesMax);

        out.writeDouble(refreshP50);
        out.writeDouble(refreshP90);
        out.writeDouble(refreshMax);
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

        builder.field(INDEXING_P50_FIELD.getPreferredName(), indexingP50);
        builder.field(INDEXING_P90_FIELD.getPreferredName(), indexingP90);
        builder.field(INDEXING_MAX_FIELD.getPreferredName(), indexingMax);

        builder.field(MERGE_P50_FIELD.getPreferredName(), mergesP50);
        builder.field(MERGE_P90_FIELD.getPreferredName(), mergesP90);
        builder.field(MERGE_MAX_FIELD.getPreferredName(), mergesMax);

        builder.field(REFRESH_P50_FIELD.getPreferredName(), refreshP50);
        builder.field(REFRESH_P90_FIELD.getPreferredName(), refreshP90);
        builder.field(REFRESH_MAX_FIELD.getPreferredName(), refreshMax);

        builder.endObject();

        return builder;
    }

    public static ShardWriteLoadDistribution fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

}
