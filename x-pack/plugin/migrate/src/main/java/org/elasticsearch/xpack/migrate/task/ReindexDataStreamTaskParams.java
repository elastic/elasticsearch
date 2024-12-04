/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ReindexDataStreamTaskParams(String sourceDataStream, long startTime, int totalIndices, int totalIndicesToBeUpgraded)
    implements
        PersistentTaskParams {

    public static final String NAME = ReindexDataStreamTask.TASK_NAME;
    private static final String SOURCE_DATA_STREAM_FIELD = "source_data_stream";
    private static final String START_TIME_FIELD = "start_time";
    private static final String TOTAL_INDICES_FIELD = "total_indices";
    private static final String TOTAL_INDICES_TO_BE_UPGRADED_FIELD = "total_indices_to_be_upgraded";
    private static final ConstructingObjectParser<ReindexDataStreamTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        args -> new ReindexDataStreamTaskParams((String) args[0], (long) args[1], (int) args[2], (int) args[3])
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField(SOURCE_DATA_STREAM_FIELD));
        PARSER.declareLong(constructorArg(), new ParseField(START_TIME_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(TOTAL_INDICES_FIELD));
        PARSER.declareInt(constructorArg(), new ParseField(TOTAL_INDICES_TO_BE_UPGRADED_FIELD));
    }

    public ReindexDataStreamTaskParams(StreamInput in) throws IOException {
        this(in.readString(), in.readLong(), in.readInt(), in.readInt());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.REINDEX_DATA_STREAMS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceDataStream);
        out.writeLong(startTime);
        out.writeInt(totalIndices);
        out.writeInt(totalIndicesToBeUpgraded);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(SOURCE_DATA_STREAM_FIELD, sourceDataStream)
            .field(START_TIME_FIELD, startTime)
            .field(TOTAL_INDICES_FIELD, totalIndices)
            .field(TOTAL_INDICES_TO_BE_UPGRADED_FIELD, totalIndicesToBeUpgraded)
            .endObject();
    }

    public String getSourceDataStream() {
        return sourceDataStream;
    }

    public static ReindexDataStreamTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
