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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record NodeWriteLoadHistogramSnapshot(String nodeId, WriteLoadHistogramSnapshot writeLoadHistogramSnapshot)
    implements
        Writeable,
        ToXContentObject {

    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");

    public static final ConstructingObjectParser<NodeWriteLoadHistogramSnapshot, Void> PARSER = new ConstructingObjectParser<>(
        "node_write_load_histogram_snapshot",
        false,
        (args, name) -> new NodeWriteLoadHistogramSnapshot(
            (String) args[0],
            new WriteLoadHistogramSnapshot(
                (long) args[1],
                (HistogramSnapshot) args[2],
                (HistogramSnapshot) args[3],
                (HistogramSnapshot) args[4]
            )
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        WriteLoadHistogramSnapshot.configureParser(PARSER);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
        writeLoadHistogramSnapshot.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public NodeWriteLoadHistogramSnapshot(StreamInput in) throws IOException {
        this(in.readString(), new WriteLoadHistogramSnapshot(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        writeLoadHistogramSnapshot.writeTo(out);
    }

    public static NodeWriteLoadHistogramSnapshot parse(XContentParser parser) throws IOException {
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
