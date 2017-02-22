/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public enum DatafeedState implements Task.Status {

    STARTED, STOPPED;

    public static final String NAME = StartDatafeedAction.NAME;//"DatafeedState";

    private static final ConstructingObjectParser<DatafeedState, Void> PARSER =
            new ConstructingObjectParser<>(NAME, args -> fromString((String) args[0]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("state"));
    }

    public static DatafeedState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static DatafeedState fromStream(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown public enum DatafeedState ordinal [" + ordinal + "]");
        }
        return values()[ordinal];
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ordinal());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("state", name().toLowerCase(Locale.ROOT));
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public static DatafeedState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
