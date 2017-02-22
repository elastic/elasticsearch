/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.ml.action.OpenJobAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Jobs whether running or complete are in one of these states.
 * When a job is created it is initialised in to the state closed
 * i.e. it is not running.
 */
public enum JobState implements Task.Status {

    CLOSING, CLOSED, OPENING, OPENED, FAILED;

    public static final String NAME = OpenJobAction.NAME;//"JobState";

    private static final ConstructingObjectParser<JobState, Void> PARSER =
            new ConstructingObjectParser<>(NAME, args -> fromString((String) args[0]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("state"));
    }

    public static JobState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static JobState fromStream(StreamInput in) throws IOException {
        int ordinal = in.readVInt();
        if (ordinal < 0 || ordinal >= values().length) {
            throw new IOException("Unknown public enum JobState {\n ordinal [" + ordinal + "]");
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


    public static JobState fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * @return {@code true} if state matches any of the given {@code candidates}
     */
    public boolean isAnyOf(JobState... candidates) {
        return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
