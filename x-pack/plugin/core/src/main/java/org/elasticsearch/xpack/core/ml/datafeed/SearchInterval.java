/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record SearchInterval(long startMs, long endMs) implements ToXContentObject, Writeable {

    public static final ParseField START = new ParseField("start");
    public static final ParseField START_MS = new ParseField("start_ms");
    public static final ParseField END = new ParseField("end");
    public static final ParseField END_MS = new ParseField("end_ms");

    public SearchInterval(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.timeField(START_MS.getPreferredName(), START.getPreferredName(), startMs);
        builder.timeField(END_MS.getPreferredName(), END.getPreferredName(), endMs);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startMs);
        out.writeVLong(endMs);
    }
}
