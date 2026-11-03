/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A typed reference stored on an AI index record: its backing store, an automation, a source, or a trace.
 */
public record TypeValue(String type, String value) implements Writeable, ToXContentObject {

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    private static final ConstructingObjectParser<TypeValue, Void> PARSER = new ConstructingObjectParser<>(
        "type_value",
        args -> new TypeValue((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareString(constructorArg(), VALUE_FIELD);
    }

    public static TypeValue fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public TypeValue(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }
}
