/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public record ColumnInfo(String name, String type) implements Writeable {

    private static final InstantiatingObjectParser<ColumnInfo, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ColumnInfo, Void> parser = InstantiatingObjectParser.builder(
            "esql/column_info",
            true,
            ColumnInfo.class
        );
        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareString(constructorArg(), new ParseField("type"));
        PARSER = parser.build();
    }

    public static ColumnInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ColumnInfo(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type);
        builder.endObject();
        return builder;
    }
}
