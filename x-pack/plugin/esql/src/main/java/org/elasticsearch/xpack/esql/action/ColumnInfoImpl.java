/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ColumnInfoImpl implements ColumnInfo {

    public static final InstantiatingObjectParser<ColumnInfoImpl, Void> PARSER;
    static {
        InstantiatingObjectParser.Builder<ColumnInfoImpl, Void> parser = InstantiatingObjectParser.builder(
            "esql/column_info",
            true,
            ColumnInfoImpl.class
        );
        parser.declareString(constructorArg(), new ParseField("name"));
        parser.declareString(constructorArg(), new ParseField("type"));
        PARSER = parser.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof ColumnInfoImpl that)) {
            return Objects.equals(name, that.name) && Objects.equals(type, that.type);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    public static ColumnInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private String name;
    private DataType type;

    @ParserConstructor
    public ColumnInfoImpl(String name, String type) {
        this(name, DataType.fromEs(type));
    }

    public ColumnInfoImpl(String name, DataType type) {
        this.name = name;
        this.type = type;
    }

    public ColumnInfoImpl(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type.outputType());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type.outputType());
        builder.endObject();
        return builder;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String outputType() {
        return type.outputType();
    }

    public DataType type() {
        return type;
    }
}
