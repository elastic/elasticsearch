/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.sql.proto.ProtoUtils.parseFieldsValue;

/**
 * Represent a strongly typed parameter value
 */
public class SqlTypedParamValue implements ToXContentObject {
    private static final ConstructingObjectParser<SqlTypedParamValue, Void> PARSER =
            new ConstructingObjectParser<>("params", true, objects ->
            new SqlTypedParamValue((String) objects[1], objects[0]
                    ));

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField TYPE = new ParseField("type");

    static {
        PARSER.declareField(constructorArg(), (p, c) -> parseFieldsValue(p), VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareString(constructorArg(), TYPE);
    }

    public final Object value;
    public final String type;

    public SqlTypedParamValue(String type, Object value) {
        this.value = value;
        this.type = type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.field("value", value);
        builder.endObject();
        return builder;
    }

    public static SqlTypedParamValue fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlTypedParamValue that = (SqlTypedParamValue) o;
        return Objects.equals(value, that.value) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public String toString() {
        return String.valueOf(value) + "[" + type + "]";
    }
}