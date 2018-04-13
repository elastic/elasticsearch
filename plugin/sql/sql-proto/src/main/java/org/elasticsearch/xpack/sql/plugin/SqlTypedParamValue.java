/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xpack.sql.type.DataType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represent a strongly typed parameter value
 */
public class SqlTypedParamValue implements ToXContentObject, Writeable {
    private static final ConstructingObjectParser<SqlTypedParamValue, Void> PARSER =
            new ConstructingObjectParser<>("params", true, objects ->
                    new SqlTypedParamValue(
                            objects[0],
                            DataType.fromEsType((String) objects[1])));

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField TYPE = new ParseField("type");

    static {
        PARSER.declareField(constructorArg(), (p, c) -> XContentParserUtils.parseFieldsValue(p), VALUE, ObjectParser.ValueType.VALUE);
        PARSER.declareString(constructorArg(), TYPE);
    }

    public final Object value;
    public final DataType dataType;

    public SqlTypedParamValue(Object value, DataType dataType) {
        this.value = value;
        this.dataType = dataType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", dataType.esType);
        builder.field("value", value);
        builder.endObject();
        return builder;
    }

    public static SqlTypedParamValue fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(dataType);
        out.writeGenericValue(value);
    }

    public SqlTypedParamValue(StreamInput in) throws IOException {
        dataType = in.readEnum(DataType.class);
        value = in.readGenericValue();
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
        return Objects.equals(value, that.value) &&
                dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    @Override
    public String toString() {
        return String.valueOf(value) + "[" + dataType + "]";
    }
}