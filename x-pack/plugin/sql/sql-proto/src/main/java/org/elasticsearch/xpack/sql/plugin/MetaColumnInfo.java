/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a column returned by the listColumns response
 */
public class MetaColumnInfo implements Writeable, ToXContentObject {

    private static final ConstructingObjectParser<MetaColumnInfo, Void> PARSER =
            new ConstructingObjectParser<>("column_info", true, objects ->
                    new MetaColumnInfo(
                            (String) objects[0],
                            (String) objects[1],
                            (String) objects[2],
                            objects[3] == null ? null : JDBCType.valueOf((int) objects[3]),
                            objects[4] == null ? 0 : (int) objects[4],
                            (int) objects[5]));

    private static final ParseField TABLE = new ParseField("table");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField ES_TYPE = new ParseField("type");
    private static final ParseField JDBC_TYPE = new ParseField("jdbc_type");
    private static final ParseField SIZE = new ParseField("size");
    private static final ParseField POSITION = new ParseField("position");

    static {
        PARSER.declareString(constructorArg(), TABLE);
        PARSER.declareString(constructorArg(), NAME);
        PARSER.declareString(constructorArg(), ES_TYPE);
        PARSER.declareInt(optionalConstructorArg(), JDBC_TYPE);
        PARSER.declareInt(optionalConstructorArg(), SIZE);
        PARSER.declareInt(constructorArg(), POSITION);
    }

    private final String table;
    private final String name;
    private final String esType;
    @Nullable
    private final JDBCType jdbcType;
    private final int size;
    private final int position;

    public MetaColumnInfo(String table, String name, String esType, JDBCType jdbcType, int size, int position) {
        this.table = table;
        this.name = name;
        this.esType = esType;
        this.jdbcType = jdbcType;
        this.size = size;
        this.position = position;
    }

    public MetaColumnInfo(String table, String name, String esType, int position) {
        this(table, name, esType, null, 0, position);
    }

    MetaColumnInfo(StreamInput in) throws IOException {
        table = in.readString();
        name = in.readString();
        esType = in.readString();
        if (in.readBoolean()) {
            jdbcType = JDBCType.valueOf(in.readVInt());
            size = in.readVInt();
        } else {
            jdbcType = null;
            size = 0;
        }
        position = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(table);
        out.writeString(name);
        out.writeString(esType);
        if (jdbcType != null) {
            out.writeBoolean(true);
            out.writeVInt(jdbcType.getVendorTypeNumber());
            out.writeVInt(size);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(position);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("table", table);
        builder.field("name", name);
        builder.field("type", esType);
        if (jdbcType != null) {
            builder.field("jdbc_type", jdbcType.getVendorTypeNumber());
            builder.field("size", size);
        }
        builder.field("position", position);
        return builder.endObject();
    }


    public static MetaColumnInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Name of the table.
     */
    public String table() {
        return table;
    }

    /**
     * Name of the column.
     */
    public String name() {
        return name;
    }

    /**
     * The type of the column in Elasticsearch.
     */
    public String esType() {
        return esType;
    }

    /**
     * The type of the column as it would be returned by a JDBC driver.
     */
    public JDBCType jdbcType() {
        return jdbcType;
    }

    /**
     * Precision
     */
    public int size() {
        return size;
    }

    /**
     * Column position with in the tables
     */
    public int position() {
        return position;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetaColumnInfo that = (MetaColumnInfo) o;
        return size == that.size &&
                position == that.position &&
                Objects.equals(table, that.table) &&
                Objects.equals(name, that.name) &&
                Objects.equals(esType, that.esType) &&
                jdbcType == that.jdbcType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, name, esType, jdbcType, size, position);
    }

}
