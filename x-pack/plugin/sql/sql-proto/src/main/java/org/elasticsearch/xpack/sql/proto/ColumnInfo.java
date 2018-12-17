/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a column returned with first query response.
 * As this represents the response for all drivers, it is important for it to be explicit about
 * its structure, in particular types (using es_type and jdbc_type instead of DataType).
 */
public class ColumnInfo implements ToXContentObject {

    private static final ConstructingObjectParser<ColumnInfo, Void> PARSER =
        new ConstructingObjectParser<>("column_info", true, objects ->
            new ColumnInfo(
                objects[0] == null ? "" : (String) objects[0],
                (String) objects[1],
                (String) objects[2],
                objects[3] == null ? null : (int) objects[3],
                objects[4] == null ? 0 : (int) objects[4]));

    private static final ParseField TABLE = new ParseField("table");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField ES_TYPE = new ParseField("type");
    private static final ParseField JDBC_TYPE = new ParseField("jdbc_type");
    private static final ParseField DISPLAY_SIZE = new ParseField("display_size");

    static {
        PARSER.declareString(optionalConstructorArg(), TABLE);
        PARSER.declareString(constructorArg(), NAME);
        PARSER.declareString(constructorArg(), ES_TYPE);
        PARSER.declareInt(optionalConstructorArg(), JDBC_TYPE);
        PARSER.declareInt(optionalConstructorArg(), DISPLAY_SIZE);
    }

    private final String table;
    private final String name;
    private final String esType;
    @Nullable
    private final Integer jdbcType;
    private final int displaySize;

    public ColumnInfo(String table, String name, String esType, Integer jdbcType, int displaySize) {
        this.table = table;
        this.name = name;
        this.esType = esType;
        this.jdbcType = jdbcType;
        this.displaySize = displaySize;
    }

    public ColumnInfo(String table, String name, String esType) {
        this.table = table;
        this.name = name;
        this.esType = esType;
        this.jdbcType = null;
        this.displaySize = 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (table != null && table.isEmpty() == false) {
            builder.field("table", table);
        }
        builder.field("name", name);
        builder.field("type", esType);
        if (jdbcType != null) {
            builder.field("jdbc_type", jdbcType);
            builder.field("display_size", displaySize);
        }
        return builder.endObject();
    }


    public static ColumnInfo fromXContent(XContentParser parser) {
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
    public Integer jdbcType() {
        return jdbcType;
    }

    /**
     * Used by JDBC
     */
    public int displaySize() {
        return displaySize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnInfo that = (ColumnInfo) o;
        return displaySize == that.displaySize &&
            Objects.equals(table, that.table) &&
            Objects.equals(name, that.name) &&
            Objects.equals(esType, that.esType) &&
            Objects.equals(jdbcType, that.jdbcType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(table, name, esType, jdbcType, displaySize);
    }

    @Override
    public String toString() {
        return ProtoUtils.toString(this);
    }
}