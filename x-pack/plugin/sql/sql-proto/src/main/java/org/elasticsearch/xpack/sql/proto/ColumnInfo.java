/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.proto;

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
 * its structure, in particular types (using es_type instead of DataType).
 */
public class ColumnInfo implements ToXContentObject {

    private static final ConstructingObjectParser<ColumnInfo, Void> PARSER =
        new ConstructingObjectParser<>("column_info", true, objects ->
            new ColumnInfo(
                (String) objects[0],
                (String) objects[1],
                (String) objects[2],
                (String) objects[3],
                (Integer) objects[4]));

    private static final ParseField TABLE = new ParseField("table");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField BASE_NAME = new ParseField("base_name");
    private static final ParseField ES_TYPE = new ParseField("type");
    private static final ParseField DISPLAY_SIZE = new ParseField("display_size");

    static {
        PARSER.declareString(optionalConstructorArg(), TABLE);
        PARSER.declareString(constructorArg(), NAME);
        PARSER.declareString(optionalConstructorArg(), BASE_NAME);
        PARSER.declareString(constructorArg(), ES_TYPE);
        PARSER.declareInt(optionalConstructorArg(), DISPLAY_SIZE);
    }

    private final String table;
    private final String name;
    private final String baseName;
    private final String esType;
    private final Integer displaySize;

    public ColumnInfo(String table, String name, String baseName, String esType, Integer displaySize) {
        this.table = table;
        this.name = name;
        this.baseName = baseName;
        this.esType = esType;
        this.displaySize = displaySize;
    }

    public ColumnInfo(String table, String name, String esType, Integer displaySize) {
        this(table, name, null, esType, displaySize);
    }

    public ColumnInfo(String name, String esType, Integer displaySize) {
        this(null, name, null, esType, displaySize);
    }

    public ColumnInfo(String table, String name, String esType) {
        this(table, name, null, esType, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (table != null && table.isEmpty() == false) {
            builder.field("table", table);
        }
        builder.field("name", name);
        if (baseName != null) {
            builder.field("base_name", baseName);
        }
        builder.field("type", esType);
        if (displaySize != null) {
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
     * Base name (source) of the column.
     */
    public String baseName() {
        return baseName;
    }

    /**
     * The type of the column in Elasticsearch.
     */
    public String esType() {
        return esType;
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
        return Objects.equals(displaySize, that.displaySize) &&
            Objects.equals(table, that.table) &&
            Objects.equals(name, that.name) &&
            Objects.equals(baseName, that.baseName) &&
            Objects.equals(esType, that.esType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, name, baseName, esType, displaySize);
    }

    @Override
    public String toString() {
        return ProtoUtils.toString(this);
    }
}
