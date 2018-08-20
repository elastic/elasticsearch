/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response to perform an sql query for JDBC/CLI client
 */
public class SqlQueryResponse {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlQueryResponse, Void> PARSER = new ConstructingObjectParser<>("sql", true,
            objects -> new SqlQueryResponse(
                    objects[0] == null ? "" : (String) objects[0],
                    (List<ColumnInfo>) objects[1],
                    (List<List<Object>>) objects[2]));

    public static final ParseField CURSOR = new ParseField("cursor");
    public static final ParseField COLUMNS = new ParseField("columns");
    public static final ParseField ROWS = new ParseField("rows");

    static {
        PARSER.declareString(optionalConstructorArg(), CURSOR);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ColumnInfo.fromXContent(p), COLUMNS);
        PARSER.declareField(constructorArg(), (p, c) -> parseRows(p), ROWS, ValueType.OBJECT_ARRAY);
    }

    // TODO: Simplify cursor handling
    private final String cursor;
    private final List<ColumnInfo> columns;
    // TODO investigate reusing Page here - it probably is much more efficient
    private final List<List<Object>> rows;

    public SqlQueryResponse(String cursor, @Nullable List<ColumnInfo> columns, List<List<Object>> rows) {
        this.cursor = cursor;
        this.columns = columns;
        this.rows = rows;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results. If equal to "" then there is no next page.
     */
    public String cursor() {
        return cursor;
    }

    public long size() {
        return rows.size();
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    public List<List<Object>> rows() {
        return rows;
    }

    public static SqlQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static List<List<Object>> parseRows(XContentParser parser) throws IOException {
        List<List<Object>> list = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                list.add(parseRow(parser));
            } else {
                throw new IllegalStateException("expected start array but got [" + parser.currentToken() + "]");
            }
        }
        return list;
    }

    public static List<Object> parseRow(XContentParser parser) throws IOException {
        List<Object> list = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken().isValue()) {
                list.add(ProtoUtils.parseFieldsValue(parser));
            } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                list.add(null);
            } else {
                throw new IllegalStateException("expected value but got [" + parser.currentToken() + "]");
            }
        }
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlQueryResponse that = (SqlQueryResponse) o;
        return Objects.equals(cursor, that.cursor) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, columns, rows);
    }

}
