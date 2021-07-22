/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.sql.proto.Protocol.COLUMNS_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.ID_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.IS_PARTIAL_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.IS_RUNNING_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.ROWS_NAME;

/**
 * Response to perform an sql query for JDBC/CLI client
 */
public class SqlQueryResponse {

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlQueryResponse, Void> PARSER = new ConstructingObjectParser<>("sql", true,
            objects -> new SqlQueryResponse(
                    objects[0] == null ? "" : (String) objects[0],
                    (List<ColumnInfo>) objects[1],
                    (List<List<Object>>) objects[2],
                    (String) objects[3],
                    objects[4] != null && (boolean) objects[4],
                    objects[5] != null && (boolean) objects[5]));

    public static final ParseField CURSOR = new ParseField(CURSOR_NAME);
    public static final ParseField COLUMNS = new ParseField(COLUMNS_NAME);
    public static final ParseField ROWS = new ParseField(ROWS_NAME);
    public static final ParseField ID = new ParseField(ID_NAME);
    public static final ParseField IS_PARTIAL = new ParseField(IS_PARTIAL_NAME);
    public static final ParseField IS_RUNNING = new ParseField(IS_RUNNING_NAME);

    static {
        PARSER.declareString(optionalConstructorArg(), CURSOR);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ColumnInfo.fromXContent(p), COLUMNS);
        PARSER.declareField(constructorArg(), (p, c) -> parseRows(p), ROWS, ValueType.OBJECT_ARRAY);
        PARSER.declareString(optionalConstructorArg(), ID);
        PARSER.declareBoolean(optionalConstructorArg(), IS_PARTIAL);
        PARSER.declareBoolean(optionalConstructorArg(), IS_RUNNING);
    }

    // TODO: Simplify cursor handling
    private final String cursor;
    private final List<ColumnInfo> columns;
    // TODO investigate reusing Page here - it probably is much more efficient
    private final List<List<Object>> rows;
    // async
    private final @Nullable String asyncExecutionId;
    private final boolean isPartial;
    private final boolean isRunning;

    public SqlQueryResponse(String cursor, @Nullable List<ColumnInfo> columns, List<List<Object>> rows) {
        this(cursor, columns, rows, null, false, false);
    }

    public SqlQueryResponse(String cursor, @Nullable List<ColumnInfo> columns, List<List<Object>> rows, String asyncExecutionId,
                            boolean isPartial, boolean isRunning) {
        this.cursor = cursor;
        this.columns = columns;
        this.rows = rows;
        this.asyncExecutionId = asyncExecutionId;
        this.isPartial = isPartial;
        this.isRunning = isRunning;
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

    public String id() {
        return asyncExecutionId;
    }

    public boolean isPartial() {
        return isPartial;
    }

    public boolean isRunning() {
        return isRunning;
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
        List<Object> savedList = null;
        for (boolean parsing = true; parsing; ) {
            switch (parser.nextToken()) {
                case END_ARRAY:
                    if (savedList != null) {
                        savedList.add(list);
                        list = savedList;
                        savedList = null;
                    } else {
                        parsing = false;
                    }
                    break;
                case START_ARRAY:
                    if (savedList == null) {
                        savedList = list;
                        list = new ArrayList<>();
                    } else {
                        throw new IllegalStateException("multidimensional multivalue not supported");
                    }
                    break;
                case VALUE_NULL:
                    list.add(null);
                    break;
                default:
                    if (parser.currentToken().isValue()) {
                        list.add(ProtoUtils.parseFieldsValue(parser));
                    } else {
                        throw new IllegalStateException("expected value but got [" + parser.currentToken() + "]");
                    }
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
                Objects.equals(rows, that.rows) &&
                Objects.equals(asyncExecutionId, that.asyncExecutionId) &&
                isPartial == that.isPartial &&
                isRunning == that.isRunning;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, columns, rows, asyncExecutionId, isPartial, isRunning);
    }

}
