/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;

/**
 * Response to perform an sql query
 */
public class SqlQueryResponse extends ActionResponse implements ToXContentObject {

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
    private String cursor;
    private List<ColumnInfo> columns;
    // TODO investigate reusing Page here - it probably is much more efficient
    private List<List<Object>> rows;

    public SqlQueryResponse() {
    }

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

    public SqlQueryResponse cursor(String cursor) {
        this.cursor = cursor;
        return this;
    }

    public SqlQueryResponse columns(List<ColumnInfo> columns) {
        this.columns = columns;
        return this;
    }

    public SqlQueryResponse rows(List<List<Object>> rows) {
        this.rows = rows;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        cursor = in.readString();
        if (in.readBoolean()) {
            // We might have rows without columns and we might have columns without rows
            // So we send the column size twice, just to keep the protocol simple
            int columnCount = in.readVInt();
            List<ColumnInfo> columns = new ArrayList<>(columnCount);
            for (int c = 0; c < columnCount; c++) {
                columns.add(new ColumnInfo(in));
            }
            this.columns = unmodifiableList(columns);
        } else {
            this.columns = null;
        }
        int rowCount = in.readVInt();
        List<List<Object>> rows = new ArrayList<>(rowCount);
        if (rowCount > 0) {
            int columnCount = in.readVInt();
            for (int r = 0; r < rowCount; r++) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int c = 0; c < columnCount; c++) {
                    row.add(in.readGenericValue());
                }
                rows.add(unmodifiableList(row));
            }
        }
        this.rows = unmodifiableList(rows);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(cursor);
        if (columns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(columns.size());
            for (ColumnInfo column : columns) {
                column.writeTo(out);
            }
        }
        out.writeVInt(rows.size());
        if (rows.size() > 0) {
            out.writeVInt(rows.get(0).size());
            for (List<Object> row : rows) {
                for (Object value : row) {
                    out.writeGenericValue(value);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        AbstractSqlRequest.Mode mode = AbstractSqlRequest.Mode.fromString(params.param("mode"));
        builder.startObject();
        {
            if (columns != null) {
                builder.startArray("columns");
                {
                    for (ColumnInfo column : columns) {
                        column.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            builder.startArray("rows");
            for (List<Object> row : rows()) {
                builder.startArray();
                for (Object value : row) {
                    value(builder, mode, value);
                }
                builder.endArray();
            }
            builder.endArray();

            if (cursor.equals("") == false) {
                builder.field(SqlQueryRequest.CURSOR.getPreferredName(), cursor);
            }
        }
        return builder.endObject();
    }

    /**
     * Serializes the provided value in SQL-compatible way based on the client mode
     */
    public static XContentBuilder value(XContentBuilder builder, AbstractSqlRequest.Mode mode, Object value) throws IOException {
        if (mode == AbstractSqlRequest.Mode.JDBC && value instanceof ReadableDateTime) {
            // JDBC cannot parse dates in string format
            builder.value(((ReadableDateTime) value).getMillis());
        } else {
            builder.value(value);
        }
        return builder;
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
                list.add(parseFieldsValue(parser));
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

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
