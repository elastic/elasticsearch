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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseStoredFieldsValue;

/**
 * Response to perform an sql query
 */
public class SqlResponse extends ActionResponse implements ToXContentObject {
    public static final String JDBC_ENABLED_PARAM = "jdbc_enabled";
    public static final int UNKNOWN_DISPLAY_SIZE = -1;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlResponse, Void> PARSER = new ConstructingObjectParser<>("sql", true,
            objects -> new SqlResponse(
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

    public SqlResponse() {
    }

    public SqlResponse(String cursor, @Nullable List<ColumnInfo> columns, List<List<Object>> rows) {
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

    public SqlResponse cursor(String cursor) {
        this.cursor = cursor;
        return this;
    }

    public SqlResponse columns(List<ColumnInfo> columns) {
        this.columns = columns;
        return this;
    }

    public SqlResponse rows(List<List<Object>> rows) {
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
        boolean isJdbcAllowed = params.paramAsBoolean(JDBC_ENABLED_PARAM, true);
        builder.startObject();
        {
            if (columns != null) {
                builder.startArray("columns");
                {
                    for (ColumnInfo column : columns) {
                        column.toXContent(builder, isJdbcAllowed);
                    }
                }
                builder.endArray();
            }
            builder.startArray("rows");
            for (List<Object> row : rows()) {
                builder.startArray();
                for (Object value : row) {
                    builder.value(value);
                }
                builder.endArray();
            }
            builder.endArray();

            if (cursor.equals("") == false) {
                builder.field(SqlRequest.CURSOR.getPreferredName(), cursor);
            }
        }
        return builder.endObject();
    }

    public static SqlResponse fromXContent(XContentParser parser) {
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
                list.add(parseStoredFieldsValue(parser));
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
        SqlResponse that = (SqlResponse) o;
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


    private static final ConstructingObjectParser<ColumnInfo, Void> COLUMN_INFO_PARSER =
            new ConstructingObjectParser<>("sql", true, objects ->
                    new ColumnInfo(
                            (String) objects[0],
                            (String) objects[1],
                            objects[2] == null ? null : JDBCType.valueOf((int) objects[2]),
                            objects[3] == null ? UNKNOWN_DISPLAY_SIZE : (int) objects[3]));

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField ES_TYPE = new ParseField("type");
    private static final ParseField JDBC_TYPE = new ParseField("jdbc_type");
    private static final ParseField DISPLAY_SIZE = new ParseField("display_size");

    static {
        COLUMN_INFO_PARSER.declareString(constructorArg(), NAME);
        COLUMN_INFO_PARSER.declareString(constructorArg(), ES_TYPE);
        COLUMN_INFO_PARSER.declareInt(optionalConstructorArg(), JDBC_TYPE);
        COLUMN_INFO_PARSER.declareInt(optionalConstructorArg(), DISPLAY_SIZE);
    }

    /**
     * Information about a column.
     */
    public static final class ColumnInfo implements Writeable, ToXContentObject {
        private final String name;
        private final String esType;
        private final JDBCType jdbcType;
        private final int displaySize;

        public ColumnInfo(String name, String esType, JDBCType jdbcType, int displaySize) {
            this.name = name;
            this.esType = esType;
            this.jdbcType = jdbcType;
            this.displaySize = displaySize;
        }

        ColumnInfo(StreamInput in) throws IOException {
            name = in.readString();
            esType = in.readString();
            jdbcType = JDBCType.valueOf(in.readVInt());
            displaySize = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(esType);
            out.writeVInt(jdbcType.getVendorTypeNumber());
            out.writeVInt(displaySize);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return toXContent(builder, params.paramAsBoolean(JDBC_ENABLED_PARAM, true));
        }

        public XContentBuilder toXContent(XContentBuilder builder, boolean isJdbcAllowed) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.field("type", esType);
            if (isJdbcAllowed && jdbcType != null) {
                builder.field("jdbc_type", jdbcType.getVendorTypeNumber());
                builder.field("display_size", displaySize);
            }
            return builder.endObject();
        }


        public static ColumnInfo fromXContent(XContentParser parser) {
            return COLUMN_INFO_PARSER.apply(parser, null);
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
         * Used by JDBC
         */
        public int displaySize() {
            return displaySize;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            ColumnInfo other = (ColumnInfo) obj;
            return name.equals(other.name)
                    && esType.equals(other.esType)
                    && jdbcType.equals(other.jdbcType)
                    && displaySize == other.displaySize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, esType, jdbcType, displaySize);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
