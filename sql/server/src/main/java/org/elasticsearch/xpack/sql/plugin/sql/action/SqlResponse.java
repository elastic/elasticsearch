/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

public class SqlResponse extends ActionResponse implements ToXContentObject {
    private Cursor cursor;
    private long size;
    private int columnCount;
    private List<ColumnInfo> columns;
    // TODO investigate reusing Page here - it probably is much more efficient
    private List<List<Object>> rows;

    public SqlResponse() {
    }

    public SqlResponse(Cursor cursor, long size, int columnCount, @Nullable List<ColumnInfo> columns, List<List<Object>> rows) {
        this.cursor = cursor;
        this.size = size;
        this.columnCount = columnCount;
        this.columns = columns;
        this.rows = rows;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results. If equal to {@link Cursor#EMPTY} then there is no next page.
     */
    public Cursor cursor() {
        return cursor;
    }

    public long size() {
        return size;
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    public List<List<Object>> rows() {
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        cursor = in.readNamedWriteable(Cursor.class);
        size = in.readVLong();
        columnCount = in.readVInt();
        if (in.readBoolean()) {
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
        for (int r = 0; r < rowCount; r++) {
            List<Object> row = new ArrayList<>(columnCount);
            for (int c = 0; c < columnCount; c++) {
                row.add(in.readGenericValue());
            }
            rows.add(unmodifiableList(row));
        }
        this.rows = unmodifiableList(rows);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(cursor);
        out.writeVLong(size);
        out.writeVInt(columnCount);
        if (columns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            assert columns.size() == columnCount;
            for (ColumnInfo column : columns) {
                column.writeTo(out);
            }
        }
        out.writeVInt(rows.size());
        for (List<Object> row : rows) {
            assert row.size() == columnCount;
            for (Object value : row) {
                out.writeGenericValue(value);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("size", size());
            if (columns != null) {
                builder.startArray("columns"); {
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
                    builder.value(value);
                }
                builder.endArray();
            }
            builder.endArray();

            if (cursor != Cursor.EMPTY) {
                builder.field(SqlRequest.CURSOR.getPreferredName(), Cursor.encodeToString(cursor));
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlResponse that = (SqlResponse) o;
        return size == that.size &&
                Objects.equals(cursor, that.cursor) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, size, columns, rows);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
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
            builder.startObject();
            builder.field("name", name);
            builder.field("type", esType);
            // TODO include jdbc_type?
            return builder.endObject();
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