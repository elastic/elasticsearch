/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.CURSOR;
import static org.elasticsearch.xpack.sql.proto.Mode.CLI;
import static org.elasticsearch.xpack.sql.proto.Mode.JDBC;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.isClientCompatible;

/**
 * Response to perform an sql query
 */
public class SqlQueryResponse extends ActionResponse implements ToXContentObject, QlStatusResponse.AsyncStatus {

    // TODO: Simplify cursor handling
    private String cursor;
    private Mode mode;
    private SqlVersion sqlVersion;
    private boolean columnar;
    private List<ColumnInfo> columns;
    // TODO investigate reusing Page here - it probably is much more efficient
    private List<List<Object>> rows;
    private static final String INTERVAL_CLASS_NAME = "Interval";
    // async
    private final @Nullable String asyncExecutionId;
    private final boolean isPartial;
    private final boolean isRunning;

    public SqlQueryResponse(StreamInput in) throws IOException {
        cursor = in.readString();
        if (in.readBoolean()) {
            // We might have rows without columns and we might have columns without rows
            // So we send the column size twice, just to keep the protocol simple
            int columnCount = in.readVInt();
            List<ColumnInfo> columns = new ArrayList<>(columnCount);
            for (int c = 0; c < columnCount; c++) {
                columns.add(readColumnInfo(in));
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
        columnar = in.readBoolean();
        asyncExecutionId = in.readOptionalString();
        isPartial = in.readBoolean();
        isRunning = in.readBoolean();
    }

    public SqlQueryResponse(
        String cursor,
        Mode mode,
        SqlVersion sqlVersion,
        boolean columnar,
        @Nullable List<ColumnInfo> columns,
        List<List<Object>> rows,
        @Nullable String asyncExecutionId,
        boolean isPartial,
        boolean isRunning
    ) {
        this.cursor = cursor;
        this.mode = mode;
        this.sqlVersion = sqlVersion != null ? sqlVersion : SqlVersions.SERVER_COMPAT_VERSION;
        this.columnar = columnar;
        this.columns = columns;
        this.rows = rows;
        this.asyncExecutionId = asyncExecutionId;
        this.isPartial = isPartial;
        this.isRunning = isRunning;
    }

    public SqlQueryResponse(
        String cursor,
        Mode mode,
        SqlVersion sqlVersion,
        boolean columnar,
        @Nullable List<ColumnInfo> columns,
        List<List<Object>> rows
    ) {
        this(cursor, mode, sqlVersion, columnar, columns, rows, null, false, false);
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results. If equal to "" then there is no next page.
     */
    public String cursor() {
        return cursor;
    }

    public boolean hasCursor() {
        return StringUtils.EMPTY.equals(cursor) == false;
    }

    public long size() {
        return rows.size();
    }

    public List<ColumnInfo> columns() {
        return columns;
    }

    public boolean columnar() {
        return columnar;
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(cursor);
        if (columns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(columns.size());
            for (ColumnInfo column : columns) {
                writeColumnInfo(out, column);
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
        out.writeBoolean(columnar);
        out.writeOptionalString(asyncExecutionId);
        out.writeBoolean(isPartial);
        out.writeBoolean(isRunning);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (hasId()) {
                builder.field(Protocol.ID_NAME, asyncExecutionId);
                builder.field(Protocol.IS_PARTIAL_NAME, isPartial);
                builder.field(Protocol.IS_RUNNING_NAME, isRunning);
            }

            if (columns != null) {
                builder.startArray("columns");

                for (ColumnInfo column : columns) {
                    toXContent(column, builder, params);
                }
                builder.endArray();
            }

            if (columnar) {
                // columns can be specified (for the first REST request for example), or not (on a paginated/cursor based request)
                // if the columns are missing, we take the first rows' size as the number of columns
                long columnsCount = columns != null ? columns.size() : 0;
                if (size() > 0) {
                    columnsCount = rows().get(0).size();
                }

                builder.startArray("values");
                for (int index = 0; index < columnsCount; index++) {
                    builder.startArray();
                    for (List<Object> row : rows()) {
                        value(builder, mode, sqlVersion, row.get(index));
                    }
                    builder.endArray();
                }
                builder.endArray();
            } else {
                builder.startArray("rows");
                for (List<Object> row : rows()) {
                    builder.startArray();
                    for (Object value : row) {
                        value(builder, mode, sqlVersion, value);
                    }
                    builder.endArray();
                }
                builder.endArray();
            }

            if (cursor.equals("") == false) {
                builder.field(CURSOR.getPreferredName(), cursor);
            }
        }
        return builder.endObject();
    }

    /**
     * See sql-proto {@link org.elasticsearch.xpack.sql.proto.Payloads#generate(JsonGenerator, ColumnInfo)}
     */
    private static XContentBuilder toXContent(ColumnInfo info, XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        String table = info.table();
        if (table != null && table.isEmpty() == false) {
            builder.field("table", table);
        }
        builder.field("name", info.name());
        builder.field("type", info.esType());
        if (info.displaySize() != null) {
            builder.field("display_size", info.displaySize());
        }
        return builder.endObject();
    }

    /**
     * Serializes the provided value in SQL-compatible way based on the client mode
     */
    public static XContentBuilder value(XContentBuilder builder, Mode mode, SqlVersion sqlVersion, Object value) throws IOException {
        if (value instanceof ZonedDateTime zdt) {
            // use the ISO format
            if (mode == JDBC && isClientCompatible(SqlVersions.SERVER_COMPAT_VERSION, sqlVersion)) {
                builder.value(StringUtils.toString(zdt, sqlVersion));
            } else {
                builder.value(StringUtils.toString(zdt));
            }
        } else if (mode == CLI && value != null && value.getClass().getSuperclass().getSimpleName().equals(INTERVAL_CLASS_NAME)) {
            // use the SQL format for intervals when sending back the response for CLI
            // all other clients will receive ISO 8601 formatted intervals
            builder.value(value.toString());
        } else if (value instanceof org.elasticsearch.xpack.versionfield.Version) {
            builder.value(value.toString());
        } else {
            builder.value(value);
        }
        return builder;
    }

    public static ColumnInfo readColumnInfo(StreamInput in) throws IOException {
        String table = in.readString();
        String name = in.readString();
        String esType = in.readString();
        Integer displaySize = in.readOptionalVInt();

        return new ColumnInfo(table, name, esType, displaySize);
    }

    public static void writeColumnInfo(StreamOutput out, ColumnInfo columnInfo) throws IOException {
        out.writeString(columnInfo.table());
        out.writeString(columnInfo.name());
        out.writeString(columnInfo.esType());
        out.writeOptionalVInt(columnInfo.displaySize());
    }

    public boolean hasId() {
        return Strings.hasText(asyncExecutionId);
    }

    @Override
    public String id() {
        return asyncExecutionId;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public boolean isPartial() {
        return isPartial;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlQueryResponse that = (SqlQueryResponse) o;
        return Objects.equals(cursor, that.cursor) && Objects.equals(columns, that.columns) && Objects.equals(rows, that.rows);
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
