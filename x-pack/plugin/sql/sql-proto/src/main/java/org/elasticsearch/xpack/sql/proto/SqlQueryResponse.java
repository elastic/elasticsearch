/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.xpack.sql.proto.core.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Response to perform an sql query for JDBC/CLI client
 */
public class SqlQueryResponse {

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

    public SqlQueryResponse(
        String cursor,
        @Nullable List<ColumnInfo> columns,
        List<List<Object>> rows,
        String asyncExecutionId,
        boolean isPartial,
        boolean isRunning
    ) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlQueryResponse that = (SqlQueryResponse) o;
        return Objects.equals(cursor, that.cursor)
            && Objects.equals(columns, that.columns)
            && Objects.equals(rows, that.rows)
            && Objects.equals(asyncExecutionId, that.asyncExecutionId)
            && isPartial == that.isPartial
            && isRunning == that.isRunning;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, columns, rows, asyncExecutionId, isPartial, isRunning);
    }

}
