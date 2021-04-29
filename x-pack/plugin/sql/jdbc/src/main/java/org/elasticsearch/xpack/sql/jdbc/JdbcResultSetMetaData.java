/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

class JdbcResultSetMetaData implements ResultSetMetaData, JdbcWrapper {

    private final JdbcResultSet rs;
    private final List<JdbcColumnInfo> columns;

    JdbcResultSetMetaData(JdbcResultSet rs, List<JdbcColumnInfo> columns) {
        this.rs = rs;
        this.columns = columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
        checkOpen();
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        column(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        column(column);
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        column(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        column(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        column(column);
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return TypeUtils.isSigned(column(column).type);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return column(column).displaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        JdbcColumnInfo info = column(column);
        return EMPTY.equals(info.label) ? info.name : info.label;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return column(column).name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return column(column).schema;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        column(column);
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        column(column);
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return column(column).table;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return column(column).catalog;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return column(column).type.getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return column(column).type.getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        column(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        column(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        column(column);
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return TypeUtils.classOf(column(column).type).getName();
    }

    private void checkOpen() throws SQLException {
        if (rs != null) {
            rs.checkOpen();
        }
    }

    private JdbcColumnInfo column(int column) throws SQLException {
        checkOpen();
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Invalid column index [" + column + "]");
        }
        return columns.get(column - 1);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)", getClass().getSimpleName(), columns);
    }
}
