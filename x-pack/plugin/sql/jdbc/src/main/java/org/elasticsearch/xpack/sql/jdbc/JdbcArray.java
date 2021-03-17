/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.jdbc.JdbcDatabaseMetaData.columnInfo;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDatabaseMetaData.memorySet;

class JdbcArray implements Array {

    static final String TABLE_NAME = "ARRAY";
    static final String COLUMN_INDEX = "index";
    static final String COLUMN_VALUE = "value";

    private final TimeZone timeZone;
    private final EsType baseType;
    private final List<?> values;
    private boolean freed = false;

    JdbcArray(TimeZone timeZone, EsType baseType, List<?> values) {
        this.timeZone = timeZone;
        this.baseType = baseType;
        this.values = values;
    }
    @Override
    public String getBaseTypeName() throws SQLException {
        checkNotFreed();
        return baseType.getName();
    }

    @Override
    public int getBaseType() throws SQLException {
        checkNotFreed();
        return baseType.getVendorTypeNumber();
    }

    @Override
    public Object getArray() throws SQLException {
        checkNotFreed();
        return values.toArray();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        checkNullOrEmptyMap(map);
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        checkNotFreed();
        if (index < 1 || index > Integer.MAX_VALUE) {
            throw new SQLException("Index value [" + index + "] out of range [1, " + Integer.MAX_VALUE + "]");
        }
        if (count < 0) {
            throw new SQLException("Illegal negative count [" + count + "]");
        }
        int index0 = (int) index - 1; // 0-based index
        int available = index0 < values.size() ? values.size() - index0 : 0;
        return available > 0 ?
            Arrays.copyOfRange(values.toArray(), index0, index0 + Math.min(count, available)):
            new Object[0];
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        checkNullOrEmptyMap(map);
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet(1, values.size());
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        checkNullOrEmptyMap(map);
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        checkNotFreed();
        Object[] vals = (Object[]) getArray(index, count);
        Object[][] data = new Object[vals.length][2];
        for (int i = 0; i < vals.length; i++) {
            data[i][0] = index + i;
            data[i][1] = vals[i];
        }
        List<JdbcColumnInfo> columns = columnInfo(TABLE_NAME, COLUMN_INDEX, JDBCType.BIGINT, COLUMN_VALUE, baseType);
        return memorySet(timeZone, columns, data);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        checkNullOrEmptyMap(map);
        return getResultSet(index, count);
    }

    @Override
    public void free() throws SQLException {
        freed = true;
    }

    private void checkNotFreed() throws SQLException {
        if (freed) {
            throw new SQLException("Array has been freed already");
        }
    }

    private void checkNullOrEmptyMap(Map<String, Class<?>> map) throws SQLFeatureNotSupportedException {
        if (map != null && map.isEmpty() == false) {
            throw new SQLFeatureNotSupportedException("non-empty Map parameter not supported");
        }
    }
}
