/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.jdbc.TypeUtils.baseType;

public class JdbcArray implements Array {

    private final EsType type;
    private final List<?> values;

    public JdbcArray(EsType type, List<?> values) {
        this.type = type;
        this.values = values;
    }
    @Override
    public String getBaseTypeName() throws SQLException {
        return baseType(type).getName();
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType(type).getVendorTypeNumber();
    }

    @Override
    public Object getArray() throws SQLException {
        return values.toArray();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getArray();
        }
        throw new SQLFeatureNotSupportedException("getArray with non-empty Map not supported");
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
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
        if (map == null || map.isEmpty()) {
            return getArray(index, count);
        }
        throw new SQLFeatureNotSupportedException("getArray with non-empty Map not supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as ResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as ResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as ResultSet not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array as ResultSet not supported");
    }

    @Override
    public void free() throws SQLException {
        // nop
    }
}
