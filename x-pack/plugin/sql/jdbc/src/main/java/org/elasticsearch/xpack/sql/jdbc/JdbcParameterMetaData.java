/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.jdbc.PreparedQuery.ParamInfo;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

class JdbcParameterMetaData implements ParameterMetaData, JdbcWrapper {

    private final JdbcPreparedStatement ps;

    JdbcParameterMetaData(JdbcPreparedStatement ps) {
        this.ps = ps;
    }

    @Override
    public int getParameterCount() throws SQLException {
        ps.checkOpen();
        return ps.query.paramCount();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        ps.checkOpen();
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return TypeUtils.isSigned(paramInfo(param).type);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        ps.checkOpen();
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        ps.checkOpen();
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return paramInfo(param).type.getVendorTypeNumber().intValue();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return paramInfo(param).type.getName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return TypeUtils.classOf(paramInfo(param).type).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        ps.checkOpen();
        return parameterModeUnknown;
    }

    private ParamInfo paramInfo(int param) throws SQLException {
        ps.checkOpen();
        return ps.query.getParam(param);
    }
}
