/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
    final PreparedQuery query;

    JdbcPreparedStatement(JdbcConnection con, JdbcConfiguration info, String sql) throws SQLException {
        super(con, info);
        this.query = PreparedQuery.prepare(sql);
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        executeQuery();
        return true;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        initResultSet(query.sql(), query.params());
        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    private void setParam(int parameterIndex, Object value, int type) throws SQLException {
        checkOpen();

        if (parameterIndex < 0 || parameterIndex > query.paramCount()) {
            throw new SQLException("Invalid parameter index [ " + parameterIndex + "; needs to be between 1 and [" + query.paramCount() +
                    "]");
        }

        query.setParam(parameterIndex, value, JDBCType.valueOf(type));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, null, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, x, Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, x, Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, x, Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, x, Types.REAL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Bytes not implemented yet");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("Date/Time not implemented yet");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("Date/Time not implemented yet");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Date/Time not implemented yet");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        query.clearParams();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new UnsupportedOperationException("Object not implemented yet");
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return rs != null ? rs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Dates not implemented yet");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Dates not implemented yet");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Dates not implemented yet");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Datalink not supported");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new JdbcParameterMetaData(this);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Object not implemented yet");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }
}