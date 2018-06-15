/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.type.DataType;

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
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
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
        setObject(parameterIndex, x, Types.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x, Types.TINYINT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x, Types.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x, Types.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x, Types.REAL);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x, Types.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x, Types.BIGINT);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
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
        // the value of scaleOrLength parameter doesn't matter, as it's not used in the called method below
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setParam(parameterIndex, null, Types.NULL);
            return;
        }
        
        // check also here the unsupported types so that any unsupported interfaces ({@code java.sql.Struct},
        // {@code java.sql.Array} etc) will generate the correct exception message. Otherwise, the method call
        // {@code TypeConverter.fromJavaToJDBC(x.getClass())} will report the implementing class as not being supported.
        checkKnownUnsupportedTypes(x);
        setObject(parameterIndex, x, TypeConverter.fromJavaToJDBC(x.getClass()).getVendorTypeNumber(), 0);
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
        setObject(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return rs != null ? rs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (cal == null) {
            setTimestamp(parameterIndex, x);
            return;
        }
        if (x == null) {
            setParam(parameterIndex, null, Types.TIMESTAMP);
            return;
        }
        
        Calendar c = (Calendar) cal.clone();
        c.setTimeInMillis(x.getTime());
        setTimestamp(parameterIndex, new Timestamp(c.getTimeInMillis()));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new JdbcParameterMetaData(this);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        setObject(parameterIndex, x);
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
        setObject(parameterIndex, value);
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
        setObject(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkOpen();
        
        JDBCType targetJDBCType;
        try {
            // this is also a way to check early for the validity of the desired sql type
            targetJDBCType = JDBCType.valueOf(targetSqlType);
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.getMessage());
        }
        
        // set the null value on the type and exit
        if (x == null) {
            setParam(parameterIndex, null, targetSqlType);
            return;
        }
        
        checkKnownUnsupportedTypes(x);
        if (x instanceof Boolean
                || x instanceof Byte
                || x instanceof Short
                || x instanceof Integer
                || x instanceof Long
                || x instanceof Float
                || x instanceof Double
                || x instanceof String) {
            try {
                setParam(parameterIndex, 
                        TypeConverter.convert(x, TypeConverter.fromJavaToJDBC(x.getClass()), DataType.fromJdbcTypeToJava(targetJDBCType)), 
                        targetSqlType);
            } catch (ClassCastException cce) {
                throw new SQLException("Unable to convert " + x.getClass().getName() + " to " + targetJDBCType, cce);
            }
        } else if (x instanceof Timestamp
                || x instanceof Calendar
                || x instanceof java.util.Date
                || x instanceof LocalDateTime) {
            if (targetJDBCType == JDBCType.TIMESTAMP ) {
                // converting to {@code java.util.Date} because this is the type supported by {@code XContentBuilder} for serialization
                java.util.Date dateToSet;
                if (x instanceof Timestamp) {
                    dateToSet = new java.util.Date(((Timestamp) x).getTime());
                } else if (x instanceof Calendar) {
                    dateToSet = ((Calendar) x).getTime();
                } else if (x instanceof java.util.Date) {
                    dateToSet = (java.util.Date) x;
                } else {
                    LocalDateTime ldt = (LocalDateTime) x;
                    Calendar cal = Calendar.getInstance(cfg.timeZone());
                    cal.set(ldt.getYear(), ldt.getMonthValue() - 1, ldt.getDayOfMonth(), ldt.getHour(), ldt.getMinute(), ldt.getSecond());
                    
                    dateToSet = cal.getTime();
                }

                setParam(parameterIndex, dateToSet, Types.TIMESTAMP);
            } else if (targetJDBCType == JDBCType.VARCHAR) {
                setParam(parameterIndex, String.valueOf(x), Types.VARCHAR);
            } else {
                // anything other than VARCHAR and TIMESTAMP is not supported in this JDBC driver
                throw new SQLFeatureNotSupportedException("Conversion from type " + x.getClass().getName() + " to " + targetJDBCType + " not supported");
            }
        } else {
            throw new SQLFeatureNotSupportedException("Conversion from type " + x.getClass().getName() + " to " + targetJDBCType + " not supported");
        }
    }

    private void checkKnownUnsupportedTypes(Object x) throws SQLFeatureNotSupportedException {
        if (x instanceof Struct) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.Struct are not supported");
        } else if (x instanceof Array) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.Array are not supported");
        } else if (x instanceof SQLXML) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.SQLXML are not supported");
        } else if (x instanceof RowId) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.RowId are not supported");
        } else if (x instanceof Ref) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.Ref are not supported");
        } else if (x instanceof Blob) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.Blob are not supported");
        } else if (x instanceof NClob) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.NClob are not supported");
        } else if (x instanceof Clob) {
            throw new SQLFeatureNotSupportedException("Objects of type java.sql.Clob are not supported");
        } else if (x instanceof LocalDate 
                || x instanceof LocalTime
                || x instanceof OffsetTime
                || x instanceof OffsetDateTime
                || x instanceof java.sql.Date
                || x instanceof java.sql.Time
                || x instanceof URL
                || x instanceof BigDecimal) {
            throw new SQLFeatureNotSupportedException("Objects of type " + x.getClass().getName() + " are not supported");
        } else if (x instanceof byte[]) {
            throw new UnsupportedOperationException("Bytes not implemented yet");
        }
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