/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

import static java.time.ZoneOffset.UTC;

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

    private void setParam(int parameterIndex, Object value, int sqlType) throws SQLException {
        setParam(parameterIndex, value, TypeUtils.of(sqlType));
    }

    private void setParam(int parameterIndex, Object value, EsType type) throws SQLException {
        checkOpen();

        if (parameterIndex < 0 || parameterIndex > query.paramCount()) {
            throw new SQLException("Invalid parameter index [ " + parameterIndex + "; needs to be between 1 and [" + query.paramCount() +
                    "]");
        }

        query.setParam(parameterIndex, value, type);
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
        // ES lacks proper BigDecimal support, so this function simply maps a BigDecimal to a double, while verifying that no definition
        // is lost (i.e. the original value can be conveyed as a double).
        // While long (i.e. BIGINT) has a larger scale (than double), double has the higher precision more appropriate for BigDecimal.
        if (x.compareTo(BigDecimal.valueOf(x.doubleValue())) != 0) {
            throw new SQLException("BigDecimal value [" + x + "] out of supported double's range.");
        }
        setDouble(parameterIndex, x.doubleValue());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x, Types.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x, Types.VARBINARY);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, x, Types.TIMESTAMP);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject(parameterIndex, x, Types.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x, Types.TIMESTAMP);
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
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setParam(parameterIndex, null, EsType.NULL);
            return;
        }

        // check also here the unsupported types so that any unsupported interfaces ({@code java.sql.Struct},
        // {@code java.sql.Array} etc) will generate the correct exception message. Otherwise, the method call
        // {@code TypeConverter.fromJavaToJDBC(x.getClass())} will report the implementing class as not being supported.
        checkKnownUnsupportedTypes(x);
        setObject(parameterIndex, x, TypeUtils.of(x.getClass()).getVendorTypeNumber(), 0);
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
        if (cal == null) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
            return;
        }
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }
        // converting to UTC since this is what ES is storing internally
        setObject(parameterIndex, new Date(TypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), Types.TIMESTAMP);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (cal == null) {
            setObject(parameterIndex, x, Types.TIME);
            return;
        }
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
            return;
        }
        // converting to UTC since this is what ES is storing internally
        setObject(parameterIndex, new Time(TypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), Types.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (cal == null) {
            setObject(parameterIndex, x, Types.TIMESTAMP);
            return;
        }
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }
        // converting to UTC since this is what ES is storing internally
        setObject(parameterIndex, new Timestamp(TypeConverter.convertFromCalendarToUTC(x.getTime(), cal)), Types.TIMESTAMP);
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
        setObject(parameterIndex, x, TypeUtils.asSqlType(targetSqlType), scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, TypeUtils.of(targetSqlType), targetSqlType.getName());
    }

    private void setObject(int parameterIndex, Object x, EsType dataType, String typeString) throws SQLException {
        checkOpen();
        // set the null value on the type and exit
        if (x == null) {
            setParam(parameterIndex, null, dataType);
            return;
        }

        checkKnownUnsupportedTypes(x);
        if (x instanceof byte[]) {
            if (dataType != EsType.BINARY) {
                throw new SQLFeatureNotSupportedException(
                        "Conversion from type [byte[]] to [" + typeString + "] not supported");
            }
            setParam(parameterIndex, x, EsType.BINARY);
            return;
        }

        if (x instanceof Timestamp
                || x instanceof Calendar
                || x instanceof Date
                || x instanceof LocalDateTime
                || x instanceof Time
                || x instanceof java.util.Date)
        {
            if (dataType == EsType.DATETIME || dataType == EsType.TIME) {
                // converting to {@code java.util.Date} because this is the type supported by {@code XContentBuilder} for serialization
                java.util.Date dateToSet;
                if (x instanceof Timestamp) {
                    dateToSet = new java.util.Date(((Timestamp) x).getTime());
                } else if (x instanceof Calendar) {
                    dateToSet = ((Calendar) x).getTime();
                } else if (x instanceof Date) {
                    dateToSet = new java.util.Date(((Date) x).getTime());
                } else if (x instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) x;
                    dateToSet = new java.util.Date(ldt.toInstant(UTC).toEpochMilli());
                } else if (x instanceof Time) {
                    dateToSet = new java.util.Date(((Time) x).getTime());
                } else {
                    dateToSet = (java.util.Date) x;
                }

                setParam(parameterIndex, dateToSet, dataType);
                return;
            } else if (TypeUtils.isString(dataType)) {
                setParam(parameterIndex, String.valueOf(x), dataType);
                return;
            }
            // anything else other than VARCHAR and TIMESTAMP is not supported in this JDBC driver
            throw new SQLFeatureNotSupportedException(
                    "Conversion from type [" + x.getClass().getName() + "] to [" + typeString + "] not supported");
        }

        if (x instanceof Boolean
                || x instanceof Byte
                || x instanceof Short
                || x instanceof Integer
                || x instanceof Long
                || x instanceof Float
                || x instanceof Double
                || x instanceof String) {
            setParam(parameterIndex,
                    TypeConverter.convert(x, TypeUtils.of(x.getClass()), (Class<?>) TypeUtils.classOf(dataType), typeString),
                    dataType);
            return;
        }

        throw new SQLFeatureNotSupportedException(
                "Conversion from type [" + x.getClass().getName() + "] to [" + typeString + "] not supported");
    }

    private void checkKnownUnsupportedTypes(Object x) throws SQLFeatureNotSupportedException {
        List<Class<?>> unsupportedTypes = new ArrayList<>(Arrays.asList(Struct.class, Array.class, SQLXML.class,
                RowId.class, Ref.class, Blob.class, NClob.class, Clob.class, LocalDate.class, LocalTime.class,
                OffsetTime.class, OffsetDateTime.class, URL.class, BigDecimal.class));

        for (Class<?> clazz:unsupportedTypes) {
           if (clazz.isAssignableFrom(x.getClass())) {
                throw new SQLFeatureNotSupportedException("Objects of type [" + clazz.getName() + "] are not supported");
           }
        }
    }

    private Calendar getDefaultCalendar() {
        return Calendar.getInstance(cfg.timeZone(), Locale.ROOT);
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
