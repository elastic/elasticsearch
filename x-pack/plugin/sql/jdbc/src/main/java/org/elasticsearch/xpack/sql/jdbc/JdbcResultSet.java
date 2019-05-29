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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DATE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DATETIME;
import static org.elasticsearch.xpack.sql.jdbc.EsType.TIME;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.asDateTimeField;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.dateTimeAsMillisSinceEpoch;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.asTimestamp;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.timeAsMillisSinceEpoch;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.timeAsTime;
import static org.elasticsearch.xpack.sql.jdbc.JdbcDateUtils.timeAsTimestamp;

class JdbcResultSet implements ResultSet, JdbcWrapper {

    // temporary calendar instance (per connection) used for normalizing the date and time
    // even though the cfg is already in UTC format, JDBC 3.0 requires java.sql.Time to have its date
    // removed (set to Jan 01 1970) and java.sql.Date to have its HH:mm:ss component removed
    // instead of dealing with longs, a Calendar object is used instead
    private final Calendar defaultCalendar;

    private final JdbcStatement statement;
    private final Cursor cursor;
    private final Map<String, Integer> nameToIndex = new LinkedHashMap<>();

    private boolean closed = false;
    private boolean wasNull = false;

    private int rowNumber;

    JdbcResultSet(JdbcConfiguration cfg, @Nullable JdbcStatement statement, Cursor cursor) {
        this.statement = statement;
        this.cursor = cursor;
        // statement can be null so we have to extract the timeZone from the non-nullable cfg
        // TODO: should we consider the locale as well?
        this.defaultCalendar = Calendar.getInstance(cfg.timeZone(), Locale.ROOT);

        List<JdbcColumnInfo> columns = cursor.columns();
        for (int i = 0; i < columns.size(); i++) {
            nameToIndex.put(columns.get(i).name, Integer.valueOf(i + 1));
        }
    }

    private Object column(int columnIndex) throws SQLException {
        checkOpen();
        if (columnIndex < 1 || columnIndex > cursor.columnSize()) {
            throw new SQLException("Invalid column index [" + columnIndex + "]");
        }
        Object object = null;
        try {
            object = cursor.column(columnIndex - 1);
        } catch (IllegalArgumentException iae) {
            throw new SQLException(iae.getMessage());
        }
        wasNull = (object == null);
        return object;
    }

    private int column(String columnName) throws SQLException {
        checkOpen();
        Integer index = nameToIndex.get(columnName);
        if (index == null) {
            throw new SQLException("Invalid column label [" + columnName + "]");
        }
        return index.intValue();
    }

    private EsType columnType(int columnIndex) {
        return cursor.columns().get(columnIndex - 1).type;
    }

    void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Closed result set");
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        if (cursor.next()) {
            rowNumber++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            if (statement != null) {
                statement.resultSetWasClosed();
            }
            cursor.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getObject(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Boolean.class) : false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Byte.class) : 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Short.class) : 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Integer.class) : 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Long.class) : 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Float.class) : 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return column(columnIndex) != null ? getObject(columnIndex, Double.class) : 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            return (byte[]) column(columnIndex);
        } catch (ClassCastException cce) {
            throw new SQLException("unable to convert column " + columnIndex + " to a byte array", cce);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return asDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return asTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return asTimeStamp(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(column(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(column(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(column(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(column(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(column(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(column(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(column(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(column(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(column(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        // TODO: the error message in case the value in the column cannot be converted to a Date refers to a column index
        // (for example - "unable to convert column 4 to a long") and not to the column name, which is a bit confusing.
        // Should we reconsider this? Maybe by catching the exception here and rethrowing it with the columnLabel instead.
        return getDate(column(columnLabel));
    }

    private Long dateTimeAsMillis(int columnIndex) throws SQLException {
        Object val = column(columnIndex);
        EsType type = columnType(columnIndex);
        try {
            // TODO: the B6 appendix of the jdbc spec does mention CHAR, VARCHAR, LONGVARCHAR, DATE, TIMESTAMP as supported
            // jdbc types that should be handled by getDate and getTime methods. From all of those we support VARCHAR and
            // TIMESTAMP. Should we consider the VARCHAR conversion as a later enhancement?
            if (DATETIME == type) {
                // the cursor can return an Integer if the date-since-epoch is small enough, XContentParser (Jackson) will
                // return the "smallest" data type for numbers when parsing
                // TODO: this should probably be handled server side
                if (val == null) {
                    return null;
                }
                return asDateTimeField(val, JdbcDateUtils::dateTimeAsMillisSinceEpoch, Function.identity());
            }
            if (DATE == type) {
                return dateTimeAsMillisSinceEpoch(val.toString());
            }
            if (TIME == type) {
                return timeAsMillisSinceEpoch(val.toString());
            }
            return val == null ? null : (Long) val;
        } catch (ClassCastException cce) {
            throw new SQLException(
                    format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Long", val, type.getName()), cce);
        }
    }

    private Date asDate(int columnIndex) throws SQLException {
        Object val = column(columnIndex);

        if (val == null) {
            return null;
        }

        EsType type = columnType(columnIndex);
        if (type == TIME) {
            return new Date(0L);
        }

        try {

            return JdbcDateUtils.asDate(val.toString());
        } catch (Exception e) {
            throw new SQLException(
                format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Date", val, type.getName()), e);
        }
    }

    private Time asTime(int columnIndex) throws SQLException {
        Object val = column(columnIndex);

        if (val == null) {
            return null;
        }

        EsType type = columnType(columnIndex);
        if (type == DATE) {
            return new Time(0L);
        }

        try {
            if (type == TIME) {
                return timeAsTime(val.toString());
            }
            return JdbcDateUtils.asTime(val.toString());
        } catch (Exception e) {
            throw new SQLException(
                format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Time", val, type.getName()), e);
        }
    }

    private Timestamp asTimeStamp(int columnIndex) throws SQLException {
        Object val = column(columnIndex);

        if (val == null) {
            return null;
        }

        EsType type = columnType(columnIndex);
        try {
            if (val instanceof Number) {
                return asTimestamp(((Number) val).longValue());
            }
            if (type == TIME) {
                return timeAsTimestamp(val.toString());
            }
            return asTimestamp(val.toString());
        } catch (Exception e) {
            throw new SQLException(
                format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Timestamp", val, type.getName()), e);
        }
    }

    private Calendar safeCalendar(Calendar calendar) {
        return calendar == null ? defaultCalendar : calendar;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return TypeConverter.convertDate(dateTimeAsMillis(columnIndex), safeCalendar(cal));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(column(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        EsType type = columnType(columnIndex);
        if (type == DATE) {
            return new Time(0L);
        }
        return TypeConverter.convertTime(dateTimeAsMillis(columnIndex), safeCalendar(cal));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(column(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return TypeConverter.convertTimestamp(dateTimeAsMillis(columnIndex), safeCalendar(cal));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(column(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(column(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(column(columnLabel), cal);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new JdbcResultSetMetaData(this, cursor.columns());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return convert(columnIndex, null);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type is null");
        }

        return convert(columnIndex, type);
    }

    private <T> T convert(int columnIndex, Class<T> type) throws SQLException {
        Object val = column(columnIndex);

        if (val == null) {
            return null;
        }

        EsType columnType = columnType(columnIndex);
        String typeString = type != null ? type.getSimpleName() : columnType.getName();

        return TypeConverter.convert(val, columnType, type, typeString);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getObject(columnIndex);
        }
        throw new SQLFeatureNotSupportedException("getObject with non-empty Map not supported");
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(column(columnLabel));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(column(columnLabel), type);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(column(columnLabel), map);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return column(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isAfterLast not supported");
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isLast not supported");
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Rows is negative");
        }
        if (rows != getFetchSize()) {
            throw new SQLException("Fetch size cannot be changed");
        }
        // ignore fetch size since scrolls cannot be changed in flight
    }

    @Override
    public int getFetchSize() throws SQLException {
        /*
         * Instead of returning the fetch size the user requested we make a
         * stab at returning the fetch size that we actually used, returning
         * the batch size of the current row. This allows us to assert these
         * batch sizes in testing and lets us point users to something that
         * they can use for debugging.
         */
        checkOpen();
        return cursor.batchSize();
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkOpen();
        return statement;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor name not supported");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public int getType() throws SQLException {
        checkOpen();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkOpen();
        return CONCUR_READ_ONLY;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Fetch direction must be FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return FETCH_FORWARD;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString not supported");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString not supported");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s:row %d:cursor size %d:%s", getClass().getSimpleName(), rowNumber, cursor.batchSize(),
                cursor.columns());
    }
}
