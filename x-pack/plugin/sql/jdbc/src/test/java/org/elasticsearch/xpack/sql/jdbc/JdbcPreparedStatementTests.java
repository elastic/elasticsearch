/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static org.elasticsearch.xpack.sql.jdbc.EsType.BINARY;
import static org.elasticsearch.xpack.sql.jdbc.EsType.BOOLEAN;
import static org.elasticsearch.xpack.sql.jdbc.EsType.BYTE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DATETIME;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DOUBLE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.FLOAT;
import static org.elasticsearch.xpack.sql.jdbc.EsType.HALF_FLOAT;
import static org.elasticsearch.xpack.sql.jdbc.EsType.INTEGER;
import static org.elasticsearch.xpack.sql.jdbc.EsType.KEYWORD;
import static org.elasticsearch.xpack.sql.jdbc.EsType.LONG;
import static org.elasticsearch.xpack.sql.jdbc.EsType.SHORT;
import static org.elasticsearch.xpack.sql.jdbc.EsType.TIME;
import static org.elasticsearch.xpack.sql.jdbc.EsType.UNSIGNED_LONG;

public class JdbcPreparedStatementTests extends ESTestCase {

    public void testSettingBooleanValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        jps.setBoolean(1, true);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, false);
        assertEquals(false, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, true, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));
        assertTrue(value(jps) instanceof Boolean);

        jps.setObject(1, true, Types.INTEGER);
        assertEquals(1, value(jps));
        assertEquals(INTEGER, jdbcType(jps));

        jps.setObject(1, true, Types.VARCHAR);
        assertEquals("true", value(jps));
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingBooleanValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, true, Types.TIMESTAMP));
        assertEquals("Unable to convert value [true] of type [BOOLEAN] to [TIMESTAMP]", sqle.getMessage());
    }

    public void testSettingStringValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        jps.setString(1, "foo bar");
        assertEquals("foo bar", value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, "foo bar");
        assertEquals("foo bar", value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, "foo bar", Types.VARCHAR);
        assertEquals("foo bar", value(jps));
        assertEquals(KEYWORD, jdbcType(jps));
        assertTrue(value(jps) instanceof String);
    }

    public void testThrownExceptionsWhenSettingStringValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, "foo bar", Types.INTEGER));
        assertEquals("Unable to convert value [foo bar] of type [KEYWORD] to [INTEGER]", sqle.getMessage());
    }

    public void testSettingByteTypeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        jps.setByte(1, (byte) 6);
        assertEquals((byte) 6, value(jps));
        assertEquals(BYTE, jdbcType(jps));

        jps.setObject(1, (byte) 6);
        assertEquals((byte) 6, value(jps));
        assertEquals(BYTE, jdbcType(jps));
        assertTrue(value(jps) instanceof Byte);

        jps.setObject(1, (byte) 0, Types.BOOLEAN);
        assertEquals(false, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, (byte) 123, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, (byte) 123, Types.INTEGER);
        assertEquals(123, value(jps));
        assertEquals(INTEGER, jdbcType(jps));

        jps.setObject(1, (byte) 123, Types.BIGINT);
        assertEquals(123L, value(jps));
        assertEquals(LONG, jdbcType(jps));

        jps.setObject(1, (byte) 123, Types.BIGINT, 20);
        assertEquals(BigInteger.valueOf(123), value(jps));
        assertEquals(UNSIGNED_LONG, jdbcType(jps));

        jps.setObject(1, (byte) -128, Types.DOUBLE);
        assertEquals(-128.0, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingByteTypeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, (byte) 6, Types.TIMESTAMP));
        assertEquals("Unable to convert value [6] of type [BYTE] to [TIMESTAMP]", sqle.getMessage());
    }

    public void testSettingShortTypeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        short someShort = randomShort();
        jps.setShort(1, someShort);
        assertEquals(someShort, value(jps));
        assertEquals(SHORT, jdbcType(jps));

        jps.setObject(1, someShort);
        assertEquals(someShort, value(jps));
        assertEquals(SHORT, jdbcType(jps));
        assertTrue(value(jps) instanceof Short);

        jps.setObject(1, (short) 1, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, (short) -32700, Types.DOUBLE);
        assertEquals(-32700.0, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));

        jps.setObject(1, someShort, Types.INTEGER);
        assertEquals((int) someShort, value(jps));
        assertEquals(INTEGER, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingShortTypeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, (short) 6, Types.TIMESTAMP));
        assertEquals("Unable to convert value [6] of type [SHORT] to [TIMESTAMP]", sqle.getMessage());

        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, 256, Types.TINYINT));
        assertEquals("Numeric " + 256 + " out of range", sqle.getMessage());
    }

    public void testSettingIntegerValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        int someInt = randomInt();
        jps.setInt(1, someInt);
        assertEquals(someInt, value(jps));
        assertEquals(INTEGER, jdbcType(jps));

        jps.setObject(1, someInt);
        assertEquals(someInt, value(jps));
        assertEquals(INTEGER, jdbcType(jps));
        assertTrue(value(jps) instanceof Integer);

        jps.setObject(1, someInt, Types.VARCHAR);
        assertEquals(String.valueOf(someInt), value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, someInt, Types.FLOAT);
        assertEquals(Double.valueOf(someInt), value(jps));
        assertTrue(value(jps) instanceof Double);
        assertEquals(HALF_FLOAT, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingIntegerValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        int someInt = randomInt();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someInt, Types.TIMESTAMP));
        assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [INTEGER] to [TIMESTAMP]", someInt), sqle.getMessage());

        Integer randomIntNotShort = randomIntBetween(32768, Integer.MAX_VALUE);
        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, randomIntNotShort, Types.SMALLINT));
        assertEquals("Numeric " + randomIntNotShort + " out of range", sqle.getMessage());

        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, randomIntNotShort, Types.TINYINT));
        assertEquals("Numeric " + randomIntNotShort + " out of range", sqle.getMessage());
    }

    public void testSettingLongValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        long someLong = randomLong();
        jps.setLong(1, someLong);
        assertEquals(someLong, value(jps));
        assertEquals(LONG, jdbcType(jps));

        jps.setObject(1, someLong);
        assertEquals(someLong, value(jps));
        assertEquals(LONG, jdbcType(jps));
        assertTrue(value(jps) instanceof Long);

        jps.setObject(1, someLong, Types.VARCHAR);
        assertEquals(String.valueOf(someLong), value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, someLong, Types.DOUBLE);
        assertEquals((double) someLong, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));

        jps.setObject(1, someLong, Types.FLOAT);
        assertEquals((double) someLong, value(jps));
        assertEquals(HALF_FLOAT, jdbcType(jps));
    }

    public void testSettingBigIntegerValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        BigInteger bi = BigInteger.valueOf(randomLong()).abs();

        jps.setObject(1, bi);
        assertEquals(bi, value(jps));
        assertEquals(UNSIGNED_LONG, jdbcType(jps));
        assertTrue(value(jps) instanceof BigInteger);

        jps.setObject(1, bi, Types.VARCHAR);
        assertEquals(String.valueOf(bi), value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, bi, Types.BIGINT);
        assertEquals(bi.longValueExact(), value(jps));
        assertEquals(LONG, jdbcType(jps));

        jps.setObject(1, bi, Types.DOUBLE);
        assertEquals(bi.doubleValue(), value(jps));
        assertEquals(DOUBLE, jdbcType(jps));

        jps.setObject(1, bi, Types.FLOAT);
        assertEquals(bi.doubleValue(), value(jps));
        assertEquals(HALF_FLOAT, jdbcType(jps));

        jps.setObject(1, bi, Types.REAL);
        assertEquals(bi.floatValue(), value(jps));
        assertEquals(FLOAT, jdbcType(jps));

        jps.setObject(1, BigInteger.ZERO, Types.BOOLEAN);
        assertEquals(false, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, BigInteger.TEN, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));

        jps.setObject(1, bi.longValueExact(), JDBCType.BIGINT, 19);
        assertTrue(value(jps) instanceof Long);
        assertEquals(bi.longValueExact(), value(jps));
        assertEquals(LONG, jdbcType(jps));

        jps.setObject(1, bi.longValueExact(), JDBCType.BIGINT, 20);
        assertTrue(value(jps) instanceof BigInteger);
        assertEquals(bi, value(jps));
        assertEquals(UNSIGNED_LONG, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingLongValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        long someLong = randomLong();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someLong, Types.TIMESTAMP));
        assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [LONG] to [TIMESTAMP]", someLong), sqle.getMessage());

        Long randomLongNotShort = randomLongBetween(Integer.MAX_VALUE + 1, Long.MAX_VALUE);
        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, randomLongNotShort, Types.INTEGER));
        assertEquals("Numeric " + randomLongNotShort + " out of range", sqle.getMessage());

        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, randomLongNotShort, Types.SMALLINT));
        assertEquals("Numeric " + randomLongNotShort + " out of range", sqle.getMessage());
    }

    public void testSettingFloatValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        float someFloat = randomFloat();
        jps.setFloat(1, someFloat);
        assertEquals(someFloat, value(jps));
        assertEquals(FLOAT, jdbcType(jps));

        jps.setObject(1, someFloat);
        assertEquals(someFloat, value(jps));
        assertEquals(FLOAT, jdbcType(jps));
        assertTrue(value(jps) instanceof Float);

        jps.setObject(1, someFloat, Types.VARCHAR);
        assertEquals(String.valueOf(someFloat), value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, someFloat, Types.DOUBLE);
        assertEquals((double) someFloat, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));

        jps.setObject(1, someFloat, Types.FLOAT);
        assertEquals((double) someFloat, value(jps));
        assertEquals(HALF_FLOAT, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingFloatValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        float someFloat = randomFloat();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someFloat, Types.TIMESTAMP));
        assertEquals(format(Locale.ROOT, "Unable to convert value [%.128s] of type [FLOAT] to [TIMESTAMP]", someFloat), sqle.getMessage());

        Float floatNotInt = 5_155_000_000f;
        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, floatNotInt, Types.INTEGER));
        assertEquals(LoggerMessageFormat.format("Numeric {} out of range", Math.round(floatNotInt.doubleValue())), sqle.getMessage());

        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, floatNotInt, Types.SMALLINT));
        assertEquals(LoggerMessageFormat.format("Numeric {} out of range", Math.round(floatNotInt.doubleValue())), sqle.getMessage());
    }

    public void testSettingDoubleValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        double someDouble = randomDouble();
        jps.setDouble(1, someDouble);
        assertEquals(someDouble, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));

        jps.setObject(1, someDouble);
        assertEquals(someDouble, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        assertTrue(value(jps) instanceof Double);

        jps.setObject(1, someDouble, Types.VARCHAR);
        assertEquals(String.valueOf(someDouble), value(jps));
        assertEquals(KEYWORD, jdbcType(jps));

        jps.setObject(1, someDouble, Types.REAL);
        assertEquals((float) someDouble, value(jps));
        assertEquals(FLOAT, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingDoubleValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        double someDouble = randomDouble();

        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someDouble, Types.TIMESTAMP));
        assertEquals(
            format(Locale.ROOT, "Unable to convert value [%.128s] of type [DOUBLE] to [TIMESTAMP]", someDouble),
            sqle.getMessage()
        );

        Double doubleNotInt = 5_155_000_000d;
        sqle = expectThrows(SQLException.class, () -> jps.setObject(1, doubleNotInt, Types.INTEGER));
        assertEquals(LoggerMessageFormat.format("Numeric {} out of range", ((Number) doubleNotInt).longValue()), sqle.getMessage());
    }

    public void testUnsupportedClasses() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        SQLFeatureNotSupportedException sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return null;
            }
        }));
        assertEquals("Objects of type [java.sql.Struct] are not supported", sfnse.getMessage());

        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, new URL("http://test")));
        assertEquals("Objects of type [java.net.URL] are not supported", sfnse.getMessage());

        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setURL(1, new URL("http://test")));
        assertEquals("Objects of type [java.net.URL] are not supported", sfnse.getMessage());

        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, this, Types.TIMESTAMP));
        assertEquals("Conversion from type [" + this.getClass().getName() + "] to [TIMESTAMP] not supported", sfnse.getMessage());

        SQLException se = expectThrows(SQLException.class, () -> jps.setObject(1, this, 1_000_000));
        assertEquals("Unsupported SQL type [1000000]", se.getMessage());

        SQLFeatureNotSupportedException iae = expectThrows(
            SQLFeatureNotSupportedException.class,
            () -> jps.setObject(1, randomShort(), Types.CHAR)
        );
        assertEquals("Unsupported SQL type [CHAR]", iae.getMessage());
    }

    public void testSettingTimestampValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        int randomNanos = randomInt(999999999);

        Timestamp someTimestamp = new Timestamp(randomLong());
        someTimestamp.setNanos(randomNanos);
        jps.setTimestamp(1, someTimestamp);
        assertEquals(someTimestamp.getTime(), ((ZonedDateTime) value(jps)).toInstant().toEpochMilli());
        assertEquals(someTimestamp.getNanos(), ((ZonedDateTime) value(jps)).getNano());
        assertEquals(DATETIME, jdbcType(jps));

        Calendar nonDefaultCal = randomCalendar();
        // February 29th, 2016. 01:17:55 GMT = 1456708675000 millis since epoch
        Timestamp someTimestamp2 = new Timestamp(1456708675000L);
        someTimestamp2.setNanos(randomNanos);
        jps.setTimestamp(1, someTimestamp2, nonDefaultCal);
        ZonedDateTime zdt = (ZonedDateTime) value(jps);
        assertEquals(someTimestamp2.getTime(), convertFromUTCtoCalendar(zdt, nonDefaultCal));
        assertEquals(someTimestamp2.getNanos(), zdt.getNano());
        assertEquals(DATETIME, jdbcType(jps));

        long beforeEpochTime = randomLongBetween(Long.MIN_VALUE, 0);
        Timestamp someTimestamp3 = new Timestamp(beforeEpochTime);
        someTimestamp3.setNanos(randomNanos);
        jps.setTimestamp(1, someTimestamp3, nonDefaultCal);
        zdt = (ZonedDateTime) value(jps);
        assertEquals(someTimestamp3.getTime(), convertFromUTCtoCalendar(zdt, nonDefaultCal));
        assertEquals(someTimestamp3.getNanos(), zdt.getNano());

        jps.setObject(1, someTimestamp, Types.VARCHAR);
        assertEquals(someTimestamp.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingTimestampValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Timestamp someTimestamp = new Timestamp(randomLong());

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someTimestamp, Types.INTEGER));
        assertEquals("Conversion from type [java.sql.Timestamp] to [INTEGER] not supported", sqle.getMessage());
    }

    public void testSettingTimeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        Time time = new Time(4675000);
        jps.setTime(1, time);
        assertEquals(time, value(jps));
        assertEquals(TIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        Calendar nonDefaultCal = randomCalendar();
        jps.setTime(1, time, nonDefaultCal);
        assertEquals(4675000, convertFromUTCtoCalendar(((Date) value(jps)), nonDefaultCal));
        assertEquals(TIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        jps.setObject(1, time, Types.VARCHAR);
        assertEquals(time.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingTimeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Time time = new Time(4675000);

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, time, Types.INTEGER));
        assertEquals("Conversion from type [java.sql.Time] to [INTEGER] not supported", sqle.getMessage());
    }

    public void testSettingSqlDateValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        java.sql.Date someSqlDate = new java.sql.Date(randomLong());
        jps.setDate(1, someSqlDate);
        assertEquals(someSqlDate.getTime(), ((Date) value(jps)).getTime());
        assertEquals(DATETIME, jdbcType(jps));

        someSqlDate = new java.sql.Date(randomLong());
        Calendar nonDefaultCal = randomCalendar();
        jps.setDate(1, someSqlDate, nonDefaultCal);
        assertEquals(someSqlDate.getTime(), convertFromUTCtoCalendar(((Date) value(jps)), nonDefaultCal));
        assertEquals(DATETIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        jps.setObject(1, someSqlDate, Types.VARCHAR);
        assertEquals(someSqlDate.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingSqlDateValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        java.sql.Date someSqlDate = new java.sql.Date(randomLong());

        SQLException sqle = expectThrows(
            SQLFeatureNotSupportedException.class,
            () -> jps.setObject(1, new java.sql.Date(randomLong()), Types.DOUBLE)
        );
        assertEquals("Conversion from type [" + someSqlDate.getClass().getName() + "] to [DOUBLE] not supported", sqle.getMessage());
    }

    public void testSettingCalendarValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Calendar someCalendar = randomCalendar();
        someCalendar.setTimeInMillis(randomLong());

        jps.setObject(1, someCalendar);
        assertEquals(someCalendar.getTime(), value(jps));
        assertEquals(DATETIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        jps.setObject(1, someCalendar, Types.VARCHAR);
        assertEquals(someCalendar.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));

        Calendar nonDefaultCal = randomCalendar();
        jps.setObject(1, nonDefaultCal);
        assertEquals(nonDefaultCal.getTime(), value(jps));
        assertEquals(DATETIME, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingCalendarValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Calendar someCalendar = randomCalendar();

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someCalendar, Types.DOUBLE));
        assertEquals("Conversion from type [" + someCalendar.getClass().getName() + "] to [DOUBLE] not supported", sqle.getMessage());
    }

    public void testSettingDateValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Date someDate = new Date(randomLong());

        jps.setObject(1, someDate);
        assertEquals(someDate, value(jps));
        assertEquals(DATETIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        jps.setObject(1, someDate, Types.VARCHAR);
        assertEquals(someDate.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingDateValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        Date someDate = new Date(randomLong());

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someDate, Types.BIGINT));
        assertEquals("Conversion from type [" + someDate.getClass().getName() + "] to [BIGINT] not supported", sqle.getMessage());
    }

    public void testSettingLocalDateTimeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        LocalDateTime ldt = LocalDateTime.now(Clock.systemDefaultZone());

        jps.setObject(1, ldt);
        assertEquals(Date.class, value(jps).getClass());
        assertEquals(DATETIME, jdbcType(jps));
        assertTrue(value(jps) instanceof java.util.Date);

        jps.setObject(1, ldt, Types.VARCHAR);
        assertEquals(ldt.toString(), value(jps).toString());
        assertEquals(KEYWORD, jdbcType(jps));
    }

    public void testThrownExceptionsWhenSettingLocalDateTimeValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        LocalDateTime ldt = LocalDateTime.now(Clock.systemDefaultZone());

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, ldt, Types.BIGINT));
        assertEquals("Conversion from type [" + ldt.getClass().getName() + "] to [BIGINT] not supported", sqle.getMessage());
    }

    public void testSettingByteArrayValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();

        byte[] buffer = "some data".getBytes(StandardCharsets.UTF_8);
        jps.setBytes(1, buffer);
        assertEquals(byte[].class, value(jps).getClass());
        assertEquals(BINARY, jdbcType(jps));

        jps.setObject(1, buffer);
        assertEquals(byte[].class, value(jps).getClass());
        assertEquals(BINARY, jdbcType(jps));
        assertTrue(value(jps) instanceof byte[]);

        jps.setObject(1, buffer, Types.VARBINARY);
        assertEquals(value(jps), buffer);
        assertEquals(BINARY, jdbcType(jps));

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, Types.VARCHAR));
        assertEquals("Conversion from type [byte[]] to [VARCHAR] not supported", sqle.getMessage());

        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, Types.DOUBLE));
        assertEquals("Conversion from type [byte[]] to [DOUBLE] not supported", sqle.getMessage());
    }

    public void testThrownExceptionsWhenSettingByteArrayValues() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        byte[] buffer = "foo".getBytes(StandardCharsets.UTF_8);

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, Types.VARCHAR));
        assertEquals("Conversion from type [byte[]] to [VARCHAR] not supported", sqle.getMessage());

        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, Types.DOUBLE));
        assertEquals("Conversion from type [byte[]] to [DOUBLE] not supported", sqle.getMessage());
    }

    public void testThrownExceptionsWhenSettingByteArrayValuesToEsTypes() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        byte[] buffer = "foo".getBytes(StandardCharsets.UTF_8);

        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, EsType.KEYWORD));
        assertEquals("Conversion from type [byte[]] to [KEYWORD] not supported", sqle.getMessage());

        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, buffer, EsType.INTERVAL_DAY_TO_MINUTE));
        assertEquals("Conversion from type [byte[]] to [INTERVAL_DAY_TO_MINUTE] not supported", sqle.getMessage());
    }

    private JdbcPreparedStatement createJdbcPreparedStatement() throws SQLException {
        return new JdbcPreparedStatement(new JdbcConnection(JdbcConfiguration.create("jdbc:es://l:1", null, 0), false), "?");
    }

    private EsType jdbcType(JdbcPreparedStatement jps) throws SQLException {
        return jps.query.getParam(1).type;
    }

    private Object value(JdbcPreparedStatement jps) throws SQLException {
        return jps.query.getParam(1).value;
    }

    private Calendar randomCalendar() {
        return Calendar.getInstance(randomTimeZone(), Locale.ROOT);
    }

    /**
     * @see #convertFromUTCtoCalendar(ZonedDateTime, Calendar)
     */
    private long convertFromUTCtoCalendar(Date date, Calendar nonDefaultCal) {
        return convertFromUTCtoCalendar(ZonedDateTime.ofInstant(date.toInstant(), UTC), nonDefaultCal);
    }

    /**
     * Converts from UTC to the provided Calendar.
     * Helps checking if the converted date/time values using Calendars in set*(...,Calendar) methods did convert
     * the values correctly to UTC.
     */
    private long convertFromUTCtoCalendar(ZonedDateTime zdt, Calendar nonDefaultCal) {
        return zdt.withZoneSameInstant(UTC).withZoneSameLocal(nonDefaultCal.getTimeZone().toZoneId()).toInstant().toEpochMilli();
    }
}
