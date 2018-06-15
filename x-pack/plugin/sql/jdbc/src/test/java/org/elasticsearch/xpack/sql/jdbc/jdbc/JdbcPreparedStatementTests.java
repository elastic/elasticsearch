/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.net.URL;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARCHAR;

public class JdbcPreparedStatementTests extends ESTestCase {
    
    public void testBooleanSetters() throws SQLException {
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
        
        jps.setObject(1, true, Types.INTEGER);
        assertEquals(1, value(jps));
        assertEquals(INTEGER, jdbcType(jps));
        
        jps.setObject(1, true, Types.VARCHAR);
        assertEquals("true", value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, true, Types.TIMESTAMP));
        assertEquals("Conversion from type [BOOLEAN] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testStringSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        jps.setString(1, "foo bar");
        assertEquals("foo bar", value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, "foo bar");
        assertEquals("foo bar", value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, "foo bar", Types.VARCHAR);
        assertEquals("foo bar", value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, "foo bar", Types.INTEGER));
        assertEquals("Conversion from type [VARCHAR] to [Integer] not supported", sqle.getMessage());
    }
    
    public void testByteSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        jps.setByte(1, (byte) 6);
        assertEquals((byte) 6, value(jps));
        assertEquals(TINYINT, jdbcType(jps));
        
        jps.setObject(1, (byte) 6);
        assertEquals((byte) 6, value(jps));
        assertEquals(TINYINT, jdbcType(jps));
        
        jps.setObject(1, (byte) 0, Types.BOOLEAN);
        assertEquals(false, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));
        
        jps.setObject(1, (byte) 123, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, (byte) 6, Types.TIMESTAMP));
        assertEquals("Conversion from type [TINYINT] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testShortSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        jps.setShort(1, (short) 7);
        assertEquals((short) 7, value(jps));
        assertEquals(SMALLINT, jdbcType(jps));
        
        jps.setObject(1, (short) 7);
        assertEquals((short) 7, value(jps));
        assertEquals(SMALLINT, jdbcType(jps));
        
        jps.setObject(1, (short) 1, Types.BOOLEAN);
        assertEquals(true, value(jps));
        assertEquals(BOOLEAN, jdbcType(jps));
        
        jps.setObject(1, (short) 123, Types.DOUBLE);
        assertEquals(123.0, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, (short) 6, Types.TIMESTAMP));
        assertEquals("Conversion from type [SMALLINT] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testIntegerSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        int someInt = randomInt();
        jps.setInt(1, someInt);
        assertEquals(someInt, value(jps));
        assertEquals(INTEGER, jdbcType(jps));
        
        jps.setObject(1, someInt);
        assertEquals(someInt, value(jps));
        assertEquals(INTEGER, jdbcType(jps));
        
        jps.setObject(1, someInt, Types.VARCHAR);
        assertEquals(String.valueOf(someInt), value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, someInt, Types.FLOAT);
        assertEquals(Double.valueOf(someInt), value(jps));
        assertTrue(value(jps) instanceof Double);
        assertEquals(FLOAT, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someInt, Types.TIMESTAMP));
        assertEquals("Conversion from type [INTEGER] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testLongSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        long someLong = randomLong();
        jps.setLong(1, someLong);
        assertEquals(someLong, value(jps));
        assertEquals(BIGINT, jdbcType(jps));
        
        jps.setObject(1, someLong);
        assertEquals(someLong, value(jps));
        assertEquals(BIGINT, jdbcType(jps));
        
        jps.setObject(1, someLong, Types.VARCHAR);
        assertEquals(String.valueOf(someLong), value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, someLong, Types.DOUBLE);
        assertEquals(new Double(someLong), value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someLong, Types.TIMESTAMP));
        assertEquals("Conversion from type [BIGINT] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testFloatSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        float someFloat = randomFloat();
        jps.setFloat(1, someFloat);
        assertEquals(someFloat, value(jps));
        assertEquals(REAL, jdbcType(jps));
        
        jps.setObject(1, someFloat);
        assertEquals(someFloat, value(jps));
        assertEquals(REAL, jdbcType(jps));
        
        jps.setObject(1, someFloat, Types.VARCHAR);
        assertEquals(String.valueOf(someFloat), value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, someFloat, Types.DOUBLE);
        assertEquals(new Double(someFloat), value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someFloat, Types.TIMESTAMP));
        assertEquals("Conversion from type [REAL] to [Timestamp] not supported", sqle.getMessage());
    }
    
    public void testDoubleSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        double someDouble = randomDouble();
        jps.setDouble(1, someDouble);
        assertEquals(someDouble, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        
        jps.setObject(1, someDouble);
        assertEquals(someDouble, value(jps));
        assertEquals(DOUBLE, jdbcType(jps));
        
        jps.setObject(1, someDouble, Types.VARCHAR);
        assertEquals(String.valueOf(someDouble), value(jps));
        assertEquals(VARCHAR, jdbcType(jps));
        
        jps.setObject(1, someDouble, Types.REAL);
        assertEquals(new Float(someDouble), value(jps));
        assertEquals(REAL, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLException.class, () -> jps.setObject(1, someDouble, Types.TIMESTAMP));
        assertEquals("Conversion from type [DOUBLE] to [Timestamp] not supported", sqle.getMessage());
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
        assertEquals("Objects of type java.sql.Struct are not supported", sfnse.getMessage());
        
        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, new URL("http://test")));
        assertEquals("Objects of type java.net.URL are not supported", sfnse.getMessage());
        
        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setURL(1, new URL("http://test")));
        assertEquals("Objects of type java.net.URL are not supported", sfnse.getMessage());
        
        sfnse = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, this, Types.TIMESTAMP));
        assertEquals("Conversion from type " + this.getClass().getName() + " to TIMESTAMP not supported", sfnse.getMessage());
        
        SQLException se = expectThrows(SQLException.class, () -> jps.setObject(1, this, 1_000_000));
        assertEquals("Type:1000000 is not a valid Types.java value.", se.getMessage());
    }
    
    public void testDateTimeSetters() throws SQLException {
        JdbcPreparedStatement jps = createJdbcPreparedStatement();
        
        // Timestamp
        Timestamp someTimestamp = new Timestamp(randomMillisSinceEpoch());
        jps.setTimestamp(1, someTimestamp);
        assertEquals(someTimestamp.getTime(), ((Date)value(jps)).getTime());
        assertEquals(TIMESTAMP, jdbcType(jps));
        
        jps.setObject(1, someTimestamp, Types.VARCHAR);
        assertEquals(someTimestamp.toString(), value(jps).toString());
        assertEquals(VARCHAR, jdbcType(jps));
        
        SQLException sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someTimestamp, Types.INTEGER));
        assertEquals("Conversion from type java.sql.Timestamp to INTEGER not supported", sqle.getMessage());
        
        // Calendar
        Calendar someCalendar = Calendar.getInstance();
        someCalendar.setTimeInMillis(randomMillisSinceEpoch());
        
        jps.setObject(1, someCalendar);
        assertEquals(someCalendar.getTime(), (Date) value(jps));
        assertEquals(TIMESTAMP, jdbcType(jps));
        
        jps.setObject(1, someCalendar, Types.VARCHAR);
        assertEquals(someCalendar.toString(), value(jps).toString());
        assertEquals(VARCHAR, jdbcType(jps));
        
        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someCalendar, Types.DOUBLE));
        assertEquals("Conversion from type " + someCalendar.getClass().getName() + " to DOUBLE not supported", sqle.getMessage());
        
        Calendar nonDefaultCal = Calendar.getInstance(randomTimeZone());
        jps.setObject(1, nonDefaultCal);
        assertEquals(nonDefaultCal.getTime(), (Date) value(jps));
        assertEquals(TIMESTAMP, jdbcType(jps));
        
        // java.util.Date
        Date someDate = new Date(randomMillisSinceEpoch());
        
        jps.setObject(1, someDate);
        assertEquals(someDate, (Date) value(jps));
        assertEquals(TIMESTAMP, jdbcType(jps));
        
        jps.setObject(1, someDate, Types.VARCHAR);
        assertEquals(someDate.toString(), value(jps).toString());
        assertEquals(VARCHAR, jdbcType(jps));
        
        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, someDate, Types.BIGINT));
        assertEquals("Conversion from type " + someDate.getClass().getName() + " to BIGINT not supported", sqle.getMessage());

        // java.time.LocalDateTime
        LocalDateTime ldt = LocalDateTime.now();
        
        jps.setObject(1, ldt);
        assertEquals(Date.class, value(jps).getClass());
        assertEquals(TIMESTAMP, jdbcType(jps));
        
        jps.setObject(1, ldt, Types.VARCHAR);
        assertEquals(ldt.toString(), value(jps).toString());
        assertEquals(VARCHAR, jdbcType(jps));
        
        sqle = expectThrows(SQLFeatureNotSupportedException.class, () -> jps.setObject(1, ldt, Types.BIGINT));
        assertEquals("Conversion from type " + ldt.getClass().getName() + " to BIGINT not supported", sqle.getMessage());
    }

    private long randomMillisSinceEpoch() {
        return randomLongBetween(0, System.currentTimeMillis());
    }

    private JdbcPreparedStatement createJdbcPreparedStatement() throws SQLException {
        return new JdbcPreparedStatement(null, JdbcConfiguration.create("jdbc:es://l:1", null, 0), "?");
    }

    private JDBCType jdbcType(JdbcPreparedStatement jps) throws SQLException {
        return jps.query.getParam(1).type;
    }

    private Object value(JdbcPreparedStatement jps) throws SQLException {
        return jps.query.getParam(1).value;
    }
}
