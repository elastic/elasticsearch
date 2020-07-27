/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Locale;
import java.util.StringJoiner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public abstract class PreparedStatementTestCase extends JdbcIntegrationTestCase {

    public void testSupportedTypes() throws SQLException {
        String stringVal = randomAlphaOfLength(randomIntBetween(0, 1000));
        int intVal = randomInt();
        long longVal = randomLong();
        double doubleVal = randomDouble();
        float floatVal = randomFloat();
        boolean booleanVal = randomBoolean();
        byte byteVal = randomByte();
        short shortVal = randomShort();
        BigDecimal bigDecimalVal = BigDecimal.valueOf(randomDouble());
        long millis = randomNonNegativeLong();
        Calendar calendarVal = Calendar.getInstance(randomTimeZone(), Locale.ROOT);
        Timestamp timestampVal = new Timestamp(millis);
        Timestamp timestampValWithCal = new Timestamp(JdbcTestUtils.convertFromCalendarToUTC(timestampVal.getTime(), calendarVal));
        Date dateVal = JdbcTestUtils.asDate(millis, JdbcTestUtils.UTC);
        Date dateValWithCal = JdbcTestUtils.asDate(
            JdbcTestUtils.convertFromCalendarToUTC(dateVal.getTime(), calendarVal),
            JdbcTestUtils.UTC
        );
        Time timeVal = JdbcTestUtils.asTime(millis, JdbcTestUtils.UTC);
        Time timeValWithCal = JdbcTestUtils.asTime(
            JdbcTestUtils.convertFromCalendarToUTC(timeVal.getTime(), calendarVal),
            JdbcTestUtils.UTC
        );
        java.util.Date utilDateVal = new java.util.Date(millis);
        LocalDateTime localDateTimeVal = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), JdbcTestUtils.UTC);

        try (Connection connection = esJdbc()) {
            StringJoiner sql = new StringJoiner(",", "SELECT ", "");
            for (int i = 0; i < 19; i++) {
                sql.add("?");
            }
            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                statement.setString(1, stringVal);
                statement.setInt(2, intVal);
                statement.setLong(3, longVal);
                statement.setFloat(4, floatVal);
                statement.setDouble(5, doubleVal);
                statement.setNull(6, JDBCType.DOUBLE.getVendorTypeNumber());
                statement.setBoolean(7, booleanVal);
                statement.setByte(8, byteVal);
                statement.setShort(9, shortVal);
                statement.setBigDecimal(10, bigDecimalVal);
                statement.setTimestamp(11, timestampVal);
                statement.setTimestamp(12, timestampVal, calendarVal);
                statement.setDate(13, dateVal);
                statement.setDate(14, dateVal, calendarVal);
                statement.setTime(15, timeVal);
                statement.setTime(16, timeVal, calendarVal);
                statement.setObject(17, calendarVal);
                statement.setObject(18, utilDateVal);
                statement.setObject(19, localDateTimeVal);

                try (ResultSet results = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = results.getMetaData();
                    ParameterMetaData parameterMetaData = statement.getParameterMetaData();
                    assertEquals(resultSetMetaData.getColumnCount(), parameterMetaData.getParameterCount());
                    for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                        // Makes sure that column types survived the round trip
                        assertEquals(parameterMetaData.getParameterType(i), resultSetMetaData.getColumnType(i));
                    }
                    assertTrue(results.next());
                    assertEquals(stringVal, results.getString(1));
                    assertEquals(intVal, results.getInt(2));
                    assertEquals(longVal, results.getLong(3));
                    assertEquals(floatVal, results.getFloat(4), 0.00001f);
                    assertEquals(doubleVal, results.getDouble(5), 0.00001f);
                    assertNull(results.getObject(6));
                    assertEquals(booleanVal, results.getBoolean(7));
                    assertEquals(byteVal, results.getByte(8));
                    assertEquals(shortVal, results.getShort(9));
                    assertEquals(bigDecimalVal, results.getBigDecimal(10));
                    assertEquals(timestampVal, results.getTimestamp(11));
                    assertEquals(timestampValWithCal, results.getTimestamp(12));
                    assertEquals(dateVal, results.getDate(13));
                    assertEquals(dateValWithCal, results.getDate(14));
                    assertEquals(timeVal, results.getTime(15));
                    assertEquals(timeValWithCal, results.getTime(16));
                    assertEquals(new Timestamp(calendarVal.getTimeInMillis()), results.getObject(17));
                    assertEquals(timestampVal, results.getObject(18));
                    assertEquals(timestampVal, results.getObject(19));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testDatetime() throws IOException, SQLException {
        long randomMillis = randomNonNegativeLong();
        setupIndexForDateTimeTests(randomMillis);

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, birth_date FROM emps WHERE birth_date = ?")) {
                Object dateTimeParam = randomFrom(new Timestamp(randomMillis), new Date(randomMillis));
                statement.setObject(1, dateTimeParam);
                try (ResultSet results = statement.executeQuery()) {
                    assertTrue(results.next());
                    assertEquals(1002, results.getInt(1));
                    assertEquals(new Timestamp(randomMillis), results.getTimestamp(2));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testDate() throws IOException, SQLException {
        long randomMillis = randomNonNegativeLong();
        setupIndexForDateTimeTests(randomMillis);

        try (Connection connection = esJdbc()) {
            try (
                PreparedStatement statement = connection.prepareStatement(
                    "SELECT id, birth_date FROM emps WHERE birth_date::date = ? " + "ORDER BY id"
                )
            ) {

                statement.setDate(1, new Date(JdbcTestUtils.asDate(randomMillis, JdbcTestUtils.UTC).getTime()));
                try (ResultSet results = statement.executeQuery()) {
                    for (int i = 1; i <= 3; i++) {
                        assertTrue(results.next());
                        assertEquals(1000 + i, results.getInt(1));
                        assertEquals(new Timestamp(testMillis(randomMillis, i)), results.getTimestamp(2));
                    }
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testTime() throws IOException, SQLException {
        long randomMillis = randomNonNegativeLong();
        setupIndexForDateTimeTests(randomMillis);

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, birth_date FROM emps WHERE birth_date::time = ?")) {
                Time time = JdbcTestUtils.asTime(randomMillis, JdbcTestUtils.UTC);
                statement.setObject(1, time);
                try (ResultSet results = statement.executeQuery()) {
                    assertTrue(results.next());
                    assertEquals(1002, results.getInt(1));
                    assertEquals(new Timestamp(randomMillis), results.getTimestamp(2));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testOutOfRangeBigDecimal() throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                BigDecimal tooLarge = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.ONE);
                SQLException ex = expectThrows(SQLException.class, () -> statement.setBigDecimal(1, tooLarge));
                assertThat(ex.getMessage(), equalTo("BigDecimal value [" + tooLarge + "] out of supported double's range."));
            }
        }
    }

    public void testUnsupportedParameterUse() throws IOException, SQLException {
        index("library", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });

        try (Connection connection = esJdbc()) {
            // This is the current limitation of JDBC parser that it cannot detect improper use of '?'
            try (PreparedStatement statement = connection.prepareStatement("SELECT name FROM ? WHERE page_count=?")) {
                statement.setString(1, "library");
                statement.setInt(2, 1072);
                SQLSyntaxErrorException exception = expectThrows(SQLSyntaxErrorException.class, statement::executeQuery);
                assertThat(exception.getMessage(), startsWith("line 1:18: mismatched input '?' expecting "));

            }
        }
    }

    public void testTooMayParameters() throws IOException, SQLException {
        index("library", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT name FROM library WHERE page_count=?")) {
                statement.setInt(1, 1072);
                int tooBig = randomIntBetween(2, 10);
                SQLException tooBigEx = expectThrows(SQLException.class, () -> statement.setInt(tooBig, 1072));
                assertThat(tooBigEx.getMessage(), startsWith("Invalid parameter index ["));
                int tooSmall = randomIntBetween(-10, 0);
                SQLException tooSmallEx = expectThrows(SQLException.class, () -> statement.setInt(tooSmall, 1072));
                assertThat(tooSmallEx.getMessage(), startsWith("Invalid parameter index ["));
            }
        }
    }

    public void testStringEscaping() throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?, ?, ?, ?")) {
                statement.setString(1, "foo --");
                statement.setString(2, "/* foo */");
                statement.setString(3, "\"foo");
                statement.setString(4, "'foo'");
                try (ResultSet results = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = results.getMetaData();
                    assertEquals(4, resultSetMetaData.getColumnCount());
                    for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                        assertEquals(JDBCType.VARCHAR.getVendorTypeNumber().intValue(), resultSetMetaData.getColumnType(i));
                    }
                    assertTrue(results.next());
                    assertEquals("foo --", results.getString(1));
                    assertEquals("/* foo */", results.getString(2));
                    assertEquals("\"foo", results.getString(3));
                    assertEquals("'foo'", results.getString(4));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testCommentsHandling() throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?, /* ?, */ ? -- ?")) {
                assertEquals(2, statement.getParameterMetaData().getParameterCount());
                statement.setString(1, "foo");
                statement.setString(2, "bar");
                try (ResultSet results = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = results.getMetaData();
                    assertEquals(2, resultSetMetaData.getColumnCount());
                    assertTrue(results.next());
                    assertEquals("foo", results.getString(1));
                    assertEquals("bar", results.getString(2));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testSingleParameterMultipleTypes() throws SQLException {
        String stringVal = randomAlphaOfLength(randomIntBetween(0, 1000));
        int intVal = randomInt();
        long longVal = randomLong();
        double doubleVal = randomDouble();
        float floatVal = randomFloat();
        boolean booleanVal = randomBoolean();
        byte byteVal = randomByte();
        short shortVal = randomShort();

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {

                statement.setString(1, stringVal);
                assertEquals(new Tuple<>(JDBCType.VARCHAR.getVendorTypeNumber(), stringVal), execute(statement));
                statement.setInt(1, intVal);
                assertEquals(new Tuple<>(JDBCType.INTEGER.getVendorTypeNumber(), intVal), execute(statement));
                statement.setLong(1, longVal);
                assertEquals(new Tuple<>(JDBCType.BIGINT.getVendorTypeNumber(), longVal), execute(statement));
                statement.setFloat(1, floatVal);
                assertEquals(new Tuple<>(JDBCType.REAL.getVendorTypeNumber(), floatVal), execute(statement));
                statement.setDouble(1, doubleVal);
                assertEquals(new Tuple<>(JDBCType.DOUBLE.getVendorTypeNumber(), doubleVal), execute(statement));
                statement.setNull(1, JDBCType.DOUBLE.getVendorTypeNumber());
                assertEquals(new Tuple<>(JDBCType.DOUBLE.getVendorTypeNumber(), null), execute(statement));
                statement.setBoolean(1, booleanVal);
                assertEquals(new Tuple<>(JDBCType.BOOLEAN.getVendorTypeNumber(), booleanVal), execute(statement));
                statement.setByte(1, byteVal);
                assertEquals(new Tuple<>(JDBCType.TINYINT.getVendorTypeNumber(), byteVal), execute(statement));
                statement.setShort(1, shortVal);
                assertEquals(new Tuple<>(JDBCType.SMALLINT.getVendorTypeNumber(), shortVal), execute(statement));
            }
        }
    }

    private Tuple<Integer, Object> execute(PreparedStatement statement) throws SQLException {
        try (ResultSet results = statement.executeQuery()) {
            ResultSetMetaData resultSetMetaData = results.getMetaData();
            assertTrue(results.next());
            Tuple<Integer, Object> result = new Tuple<>(resultSetMetaData.getColumnType(1), results.getObject(1));
            assertFalse(results.next());
            return result;
        }
    }

    private static long testMillis(long randomMillis, int i) {
        return randomMillis - 2 + i;
    }

    private static void setupIndexForDateTimeTests(long randomMillis) throws IOException {
        String mapping = "\"properties\":{\"id\":{\"type\":\"integer\"},\"birth_date\":{\"type\":\"date\"}}";
        createIndex("emps", Settings.EMPTY, mapping);
        for (int i = 1; i <= 3; i++) {
            int id = 1000 + i;
            long testMillis = testMillis(randomMillis, i);
            index("emps", "" + i, builder -> {
                builder.field("id", id);
                builder.field("birth_date", testMillis);
            });
        }
    }
}
