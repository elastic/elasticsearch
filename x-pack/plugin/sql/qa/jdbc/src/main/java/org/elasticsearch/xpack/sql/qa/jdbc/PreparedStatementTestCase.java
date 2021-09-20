/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;

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

import static org.elasticsearch.common.time.DateUtils.toMilliSeconds;
import static org.elasticsearch.xpack.sql.jdbc.EsType.BOOLEAN;
import static org.elasticsearch.xpack.sql.jdbc.EsType.BYTE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.DOUBLE;
import static org.elasticsearch.xpack.sql.jdbc.EsType.FLOAT;
import static org.elasticsearch.xpack.sql.jdbc.EsType.INTEGER;
import static org.elasticsearch.xpack.sql.jdbc.EsType.KEYWORD;
import static org.elasticsearch.xpack.sql.jdbc.EsType.LONG;
import static org.elasticsearch.xpack.sql.jdbc.EsType.SHORT;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.JDBC_DRIVER_VERSION;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.extractNanosOnly;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.randomTimeInNanos;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.versionSupportsDateNanos;
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
        long millis = randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999);
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
                int index = 1;
                statement.setString(index++, stringVal);
                statement.setInt(index++, intVal);
                statement.setLong(index++, longVal);
                statement.setFloat(index++, floatVal);
                statement.setDouble(index++, doubleVal);
                statement.setNull(index++, JDBCType.DOUBLE.getVendorTypeNumber());
                statement.setBoolean(index++, booleanVal);
                statement.setByte(index++, byteVal);
                statement.setShort(index++, shortVal);
                statement.setBigDecimal(index++, bigDecimalVal);
                statement.setTimestamp(index++, timestampVal);
                statement.setTimestamp(index++, timestampVal, calendarVal);
                statement.setDate(index++, dateVal);
                statement.setDate(index++, dateVal, calendarVal);
                statement.setTime(index++, timeVal);
                statement.setTime(index++, timeVal, calendarVal);
                statement.setObject(index++, calendarVal);
                statement.setObject(index++, utilDateVal);
                statement.setObject(index++, localDateTimeVal);

                try (ResultSet results = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = results.getMetaData();
                    ParameterMetaData parameterMetaData = statement.getParameterMetaData();
                    assertEquals(resultSetMetaData.getColumnCount(), parameterMetaData.getParameterCount());
                    for (int i = 1; i < resultSetMetaData.getColumnCount(); i++) {
                        // Makes sure that column types survived the round trip
                        assertEquals(parameterMetaData.getParameterType(i), resultSetMetaData.getColumnType(i));
                    }
                    assertTrue(results.next());
                    index = 1;
                    assertEquals(stringVal, results.getString(index++));
                    assertEquals(intVal, results.getInt(index++));
                    assertEquals(longVal, results.getLong(index++));
                    assertEquals(floatVal, results.getFloat(index++), 0.00001f);
                    assertEquals(doubleVal, results.getDouble(index++), 0.00001f);
                    assertNull(results.getObject(index++));
                    assertEquals(booleanVal, results.getBoolean(index++));
                    assertEquals(byteVal, results.getByte(index++));
                    assertEquals(shortVal, results.getShort(index++));
                    assertEquals(bigDecimalVal, results.getBigDecimal(index++));
                    assertEquals(timestampVal, results.getTimestamp(index++));
                    assertEquals(timestampValWithCal, results.getTimestamp(index++));
                    assertEquals(dateVal, results.getDate(index++));
                    assertEquals(dateValWithCal, results.getDate(index++));
                    assertEquals(timeVal, results.getTime(index++));
                    assertEquals(timeValWithCal, results.getTime(index++));
                    assertEquals(new Timestamp(calendarVal.getTimeInMillis()), results.getObject(index++));
                    assertEquals(timestampVal, results.getObject(index++));
                    assertEquals(timestampVal, results.getObject(index++));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testDatetime() throws IOException, SQLException {
        long randomMillis = randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999);
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

    public void testDatetimeWithNanos() throws IOException, SQLException {
        assumeTrue("Driver version [" + JDBC_DRIVER_VERSION + "] doesn't support DATETIME with nanosecond resolution]",
                versionSupportsDateNanos());

        long randomTimestampWitnNanos = randomTimeInNanos();
        int randomNanosOnly = extractNanosOnly(randomTimestampWitnNanos);
        setupIndexForDateTimeTestsWithNanos(randomTimestampWitnNanos);

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT id, test_date_nanos FROM emps WHERE test_date_nanos = ?")) {
                Timestamp ts = new Timestamp(toMilliSeconds(randomTimestampWitnNanos));
                statement.setObject(1, ts);
                try (ResultSet results = statement.executeQuery()) {
                    assertFalse(results.next());
                }

                ts.setNanos(randomNanosOnly);
                statement.setObject(1, ts);
                try (ResultSet results = statement.executeQuery()) {
                    assertTrue(results.next());
                    assertEquals(1002, results.getInt(1));
                    assertEquals(ts, results.getTimestamp(2));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testDateTimeWithNanosAgainstDriverWithoutSupport() throws IOException, SQLException {
        assumeFalse("Driver version [" + JDBC_DRIVER_VERSION + "] doesn't support DATETIME with nanosecond resolution]",
                versionSupportsDateNanos());

        long randomTimestampWitnNanos = randomTimeInNanos();
        int randomNanosOnly = extractNanosOnly(randomTimestampWitnNanos);
        setupIndexForDateTimeTestsWithNanos(randomTimestampWitnNanos);

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT id, test_date_nanos FROM emps WHERE test_date_nanos = ?")) {
                Timestamp ts = new Timestamp(toMilliSeconds(randomTimestampWitnNanos));
                statement.setObject(1, ts);
                try (ResultSet results = statement.executeQuery()) {
                    assertFalse(results.next());
                }

                ts.setNanos(randomNanosOnly);
                statement.setObject(1, ts);
                try (ResultSet results = statement.executeQuery()) {
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testDate() throws IOException, SQLException {
        long randomMillis = randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999);
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
                        assertEquals(new Timestamp(adjustTimestampForEachDocument(randomMillis, i)), results.getTimestamp(2));
                    }
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testTime() throws IOException, SQLException {
        long randomMillis = randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999);
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

    public void testWildcardField() throws IOException, SQLException {
        String mapping = "\"properties\":{\"id\":{\"type\":\"integer\"},\"text\":{\"type\":\"wildcard\"}}";
        createIndex("test", Settings.EMPTY, mapping);
        String text = randomAlphaOfLengthBetween(1, 10);

        for (int i = 1; i <= 3; i++) {
            int id = 1000 + i;
            String valueToIndex = text + i;
            index("test", "" + i, builder -> {
                builder.field("id", id);
                builder.field("text", valueToIndex);
            });
        }

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, text FROM test WHERE text = ?")) {
                int randomDocumentIndex = randomIntBetween(1, 3);
                String randomDocumentText = text + randomDocumentIndex;

                statement.setString(1, randomDocumentText);
                try (ResultSet results = statement.executeQuery()) {
                    assertTrue(results.next());
                    assertEquals(1000 + randomDocumentIndex, results.getInt(1));
                    assertEquals(randomDocumentText, results.getString(2));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testConstantKeywordField() throws IOException, SQLException {
        String mapping = "\"properties\":{\"id\":{\"type\":\"integer\"},\"text\":{\"type\":\"constant_keyword\"}}";
        createIndex("test", Settings.EMPTY, mapping);
        String text = randomAlphaOfLengthBetween(1, 10);

        for (int i = 1; i <= 3; i++) {
            int id = 1000 + i;
            index("test", "" + i, builder -> {
                builder.field("id", id);
                builder.field("text", text);
            });
        }

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, text FROM test WHERE text = ? ORDER BY id")) {
                statement.setString(1, text);

                try (ResultSet results = statement.executeQuery()) {
                    for (int i = 1; i <= 3; i++) {
                        assertTrue(results.next());
                        assertEquals(1000 + i, results.getInt(1));
                        assertEquals(text, results.getString(2));
                    }
                    assertFalse(results.next());
                }
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
                assertEquals(new Tuple<>(KEYWORD.getName(), stringVal), execute(statement));
                statement.setInt(1, intVal);
                assertEquals(new Tuple<>(INTEGER.getName(), intVal), execute(statement));
                statement.setLong(1, longVal);
                assertEquals(new Tuple<>(LONG.getName(), longVal), execute(statement));
                statement.setFloat(1, floatVal);
                assertEquals(new Tuple<>(FLOAT.getName(), floatVal), execute(statement));
                statement.setDouble(1, doubleVal);
                assertEquals(new Tuple<>(DOUBLE.getName(), doubleVal), execute(statement));
                statement.setNull(1, JDBCType.DOUBLE.getVendorTypeNumber());
                assertEquals(new Tuple<>(DOUBLE.getName(), null), execute(statement));
                statement.setBoolean(1, booleanVal);
                assertEquals(new Tuple<>(BOOLEAN.getName(), booleanVal), execute(statement));
                statement.setByte(1, byteVal);
                assertEquals(new Tuple<>(BYTE.getName(), byteVal), execute(statement));
                statement.setShort(1, shortVal);
                assertEquals(new Tuple<>(SHORT.getName(), shortVal), execute(statement));
            }
        }
    }

    private Tuple<String, Object> execute(PreparedStatement statement) throws SQLException {
        try (ResultSet results = statement.executeQuery()) {
            ResultSetMetaData resultSetMetaData = results.getMetaData();
            assertTrue(results.next());
            Tuple<String, Object> result = new Tuple<>(resultSetMetaData.getColumnTypeName(1), results.getObject(1));
            assertFalse(results.next());
            return result;
        }
    }

    // Each time the tests pass a random time in millis/nanos and this method slightly changes this timestamp
    // for each document (based on the iteration index i) so that the test can assert that the filtering is working
    // properly and only the desired docs are returned (id of each doc is also based on i and relates to the adjusted
    // timestamp).
    private static long adjustTimestampForEachDocument(long randomMillis, int i) {
        return randomMillis - 2 + i;
    }

    private static void setupIndexForDateTimeTests(long randomMillis) throws IOException {
        setupIndexForDateTimeTests(randomMillis, false);
    }

    private static void setupIndexForDateTimeTestsWithNanos(long randomNanos) throws IOException {
        setupIndexForDateTimeTests(randomNanos, true);
    }

    private static void setupIndexForDateTimeTests(long randomMillisOrNanos, boolean isNanos) throws IOException {
        String mapping = "\"properties\":{\"id\":{\"type\":\"integer\"},";
        if (isNanos) {
            mapping += "\"test_date_nanos\":{\"type\":\"date_nanos\"}}";
        } else {
            mapping += "\"birth_date\":{\"type\":\"date\"}}";
        }
        createIndex("emps", Settings.EMPTY, mapping);
        for (int i = 1; i <= 3; i++) {
            int id = 1000 + i;
            long testMillisOrNanos = adjustTimestampForEachDocument(randomMillisOrNanos, i);
            index("emps", "" + i, builder -> {
                builder.field("id", id);
                if (isNanos) {
                    builder.field("test_date_nanos", JdbcTestUtils.asStringTimestampFromNanos(testMillisOrNanos));
                } else {
                    builder.field("birth_date", testMillisOrNanos);
                }
            });
        }
    }
}
