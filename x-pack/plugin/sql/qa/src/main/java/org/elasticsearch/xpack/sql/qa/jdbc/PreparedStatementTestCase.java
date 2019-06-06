/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.common.collect.Tuple;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import static org.hamcrest.Matchers.startsWith;

public class PreparedStatementTestCase extends JdbcIntegrationTestCase {

    public void testSupportedTypes() throws Exception {
        index("library", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });

        String stringVal = randomAlphaOfLength(randomIntBetween(0, 1000));
        int intVal = randomInt();
        long longVal = randomLong();
        double doubleVal = randomDouble();
        float floatVal = randomFloat();
        boolean booleanVal = randomBoolean();
        byte byteVal = randomByte();
        short shortVal = randomShort();

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, name FROM library WHERE page_count=?")) {
                statement.setString(1, stringVal);
                statement.setInt(2, intVal);
                statement.setLong(3, longVal);
                statement.setFloat(4, floatVal);
                statement.setDouble(5, doubleVal);
                statement.setNull(6, JDBCType.DOUBLE.getVendorTypeNumber());
                statement.setBoolean(7, booleanVal);
                statement.setByte(8, byteVal);
                statement.setShort(9, shortVal);
                statement.setInt(10, 1072);

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
                    assertEquals("Don Quixote", results.getString(10));
                    assertFalse(results.next());
                }
            }
        }
    }

    public void testUnsupportedParameterUse() throws Exception {
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

    public void testTooMayParameters() throws Exception {
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

    public void testStringEscaping() throws Exception {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT ?, ?, ?, ?")) {
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

    public void testCommentsHandling() throws Exception {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT ?, /* ?, */ ? -- ?")) {
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

    public void testSingleParameterMultipleTypes() throws Exception {
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
}
