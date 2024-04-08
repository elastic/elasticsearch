/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.sql.jdbc.EsType;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.relique.jdbc.csv.CsvResultSet;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.time.ZoneOffset.UTC;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.logResultSetMetaData;
import static org.elasticsearch.xpack.sql.qa.jdbc.JdbcTestUtils.resultSetCurrentData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Utility class for doing JUnit-style asserts over JDBC.
 */
public class JdbcAssert {
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

    private static final Map<Integer, EsType> SQL_TO_TYPE = new HashMap<>();

    static {
        for (EsType type : EsType.values()) {
            SQL_TO_TYPE.putIfAbsent(type.getVendorTypeNumber().intValue(), type);
        }
    }

    public static void assertResultSets(ResultSet expected, ResultSet actual) throws SQLException {
        assertResultSets(expected, actual, null);
    }

    public static void assertResultSets(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        assertResultSets(expected, actual, logger, false);
    }

    /**
     * Assert the given result sets, potentially in a lenient way.
     * When lenientDataType is specified, the type comparison of a column is widden to reach a common, compatible ground.
     * This means promoting integer types to long and floating types to double and comparing their values.
     * For example in a non-lenient, strict case a comparison between an int and a tinyint would fail, with lenientDataType it will succeed
     * as long as the actual value is the same.
     */
    public static void assertResultSets(ResultSet expected, ResultSet actual, Logger logger, boolean lenientDataType) throws SQLException {
        assertResultSets(expected, actual, logger, lenientDataType, true);
    }

    /**
     * Assert the given result sets, potentially in a lenient way.
     * When lenientDataType is specified, the type comparison of a column is widden to reach a common, compatible ground.
     * This means promoting integer types to long and floating types to double and comparing their values.
     * For example in a non-lenient, strict case a comparison between an int and a tinyint would fail, with lenientDataType it will succeed
     * as long as the actual value is the same.
     * Also, has the option of treating the numeric results for floating point numbers in a leninent way, if chosen to. Usually,
     * we would want lenient treatment for floating point numbers in sql-spec tests where the comparison is being made with H2.
     */
    public static void assertResultSets(
        ResultSet expected,
        ResultSet actual,
        Logger logger,
        boolean lenientDataType,
        boolean lenientFloatingNumbers
    ) throws SQLException {
        try (ResultSet ex = expected; ResultSet ac = actual) {
            assertResultSetMetaData(ex, ac, logger, lenientDataType);
            assertResultSetData(ex, ac, logger, lenientDataType, lenientFloatingNumbers);
        }
    }

    public static void assertResultSetMetaData(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        assertResultSetMetaData(expected, actual, logger, false);
    }

    // MetaData doesn't consume a ResultSet thus it shouldn't close it
    public static void assertResultSetMetaData(ResultSet expected, ResultSet actual, Logger logger, boolean lenientDataType)
        throws SQLException {
        ResultSetMetaData expectedMeta = expected.getMetaData();
        ResultSetMetaData actualMeta = actual.getMetaData();

        if (logger != null) {
            logResultSetMetaData(actual, logger);
        }

        if (expectedMeta.getColumnCount() != actualMeta.getColumnCount()) {
            List<String> expectedCols = new ArrayList<>();
            for (int i = 1; i <= expectedMeta.getColumnCount(); i++) {
                expectedCols.add(expectedMeta.getColumnName(i));

            }

            List<String> actualCols = new ArrayList<>();
            for (int i = 1; i <= actualMeta.getColumnCount(); i++) {
                actualCols.add(actualMeta.getColumnName(i));
            }

            assertEquals(
                format(
                    Locale.ROOT,
                    "Different number of columns returned (expected %d but was %d);",
                    expectedMeta.getColumnCount(),
                    actualMeta.getColumnCount()
                ),
                expectedCols.toString(),
                actualCols.toString()
            );
        }

        for (int column = 1; column <= expectedMeta.getColumnCount(); column++) {
            String expectedName = expectedMeta.getColumnName(column);
            String actualName = actualMeta.getColumnName(column);

            if (expectedName.equals(actualName) == false) {
                // to help debugging, indicate the previous column (which also happened to match and thus was correct)
                String expectedSet = expectedName;
                String actualSet = actualName;
                if (column > 1) {
                    expectedSet = expectedMeta.getColumnName(column - 1) + "," + expectedName;
                    actualSet = actualMeta.getColumnName(column - 1) + "," + actualName;
                }

                assertEquals("Different column name [" + column + "]", expectedSet, actualSet);
            }

            // use the type not the name (timestamp with timezone returns spaces for example)
            int expectedType = typeOf(expectedMeta.getColumnType(column), lenientDataType);
            int actualType = typeOf(actualMeta.getColumnType(column), lenientDataType);
            String actualTypeName = actualMeta.getColumnTypeName(column);

            // since H2 cannot use a fixed timezone, the data is stored in UTC (and thus with timezone)
            if (expectedType == Types.TIMESTAMP_WITH_TIMEZONE) {
                expectedType = Types.TIMESTAMP;
            }

            // H2 treats GEOMETRY as OTHER
            if (expectedType == Types.OTHER && nameOf(actualType).startsWith("GEO_")) {
                actualType = Types.OTHER;
            }

            // since csv doesn't support real, we use float instead.....
            if (expectedType == Types.FLOAT && expected instanceof CsvResultSet) {
                expectedType = Types.REAL;
            }
            // handle intervals
            if ((expectedType == Types.VARCHAR && expected instanceof CsvResultSet) && nameOf(actualType).startsWith("INTERVAL_")) {
                expectedType = actualType;
            }

            // csv doesn't support NULL type so skip type checking
            if (actualType == Types.NULL && expected instanceof CsvResultSet) {
                expectedType = Types.NULL;
            }

            // csv and h2 both map values larger than Long.MAX_VALUE to Decimal types
            if (expectedType == Types.DECIMAL && actualTypeName.compareTo(EsType.UNSIGNED_LONG.getName()) == 0) {
                expectedType = EsType.UNSIGNED_LONG.getVendorTypeNumber();
            }

            // when lenient is used, an int is equivalent to a short, etc...
            assertEquals(
                "Different column type for column [" + expectedName + "] (" + nameOf(expectedType) + " != " + nameOf(actualType) + ")",
                expectedType,
                actualType
            );
        }
    }

    private static String nameOf(int sqlType) {
        return SQL_TO_TYPE.get(sqlType).getName();
    }

    // The ResultSet is consumed and thus it should be closed
    public static void assertResultSetData(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        assertResultSetData(expected, actual, logger, false);
    }

    public static void assertResultSetData(ResultSet expected, ResultSet actual, Logger logger, boolean lenientDataType)
        throws SQLException {
        assertResultSetData(expected, actual, logger, lenientDataType, true);
    }

    public static void assertResultSetData(
        ResultSet expected,
        ResultSet actual,
        Logger logger,
        boolean lenientDataType,
        boolean lenientFloatingNumbers
    ) throws SQLException {
        try (ResultSet ex = expected; ResultSet ac = actual) {
            doAssertResultSetData(ex, ac, logger, lenientDataType, lenientFloatingNumbers);
        }
    }

    private static void doAssertResultSetData(
        ResultSet expected,
        ResultSet actual,
        Logger logger,
        boolean lenientDataType,
        boolean lenientFloatingNumbers
    ) throws SQLException {
        ResultSetMetaData metaData = expected.getMetaData();
        int columns = metaData.getColumnCount();

        long count = 0;
        try {
            for (count = 0; expected.next(); count++) {
                assertTrue("Expected more data but no more entries found after [" + count + "]", actual.next());

                if (logger != null) {
                    logger.info(resultSetCurrentData(actual));
                }

                for (int column = 1; column <= columns; column++) {
                    int type = metaData.getColumnType(column);
                    Class<?> expectedColumnClass = null;
                    try {
                        String columnClassName = metaData.getColumnClassName(column);

                        // fix for CSV which returns the shortName not fully-qualified name
                        if (columnClassName != null && columnClassName.contains(".") == false) {
                            columnClassName = switch (columnClassName) {
                                case "Date" -> "java.sql.Date";
                                case "Time" -> "java.sql.Time";
                                case "Timestamp" -> "java.sql.Timestamp";
                                case "Int" -> "java.lang.Integer";
                                case "BigDecimal" -> "java.math.BigDecimal";
                                default -> "java.lang." + columnClassName;
                            };
                        }

                        if (columnClassName != null) {
                            expectedColumnClass = Class.forName(columnClassName);
                        }
                    } catch (ClassNotFoundException cnfe) {
                        throw new SQLException(cnfe);
                    }

                    Object expectedObject = expected.getObject(column);
                    Object actualObject = (lenientDataType && expectedColumnClass != null)
                        ? actual.getObject(column, expectedColumnClass)
                        : actual.getObject(column);

                    String msg = format(
                        Locale.ROOT,
                        "Different result for column [%s], entry [%d]",
                        metaData.getColumnName(column),
                        count + 1
                    );

                    // handle nulls first
                    if (expectedObject == null || actualObject == null) {
                        // hack for JDBC CSV nulls
                        if (expectedObject != null && "null".equals(expectedObject.toString().toLowerCase(Locale.ROOT))) {
                            assertNull(msg, actualObject);
                        } else {
                            assertEquals(msg, expectedObject, actualObject);
                        }
                    }
                    // then timestamp
                    else if (type == Types.TIMESTAMP || type == Types.TIMESTAMP_WITH_TIMEZONE) {
                        assertEquals(msg, expected.getTimestamp(column), actual.getTimestamp(column));
                    }
                    // then date
                    else if (type == Types.DATE) {
                        assertEquals(msg, convertDateToSystemTimezone(expected.getDate(column)), actual.getDate(column));
                    }
                    // and floats/doubles
                    else if (type == Types.DOUBLE) {
                        assertEquals(msg, (double) expectedObject, (double) actualObject, lenientFloatingNumbers ? 1d : 0.0d);
                    } else if (type == Types.FLOAT) {
                        assertEquals(msg, (float) expectedObject, (float) actualObject, lenientFloatingNumbers ? 1f : 0.0f);
                    } else if (type == Types.OTHER) {
                        // check geo case
                        if (actual.getMetaData().getColumnType(column) == EsType.GEO_SHAPE.getVendorTypeNumber()) {
                            // parse strings into actual objects
                            actualObject = fromWkt(actualObject.toString());
                            expectedObject = fromWkt(expectedObject.toString());

                            if (actualObject instanceof Point) {
                                // geo points are loaded form doc values where they are stored as long-encoded values leading
                                // to lose in precision
                                assertThat(expectedObject, instanceOf(Point.class));
                                assertEquals(((Point) expectedObject).getY(), ((Point) actualObject).getY(), 0.000001d);
                                assertEquals(((Point) expectedObject).getX(), ((Point) actualObject).getX(), 0.000001d);
                            }
                        } else {
                            assertEquals(msg, expectedObject, actualObject);
                        }
                    }
                    // intervals
                    else if (type == Types.VARCHAR && actualObject instanceof TemporalAmount) {
                        assertEquals(msg, expectedObject, StringUtils.toString(actualObject));
                    }
                    // unsigned_long
                    else if (expectedObject instanceof BigDecimal && actualObject instanceof BigInteger) {
                        assertEquals(expectedObject, new BigDecimal((BigInteger) actualObject));
                    }
                    // finally the actual comparison
                    else {
                        assertEquals(msg, expectedObject, actualObject);
                    }
                }
            }
        } catch (AssertionError ae) {
            if (logger != null && actual.next()) {
                logger.info("^^^ Assertion failure ^^^");
                logger.info(resultSetCurrentData(actual));
            }
            throw ae;
        }

        if (actual.next()) {
            fail("Elasticsearch [" + actual + "] still has data after [" + count + "] entries:\n" + resultSetCurrentData(actual));
        }
    }

    private static Object fromWkt(String source) {
        try {
            return WellKnownText.fromWKT(StandardValidator.instance(true), true, source);
        } catch (IOException | ParseException ex) {
            fail(ex.getMessage());
            // won't execute
            return null;
        }
    }

    /**
     * Returns the value of the given type either in a lenient fashion (widened) or strict.
     */
    private static int typeOf(int columnType, boolean lenientDataType) {
        if (lenientDataType) {
            // integer upcast to long
            if (columnType == TINYINT || columnType == SMALLINT || columnType == INTEGER || columnType == BIGINT) {
                return BIGINT;
            }
            if (columnType == FLOAT || columnType == REAL || columnType == DOUBLE) {
                return REAL;
            }
        }

        return columnType;
    }

    // Used to convert the DATE read from CSV file to a java.sql.Date at the System's timezone (-Dtests.timezone=XXXX)
    private static Date convertDateToSystemTimezone(Date date) {
        return new Date(date.toLocalDate().atStartOfDay(UTC).toInstant().toEpochMilli());
    }
}
