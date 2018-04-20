/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import org.apache.logging.log4j.Logger;
import org.relique.jdbc.csv.CsvResultSet;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JdbcAssert {
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

    public static void assertResultSets(ResultSet expected, ResultSet actual) throws SQLException {
        assertResultSets(expected, actual, null);
    }

    public static void assertResultSets(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        try (ResultSet ex = expected; ResultSet ac = actual) {
            assertResultSetMetadata(ex, ac, logger);
            assertResultSetData(ex, ac, logger);
        }
    }

    // metadata doesn't consume a ResultSet thus it shouldn't close it
    public static void assertResultSetMetadata(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        ResultSetMetaData expectedMeta = expected.getMetaData();
        ResultSetMetaData actualMeta = actual.getMetaData();

        if (logger != null) {
            JdbcTestUtils.logResultSetMetadata(actual, logger);
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

            assertEquals(format(Locale.ROOT, "Different number of columns returned (expected %d but was %d);",
                    expectedMeta.getColumnCount(), actualMeta.getColumnCount()),
                    expectedCols.toString(), actualCols.toString());
        }

        for (int column = 1; column <= expectedMeta.getColumnCount(); column++) {
            String expectedName = expectedMeta.getColumnName(column);
            String actualName = actualMeta.getColumnName(column);

            if (!expectedName.equals(actualName)) {
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
            int expectedType = expectedMeta.getColumnType(column);
            int actualType = actualMeta.getColumnType(column);

            // since H2 cannot use a fixed timezone, the data is stored in UTC (and thus with timezone)
            if (expectedType == Types.TIMESTAMP_WITH_TIMEZONE) {
                expectedType = Types.TIMESTAMP;
            }
            // since csv doesn't support real, we use float instead.....
            if (expectedType == Types.FLOAT && expected instanceof CsvResultSet) {
                expectedType = Types.REAL;
            }
            assertEquals("Different column type for column [" + expectedName + "] (" + JDBCType.valueOf(expectedType) + " != "
                    + JDBCType.valueOf(actualType) + ")", expectedType, actualType);
        }
    }

    // The ResultSet is consumed and thus it should be closed
    public static void assertResultSetData(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        try (ResultSet ex = expected; ResultSet ac = actual) {
            doAssertResultSetData(ex, ac, logger);
        }
    }
    
    private static void doAssertResultSetData(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        ResultSetMetaData metaData = expected.getMetaData();
        int columns = metaData.getColumnCount();

        long count = 0;
        try {
            for (count = 0; expected.next(); count++) {
                assertTrue("Expected more data but no more entries found after [" + count + "]", actual.next());

                if (logger != null) {
                    logger.info(JdbcTestUtils.resultSetCurrentData(actual));
                }

                for (int column = 1; column <= columns; column++) {
                    Object expectedObject = expected.getObject(column);
                    Object actualObject = actual.getObject(column);

                    int type = metaData.getColumnType(column);

                    String msg = format(Locale.ROOT, "Different result for column [" + metaData.getColumnName(column) + "], "
                            + "entry [" + (count + 1) + "]");

                    // handle nulls first
                    if (expectedObject == null || actualObject == null) {
                        assertEquals(msg, expectedObject, actualObject);
                    }
                    // then timestamp
                    else if (type == Types.TIMESTAMP || type == Types.TIMESTAMP_WITH_TIMEZONE) {
                        assertEquals(msg, expected.getTimestamp(column), actual.getTimestamp(column));
                    }
                    // and floats/doubles
                    else if (type == Types.DOUBLE) {
                        // the 1d/1f difference is used due to rounding/flooring
                        assertEquals(msg, (double) expectedObject, (double) actualObject, 1d);
                    } else if (type == Types.FLOAT) {
                        assertEquals(msg, (float) expectedObject, (float) actualObject, 1f);
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
                logger.info(JdbcTestUtils.resultSetCurrentData(actual));
            }
            throw ae;
        }

        if (actual.next()) {
            fail("Elasticsearch [" + actual + "] still has data after [" + count + "] entries:\n"
                    + JdbcTestUtils.resultSetCurrentData(actual));
        }
    }

}