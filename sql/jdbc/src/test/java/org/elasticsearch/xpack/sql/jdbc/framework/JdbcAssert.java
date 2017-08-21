/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.logging.log4j.Logger;

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

public class JdbcAssert {
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

    public static void assertResultSets(ResultSet expected, ResultSet actual) throws SQLException {
        assertResultSets(expected, actual, null);
    }

    public static void assertResultSets(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        assertResultSetMetadata(expected, actual, logger);
        assertResultSetData(expected, actual, logger);
    }

    public static void assertResultSetMetadata(ResultSet expected, ResultSet actual) throws SQLException {
        assertResultSetMetadata(expected, actual, null);
    }

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
                // NOCOMMIT this needs a comment explaining it....
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

            assertEquals("Different column type for column [" + expectedName + "] (" + JDBCType.valueOf(expectedType) + " != "
                    + JDBCType.valueOf(actualType) + ")", expectedType, actualType);
        }
    }

    public static void assertResultSetData(ResultSet expected, ResultSet actual, Logger logger) throws SQLException {
        ResultSetMetaData metaData = expected.getMetaData();
        int columns = metaData.getColumnCount();

        long count = 0;
        while (expected.next()) {
            assertTrue("Expected more data but no more entries found after [" + count + "]", actual.next());
            count++;

            if (logger != null) {
                JdbcTestUtils.logResultSetCurrentData(actual, logger);
            }

            for (int column = 1; column <= columns; column++) {
                Object expectedObject = expected.getObject(column);
                Object actualObject = actual.getObject(column);
                int type = metaData.getColumnType(column);

                String msg = "Different result for column [" + metaData.getColumnName(column)  + "], entry [" + count + "]";

                if (type == Types.TIMESTAMP) {
                    assertEquals(getTime(expected, column), getTime(actual, column));
                }

                else if (type == Types.DOUBLE) {
                    // NOCOMMIT 1d/1f seems like a huge difference.
                    assertEquals(msg, (double) expectedObject, (double) actualObject, 1d);
                } else if (type == Types.FLOAT) {
                    assertEquals(msg, (float) expectedObject, (float) actualObject, 1f);
                } else {
                    assertEquals(msg, expectedObject, actualObject);
                }
            }
        }
        assertEquals("[" + actual + "] still has data after [" + count + "] entries", expected.next(), actual.next());
    }

    private static Object getTime(ResultSet rs, int column) throws SQLException {
        return rs.getTime(column, UTC_CALENDAR).getTime();
    }
}