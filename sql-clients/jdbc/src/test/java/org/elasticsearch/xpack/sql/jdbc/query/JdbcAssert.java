/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcUtils.nameOf;

public class JdbcAssert {
    public static void assertResultSets(ResultSet expected, ResultSet actual) throws SQLException {
        assertResultSetMetadata(expected, actual);
        assertResultSetData(expected, actual);
    }

    public static void assertResultSetMetadata(ResultSet expected, ResultSet actual) throws SQLException {
        ResultSetMetaData expectedMeta = expected.getMetaData();
        ResultSetMetaData actualMeta = actual.getMetaData();

        assertEquals("Different number of columns returned", expectedMeta.getColumnCount(), actualMeta.getColumnCount());
        
        for (int column = 1; column <= expectedMeta.getColumnCount(); column++) {
            String expectedName = expectedMeta.getColumnName(column); 
            String actualName = actualMeta.getColumnName(column);
            
            if (!expectedName.equals(actualName)) {
                String expectedSet = expectedName;
                String actualSet = actualName;
                if (column > 1) {
                    expectedSet = expectedMeta.getColumnName(column - 1) + "," + expectedName;
                    actualSet = actualMeta.getColumnName(column - 1) + "," + actualName;
                }

                assertEquals(f("Different column name %d", column), expectedSet, actualSet);
            }

            // use the type not the name (timestamp with timezone returns spaces for example)
            int expectedType = expectedMeta.getColumnType(column);
            int actualType = actualMeta.getColumnType(column);

            assertEquals(f("Different column type for column '%s' (%s vs %s), ", expectedName, nameOf(expectedType), nameOf(actualType)), expectedType, actualType);
        }
    }

    public static void assertResultSetData(ResultSet expected, ResultSet actual) throws SQLException {
        ResultSetMetaData metaData = expected.getMetaData();
        int columns = metaData.getColumnCount();

        long count = 0;
        while (expected.next()) {
            assertTrue(f("Expected more data but no more entries found after %d", count++), actual.next());

            for (int column = 1; column <= columns; column++) {
                Object expectedObject = expected.getObject(column);
                Object actualObject = actual.getObject(column);
                int type = metaData.getColumnType(column);

                // handle timestamps with care because h2 returns "funny" objects
                if (type == Types.TIMESTAMP_WITH_TIMEZONE) {
                    expectedObject = expected.getTimestamp(column);
                    actualObject = actual.getTimestamp(column);
                } else if (type == Types.TIME) {
                    expectedObject = expected.getTime(column);
                    actualObject = actual.getTime(column);
                } else if (type == Types.DATE) {
                    expectedObject = expected.getDate(column);
                    actualObject = actual.getDate(column);
                }

                String msg = f("Different result for column %s, entry %d", metaData.getColumnName(column), count);

                if (type == Types.DOUBLE) {
                    // NOCOMMIT 1d/1f seems like a huge difference.
                    assertEquals(msg, (double) expectedObject, (double) actualObject, 1d);
                } else if (type == Types.FLOAT) {
                    assertEquals(msg, (float) expectedObject, (float) actualObject, 1f);
                } else {
                    assertEquals(msg, expectedObject, actualObject);
                }
            }
        }
        assertEquals(f("%s still has data after %d entries", actual, count), expected.next(), actual.next());
    }

    private static String f(String message, Object... args) {
        return format(Locale.ROOT, message, args);
    }
}