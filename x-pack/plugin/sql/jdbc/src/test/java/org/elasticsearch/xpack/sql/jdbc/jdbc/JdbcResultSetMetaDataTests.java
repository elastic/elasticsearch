/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

public class JdbcResultSetMetaDataTests extends ESTestCase {
    
    private final List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("test_keyword", JDBCType.VARCHAR, EMPTY, EMPTY, EMPTY, EMPTY, 0),
                new ColumnInfo("test_integer", JDBCType.INTEGER, EMPTY, EMPTY, EMPTY, EMPTY, 11),
                new ColumnInfo("test_double", JDBCType.DOUBLE, EMPTY, EMPTY, EMPTY, EMPTY, 25),
                new ColumnInfo("test_long", JDBCType.BIGINT, "test_table", "test", "schema", "custom_label", 20)
            );
    private final JdbcResultSetMetaData metaData = new JdbcResultSetMetaData(null, columns);

    public void testColumnsProperties() throws SQLException {
        int maxColumnIndex = columns.size();
        assertEquals(false, metaData.isAutoIncrement(randomIntBetween(1, maxColumnIndex)));
        assertEquals(true, metaData.isCaseSensitive(randomIntBetween(1, maxColumnIndex)));
        assertEquals(true, metaData.isSearchable(randomIntBetween(1, maxColumnIndex)));
        assertEquals(false, metaData.isCurrency(randomIntBetween(1, maxColumnIndex)));
        assertEquals(ResultSetMetaData.columnNullableUnknown, metaData.isNullable(randomIntBetween(1, maxColumnIndex)));
        assertEquals(false, metaData.isSigned(1));
        assertEquals(true, metaData.isSigned(2));
        assertEquals(true, metaData.isSigned(3));
        assertEquals(true, metaData.isSigned(4));
    }
    
    public void testColumnNamesAndLabels() throws SQLException {
        assertEquals("test_keyword", metaData.getColumnName(1));
        assertEquals("test_integer", metaData.getColumnName(2));
        assertEquals("test_double", metaData.getColumnName(3));
        assertEquals("test_long", metaData.getColumnName(4));
        
        assertEquals("test_keyword", metaData.getColumnLabel(1));
        assertEquals("test_integer", metaData.getColumnLabel(2));
        assertEquals("test_double", metaData.getColumnLabel(3));
        assertEquals("custom_label", metaData.getColumnLabel(4));
    }
}
