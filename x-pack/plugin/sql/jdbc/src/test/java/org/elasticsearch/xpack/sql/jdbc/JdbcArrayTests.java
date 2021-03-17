/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.DEFAULT_URI;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.TIME_ZONE;
import static org.elasticsearch.xpack.sql.jdbc.JdbcConfiguration.URL_PREFIX;
import static org.elasticsearch.xpack.sql.jdbc.TypeUtils.baseType;

public class JdbcArrayTests extends ESTestCase {

    static final List<EsType> ARRAY_TYPES = Arrays.stream(EsType.values()).filter(TypeUtils::isArray).collect(Collectors.toList());

    public void testMetaData() throws Exception {
        for (EsType arrayType : ARRAY_TYPES) {
            Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(arrayType), emptyList());

            assertEquals(baseType(arrayType).getVendorTypeNumber().intValue(), array.getBaseType());
            assertEquals(baseType(arrayType).getName(), array.getBaseTypeName());
        }
    }

    public void testGetArray() throws SQLException {
        List<Long> expected = randomList(1, 10, ESTestCase::randomLong);
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.LONG_ARRAY), expected);

        List<?> actual = asList((Object[]) array.getArray());
        assertEquals(expected, actual);
    }

    public void testArraySlicing() throws SQLException {
        List<Integer> values = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.INTEGER_ARRAY), values);

        Object[] empty = (Object[]) array.getArray(11, 2);
        assertEquals(0, empty.length);

        Object[] edgeSingleton = (Object[]) array.getArray(10, 2);
        assertEquals(9, edgeSingleton[0]);

        Object[] midSingleton = (Object[]) array.getArray(5, 1);
        assertEquals(4, midSingleton[0]);

        Object[] contained = (Object[]) array.getArray(4, 3);
        assertEquals(asList(3, 4, 5), asList(contained));

        Object[] overlapping = (Object[]) array.getArray(9, 3);
        assertEquals(asList(8, 9), asList(overlapping));

        SQLException sqle = expectThrows(SQLException.class, () -> array.getArray(0, 9));
        assertEquals("Index value [0] out of range [1, 2147483647]", sqle.getMessage());

        sqle = expectThrows(SQLException.class, () -> array.getArray(Integer.MAX_VALUE + 1L, 9));
        assertEquals("Index value [2147483648] out of range [1, 2147483647]", sqle.getMessage());
    }

    public void testSqlExceptionPastFree() throws SQLException {
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.LONG_ARRAY), emptyList());
        array.free();

        List<ThrowingRunnable> calls = asList(array::getBaseTypeName, array::getBaseType, array::getArray,
            () -> array.getArray(emptyMap()), () -> array.getArray(1, 3), () -> array.getArray( 1, 2, emptyMap()),
            array::getResultSet, () -> array.getResultSet(3, 5), () -> array.getResultSet(emptyMap()),
            () -> array.getResultSet(1, 4, emptyMap())
        );

        for (ThrowingRunnable call : calls) {
            SQLException sqle = expectThrows(SQLException.class, call);
            assertEquals("Array has been freed already", sqle.getMessage());
        }

        array.free();
    }

    public void testNonEmptyMapRejected() throws SQLException {
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.LONG_ARRAY), emptyList());
        Map<String, Class<?>> map = Map.of("foo", String.class);

        List<ThrowingRunnable> calls = asList(() -> array.getArray(map), () -> array.getArray( 1, 2, map),
            () -> array.getResultSet(map), () -> array.getResultSet(1, 4, map)
        );

        for (ThrowingRunnable call : calls) {
            SQLException sqle = expectThrows(SQLException.class, call);
            assertEquals("non-empty Map parameter not supported", sqle.getMessage());
        }
    }

    public void testArrayGetAsResultSet() throws SQLException {
        List<Integer> expected = randomList(1, 10, ESTestCase::randomInt);
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.INTEGER_ARRAY), expected);

        ResultSet resultSet = array.getResultSet();
        // meta
        ResultSetMetaData meta = resultSet.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals(Types.BIGINT, meta.getColumnType(1));
        assertEquals("index", meta.getColumnName(1));
        assertEquals(Types.INTEGER, meta.getColumnType(2));
        assertEquals("value", meta.getColumnName(2));
        // values
        for (int i = 0; i < expected.size(); i++) {
            assertTrue(resultSet.next());
            assertEquals(Long.valueOf(i + 1), resultSet.getObject(1));
            assertEquals(expected.get(i), resultSet.getObject(2));
        }
        assertFalse(resultSet.next());
    }

    public void testArrayGetAsSlicedResultSet() throws SQLException {
        List<Integer> expected = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        Array array = new JdbcArray(jdbcTestConfiguration().timeZone(), baseType(EsType.INTEGER_ARRAY), expected);
        long start = 4;
        int count = 3;
        ResultSet resultSet = array.getResultSet(start, count);

        for (int i = (int) start - 1; i < start - 1 + count; i++) {
            assertTrue(resultSet.next());
            assertEquals(Long.valueOf(i + 1), resultSet.getObject(1));
            assertEquals(expected.get(i), resultSet.getObject(2));
        }
        assertFalse(resultSet.next());

        resultSet = array.getResultSet(11, count);
        assertFalse(resultSet.next());

        resultSet = array.getResultSet(10, count);
        assertTrue(resultSet.next());
        assertEquals(10L, resultSet.getObject(1));
        assertEquals(9, resultSet.getObject(2));
        assertFalse(resultSet.next());

        SQLException sqle = expectThrows(SQLException.class, () -> array.getResultSet(0, count));
        assertEquals("Index value [0] out of range [1, 2147483647]", sqle.getMessage());
    }

    private static JdbcConfiguration jdbcTestConfiguration() throws JdbcSQLException {
        Properties properties = new Properties();
        properties.setProperty(TIME_ZONE, "Z");
        return JdbcConfiguration.create(URL_PREFIX + DEFAULT_URI.toString(), properties, 0);
    }
}
