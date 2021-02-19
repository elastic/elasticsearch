/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.sql.jdbc.TypeUtils.baseType;

public class JdbcArrayTests extends ESTestCase {

    static final List<EsType> ARRAY_TYPES = Arrays.stream(EsType.values()).filter(TypeUtils::isArray).collect(Collectors.toList());

    public void testMetaData() throws Exception {
        for (EsType arrayType : ARRAY_TYPES) {
            Array array = new JdbcArray(arrayType, emptyList());

            assertEquals(baseType(arrayType).getVendorTypeNumber().intValue(), array.getBaseType());
            assertEquals(baseType(arrayType).getName(), array.getBaseTypeName());
        }
    }

    public void testGetArray() throws SQLException {
        List<Long> expected = randomList(1, 10, ESTestCase::randomLong);
        Array array = new JdbcArray(EsType.LONG_ARRAY, expected);

        List<?> actual = asList((Object[]) array.getArray());
        assertEquals(expected, actual);
    }

    public void testArraySlicing() throws SQLException {
        List<Integer> values = IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toList());
        Array array = new JdbcArray(EsType.INTEGER_ARRAY, values);

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
}
