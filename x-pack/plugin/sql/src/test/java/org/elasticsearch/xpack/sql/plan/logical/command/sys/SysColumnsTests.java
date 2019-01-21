/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.type.TypesTests;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class SysColumnsTests extends ESTestCase {

    public void testSysColumns() {
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "index", TypesTests.loadMapping("mapping-multi-field-variation.json", true), null, rows, null, false);
        assertEquals(16, rows.size());
        assertEquals(24, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("bool", name(row));
        assertEquals(Types.BOOLEAN, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(1, bufferLength(row));

        row = rows.get(1);
        assertEquals("int", name(row));
        assertEquals(Types.INTEGER, sqlType(row));
        assertEquals(10, radix(row));
        assertEquals(4, bufferLength(row));

        row = rows.get(2);
        assertEquals("text", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));

        row = rows.get(4);
        assertEquals("date", name(row));
        assertEquals(Types.TIMESTAMP, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(24, precision(row));
        assertEquals(8, bufferLength(row));

        row = rows.get(7);
        assertEquals("some.dotted", name(row));
        assertEquals(Types.STRUCT, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(-1, bufferLength(row));

        row = rows.get(15);
        assertEquals("some.ambiguous.normalized", name(row));
        assertEquals(Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
    }

    public void testSysColumnsInOdbcMode() {
        List<List<?>> rows = new ArrayList<>();
        SysColumns.fillInRows("test", "index", TypesTests.loadMapping("mapping-multi-field-variation.json", true), null, rows, null, true);
        assertEquals(16, rows.size());
        assertEquals(24, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("bool", name(row));
        assertEquals((short) Types.BOOLEAN, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(1, bufferLength(row));

        row = rows.get(1);
        assertEquals("int", name(row));
        assertEquals((short) Types.INTEGER, sqlType(row));
        assertEquals(Short.class, radix(row).getClass());
        assertEquals(4, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(2);
        assertEquals("text", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(4);
        assertEquals("date", name(row));
        assertEquals((short) Types.TIMESTAMP, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(24, precision(row));
        assertEquals(8, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(7);
        assertEquals("some.dotted", name(row));
        assertEquals((short) Types.STRUCT, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(-1, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());

        row = rows.get(15);
        assertEquals("some.ambiguous.normalized", name(row));
        assertEquals((short) Types.VARCHAR, sqlType(row));
        assertEquals(null, radix(row));
        assertEquals(Integer.MAX_VALUE, bufferLength(row));
        assertNull(decimalPrecision(row));
        assertEquals(Short.class, nullable(row).getClass());
        assertEquals(Short.class, sqlDataType(row).getClass());
        assertEquals(Short.class, sqlDataTypeSub(row).getClass());
    }

    private static Object name(List<?> list) {
        return list.get(3);
    }

    private static Object sqlType(List<?> list) {
        return list.get(4);
    }

    private static Object precision(List<?> list) {
        return list.get(6);
    }

    private static Object bufferLength(List<?> list) {
        return list.get(7);
    }

    private static Object decimalPrecision(List<?> list) {
        return list.get(8);
    }

    private static Object radix(List<?> list) {
        return list.get(9);
    }

    private static Object nullable(List<?> list) {
        return list.get(10);
    }

    private static Object sqlDataType(List<?> list) {
        return list.get(13);
    }

    private static Object sqlDataTypeSub(List<?> list) {
        return list.get(14);
    }
}