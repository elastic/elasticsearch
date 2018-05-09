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

public class SysGeometryColumnsTests extends ESTestCase {

    public void testSysGeometryColumns() {
        List<List<?>> rows = new ArrayList<>();
        SysGeometryColumns.fillInRows("test", "index", TypesTests.loadMapping("mapping-basic.json", true), null, rows, null);
        assertEquals(1, rows.size());
        assertEquals(9, rows.get(0).size());

        List<?> row = rows.get(0);
        assertEquals("test", row.get(0));
        assertEquals("index", row.get(2));
        assertEquals("site", row.get(3));
        assertEquals(1, row.get(4));
        assertEquals(0, row.get(5));
        assertEquals(2, row.get(6));
        assertEquals(0, row.get(7));
        assertEquals("GEOMETRY", row.get(8));

    }
}
