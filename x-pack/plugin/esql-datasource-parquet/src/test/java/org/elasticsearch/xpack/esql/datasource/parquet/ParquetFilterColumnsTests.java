/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.Operators;
import org.elasticsearch.test.ESTestCase;

/**
 * {@link ParquetFilterColumns} must mint official parquet-mr column classes (the MethodHandles
 * lookup) whose {@code ColumnPath} does not split on {@code '.'}.
 */
public class ParquetFilterColumnsTests extends ESTestCase {

    public void testLookupYieldsOfficialColumnClasses() {
        assertEquals(Operators.IntColumn.class, ParquetFilterColumns.intColumn("id").getClass());
        assertEquals(Operators.LongColumn.class, ParquetFilterColumns.longColumn("id").getClass());
        assertEquals(Operators.DoubleColumn.class, ParquetFilterColumns.doubleColumn("score").getClass());
        assertEquals(Operators.BooleanColumn.class, ParquetFilterColumns.booleanColumn("flag").getClass());
        assertEquals(Operators.BinaryColumn.class, ParquetFilterColumns.binaryColumn("name").getClass());
    }

    public void testLiteralDottedNameIsOneSegment() {
        Operators.BinaryColumn col = ParquetFilterColumns.binaryColumn("source.ip");
        assertArrayEquals(new String[] { "source.ip" }, col.getColumnPath().toArray());
        assertEquals(Operators.BinaryColumn.class, col.getClass());
    }

    public void testNestedPathIsMultiSegment() {
        Operators.BinaryColumn col = ParquetFilterColumns.binaryColumn("source", "ip");
        assertArrayEquals(new String[] { "source", "ip" }, col.getColumnPath().toArray());
        assertEquals(Operators.BinaryColumn.class, col.getClass());
    }

    public void testFilterApiFromDotStringSplits() {
        // Document the parquet-mr trap this factory exists to avoid: toString() of both paths
        // is "source.ip", but equals / row-group lookup use the segment array.
        Operators.BinaryColumn split = FilterApi.binaryColumn("source.ip");
        Operators.BinaryColumn literal = ParquetFilterColumns.binaryColumn("source.ip");
        assertArrayEquals(new String[] { "source", "ip" }, split.getColumnPath().toArray());
        assertArrayEquals(new String[] { "source.ip" }, literal.getColumnPath().toArray());
        assertEquals(split.toString(), literal.toString());
        assertNotEquals(split.getColumnPath(), literal.getColumnPath());
    }

    public void testIntAndLongLiteralDottedNames() {
        assertArrayEquals(
            new String[] { "destination.port" },
            ParquetFilterColumns.intColumn("destination.port").getColumnPath().toArray()
        );
        assertArrayEquals(new String[] { "event.id" }, ParquetFilterColumns.longColumn("event.id").getColumnPath().toArray());
        assertArrayEquals(
            new String[] { "destination", "port" },
            ParquetFilterColumns.intColumn("destination", "port").getColumnPath().toArray()
        );
    }
}
