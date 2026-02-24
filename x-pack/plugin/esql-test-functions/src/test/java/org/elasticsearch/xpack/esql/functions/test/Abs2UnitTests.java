/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the Abs2 function.
 * <p>
 * These tests verify the basic functionality of the Abs2 function class,
 * including type resolution and the process methods that will be used
 * for runtime bytecode generation.
 * </p>
 */
public class Abs2UnitTests extends ESTestCase {

    /**
     * Test the static process methods that are used for runtime bytecode generation.
     */
    public void testProcessDouble() {
        assertThat(Abs2.processDouble(-42.5), equalTo(42.5));
        assertThat(Abs2.processDouble(42.5), equalTo(42.5));
        assertThat(Abs2.processDouble(0.0), equalTo(0.0));
        assertThat(Abs2.processDouble(-0.0), equalTo(0.0));
    }

    public void testProcessLong() {
        assertThat(Abs2.processLong(-42L), equalTo(42L));
        assertThat(Abs2.processLong(42L), equalTo(42L));
        assertThat(Abs2.processLong(0L), equalTo(0L));
        assertThat(Abs2.processLong(Long.MIN_VALUE + 1), equalTo(Long.MAX_VALUE));
    }

    public void testProcessInt() {
        assertThat(Abs2.processInt(-42), equalTo(42));
        assertThat(Abs2.processInt(42), equalTo(42));
        assertThat(Abs2.processInt(0), equalTo(0));
        assertThat(Abs2.processInt(Integer.MIN_VALUE + 1), equalTo(Integer.MAX_VALUE));
    }

    public void testProcessUnsignedLong() {
        // Unsigned long is already positive, should return as-is
        assertThat(Abs2.processUnsignedLong(42L), equalTo(42L));
        assertThat(Abs2.processUnsignedLong(0L), equalTo(0L));
        assertThat(Abs2.processUnsignedLong(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));
    }

    /**
     * Test type resolution for valid numeric types.
     */
    public void testTypeResolution() {
        // Test with integer
        Abs2 abs2Int = new Abs2(Source.EMPTY, new Literal(Source.EMPTY, -5, DataType.INTEGER));
        assertThat(abs2Int.dataType(), equalTo(DataType.INTEGER));
        assertThat(abs2Int.typeResolved().resolved(), equalTo(true));

        // Test with long
        Abs2 abs2Long = new Abs2(Source.EMPTY, new Literal(Source.EMPTY, -5L, DataType.LONG));
        assertThat(abs2Long.dataType(), equalTo(DataType.LONG));
        assertThat(abs2Long.typeResolved().resolved(), equalTo(true));

        // Test with double
        Abs2 abs2Double = new Abs2(Source.EMPTY, new Literal(Source.EMPTY, -5.0, DataType.DOUBLE));
        assertThat(abs2Double.dataType(), equalTo(DataType.DOUBLE));
        assertThat(abs2Double.typeResolved().resolved(), equalTo(true));

        // Test with unsigned long
        Abs2 abs2ULong = new Abs2(Source.EMPTY, new Literal(Source.EMPTY, 5L, DataType.UNSIGNED_LONG));
        assertThat(abs2ULong.dataType(), equalTo(DataType.UNSIGNED_LONG));
        assertThat(abs2ULong.typeResolved().resolved(), equalTo(true));
    }

    /**
     * Test that invalid types are rejected.
     */
    public void testInvalidTypeResolution() {
        // Test with boolean (invalid for abs2)
        Abs2 abs2Bool = new Abs2(Source.EMPTY, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        assertThat(abs2Bool.typeResolved().resolved(), equalTo(false));
        assertTrue(abs2Bool.typeResolved().message().contains("numeric"));
    }

    /**
     * Test edge cases for numeric values.
     */
    public void testEdgeCases() {
        // Test with very large double
        assertThat(Abs2.processDouble(-Double.MAX_VALUE), equalTo(Double.MAX_VALUE));
        assertThat(Abs2.processDouble(Double.MAX_VALUE), equalTo(Double.MAX_VALUE));

        // Test with very small double
        assertThat(Abs2.processDouble(-Double.MIN_VALUE), equalTo(Double.MIN_VALUE));
        assertThat(Abs2.processDouble(Double.MIN_VALUE), equalTo(Double.MIN_VALUE));

        // Test with infinity
        assertThat(Abs2.processDouble(Double.NEGATIVE_INFINITY), equalTo(Double.POSITIVE_INFINITY));
        assertThat(Abs2.processDouble(Double.POSITIVE_INFINITY), equalTo(Double.POSITIVE_INFINITY));

        // Test with NaN
        assertTrue(Double.isNaN(Abs2.processDouble(Double.NaN)));
    }
}
