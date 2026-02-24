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
 * Unit tests for the Add2 binary function.
 * <p>
 * These tests verify the basic functionality of the Add2 function class,
 * including type resolution and the process methods that will be used
 * for runtime bytecode generation.
 * </p>
 */
public class Add2Tests extends ESTestCase {

    /**
     * Test the static process methods that are used for runtime bytecode generation.
     */
    public void testProcessDouble() {
        assertThat(Add2.processDouble(3.0, 4.0), equalTo(7.0));
        assertThat(Add2.processDouble(-3.0, 4.0), equalTo(1.0));
        assertThat(Add2.processDouble(3.0, -4.0), equalTo(-1.0));
        assertThat(Add2.processDouble(-3.0, -4.0), equalTo(-7.0));
        assertThat(Add2.processDouble(0.0, 0.0), equalTo(0.0));
    }

    public void testProcessLong() {
        assertThat(Add2.processLong(3L, 4L), equalTo(7L));
        assertThat(Add2.processLong(-3L, 4L), equalTo(1L));
        assertThat(Add2.processLong(3L, -4L), equalTo(-1L));
        assertThat(Add2.processLong(-3L, -4L), equalTo(-7L));
        assertThat(Add2.processLong(0L, 0L), equalTo(0L));
    }

    public void testProcessInt() {
        assertThat(Add2.processInt(3, 4), equalTo(7));
        assertThat(Add2.processInt(-3, 4), equalTo(1));
        assertThat(Add2.processInt(3, -4), equalTo(-1));
        assertThat(Add2.processInt(-3, -4), equalTo(-7));
        assertThat(Add2.processInt(0, 0), equalTo(0));
    }

    /**
     * Test type resolution for valid numeric types.
     */
    public void testTypeResolution() {
        // Test with integer
        Add2 add2Int = new Add2(
            Source.EMPTY,
            new Literal(Source.EMPTY, 3, DataType.INTEGER),
            new Literal(Source.EMPTY, 4, DataType.INTEGER)
        );
        assertThat(add2Int.dataType(), equalTo(DataType.INTEGER));
        assertThat(add2Int.resolveType(), equalTo(Add2.TypeResolution.TYPE_RESOLVED));

        // Test with long
        Add2 add2Long = new Add2(Source.EMPTY, new Literal(Source.EMPTY, 3L, DataType.LONG), new Literal(Source.EMPTY, 4L, DataType.LONG));
        assertThat(add2Long.dataType(), equalTo(DataType.LONG));
        assertThat(add2Long.resolveType(), equalTo(Add2.TypeResolution.TYPE_RESOLVED));

        // Test with double
        Add2 add2Double = new Add2(
            Source.EMPTY,
            new Literal(Source.EMPTY, 3.0, DataType.DOUBLE),
            new Literal(Source.EMPTY, 4.0, DataType.DOUBLE)
        );
        assertThat(add2Double.dataType(), equalTo(DataType.DOUBLE));
        assertThat(add2Double.resolveType(), equalTo(Add2.TypeResolution.TYPE_RESOLVED));
    }

    /**
     * Test that invalid types are rejected.
     */
    public void testInvalidTypeResolution() {
        // Test with boolean (invalid for add2)
        Add2 add2Bool = new Add2(
            Source.EMPTY,
            new Literal(Source.EMPTY, true, DataType.BOOLEAN),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN)
        );
        Add2.TypeResolution resolution = add2Bool.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("Expected numeric type"));
    }

    /**
     * Test that mismatched types are rejected.
     */
    public void testMismatchedTypeResolution() {
        // Test with integer and long (mismatched)
        Add2 add2Mismatch = new Add2(
            Source.EMPTY,
            new Literal(Source.EMPTY, 3, DataType.INTEGER),
            new Literal(Source.EMPTY, 4L, DataType.LONG)
        );
        Add2.TypeResolution resolution = add2Mismatch.resolveType();
        assertThat(resolution.resolved(), equalTo(false));
        assertTrue(resolution.message().contains("must match"));
    }

    /**
     * Test edge cases for numeric values.
     * Note: Add2 now uses Math.addExact() and NumericUtils.asFiniteNumber()
     * to match compile-time Add behavior, so overflow throws ArithmeticException.
     */
    public void testEdgeCases() {
        // Test with very large double (but no overflow)
        assertThat(Add2.processDouble(Double.MAX_VALUE, 0.0), equalTo(Double.MAX_VALUE));

        // Test double overflow throws ArithmeticException (matches compile-time Add)
        expectThrows(ArithmeticException.class, () -> Add2.processDouble(Double.MAX_VALUE, Double.MAX_VALUE));

        // Test with infinity throws ArithmeticException
        expectThrows(ArithmeticException.class, () -> Add2.processDouble(Double.POSITIVE_INFINITY, 1.0));
        expectThrows(ArithmeticException.class, () -> Add2.processDouble(Double.NEGATIVE_INFINITY, 1.0));

        // Test with NaN throws ArithmeticException
        expectThrows(ArithmeticException.class, () -> Add2.processDouble(Double.NaN, 1.0));
        expectThrows(ArithmeticException.class, () -> Add2.processDouble(1.0, Double.NaN));

        // Test integer overflow throws ArithmeticException (matches compile-time Add)
        expectThrows(ArithmeticException.class, () -> Add2.processInt(Integer.MAX_VALUE, 1));
        expectThrows(ArithmeticException.class, () -> Add2.processLong(Long.MAX_VALUE, 1L));
    }

    /**
     * Test accessors.
     */
    public void testAccessors() {
        Literal left = new Literal(Source.EMPTY, 3, DataType.INTEGER);
        Literal right = new Literal(Source.EMPTY, 4, DataType.INTEGER);
        Add2 add2 = new Add2(Source.EMPTY, left, right);

        assertThat(add2.left(), equalTo(left));
        assertThat(add2.right(), equalTo(right));
    }
}
