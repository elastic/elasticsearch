/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.math;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;


public class ToNumberFunctionProcessorTests extends ESTestCase {

    private static Object process(Object value, Object base) {
        return new ToNumber(EMPTY, l(value), l(base)).makePipe().asProcessor().process(null);
    }

    private static String error(Object value, Object base) {
        QlIllegalArgumentException saie = expectThrows(QlIllegalArgumentException.class,
            () -> new ToNumber(EMPTY, l(value), l(base)).makePipe().asProcessor().process(null));
        return saie.getMessage();
    }

    public void toNumberWithLongRange() {
        long number = randomLongBetween(Integer.MAX_VALUE, Long.MAX_VALUE);

        assertEquals(number, process(Long.toString(number), null));
        assertEquals(number, process("0x" + Long.toHexString(number), null));

        assertEquals(number, process(Long.toString(number), 10));
        assertEquals(number, process(Long.toOctalString(number), 8));
        assertEquals(number, process(Long.toHexString(number), 16));
        assertEquals(number, process("0x" + Long.toHexString(number), 16));
    }

    public void toNumberWithPositiveInteger() {
        int number = randomIntBetween(0, 1000);

        assertEquals(number, process(Integer.toString(number), null));
        assertEquals(number, process("0x" + Integer.toHexString(number), null));

        assertEquals(number, process(Integer.toString(number), 10));
        assertEquals(number, process(Integer.toOctalString(number), 8));
        assertEquals(number, process(Integer.toHexString(number), 16));
        assertEquals(number, process("0x" + Integer.toHexString(number), 16));
    }

    public void toNumberWithNegativeInteger() {
        int posInt = randomIntBetween(1, 1000);
        int negInt = -posInt;

        assertEquals(negInt, process(Integer.toString(negInt), null));

        assertEquals(negInt, process(Integer.toString(negInt), 10));
        assertEquals(negInt, process("-" + Integer.toOctalString(posInt), 8));
        assertEquals(negInt, process("-" + Integer.toHexString(posInt), 16));

        assertEquals(negInt, process("-0x" + Integer.toHexString(posInt), 16));
    }

    public void toNumberWithPositiveFloat() {
        double number = randomDoubleBetween(0.0, 1000.0, true);

        assertEquals(number, process(Double.toString(number), null));
        assertEquals(number, process(Double.toString(number), 10));
    }

    public void toNumberWithNegativeFloat() {
        double number = randomDoubleBetween(-1000.0, -0.1, true);

        assertEquals(number, process(Double.toString(number), null));
        assertEquals(number, process(Double.toString(number), 10));
    }

    public void toNumberWithMissingInput() {
        assertNull(process(null, null));
        assertNull(process(null, 8));
        assertNull(process(null, 10));
        assertNull(process(null, 16));
    }

    public void toNumberWithPositiveExponent() {
        int number = randomIntBetween(-100, 100);
        int exponent = randomIntBetween(0, 20);

        double expected = Math.pow((double) number, (double) exponent);

        assertEquals(expected, process(number  + "e" + exponent, null));
        assertEquals(expected, process(number  + "e" + exponent, 10));
    }

    public void toNumberWithNegativeExponent() {
        int number = randomIntBetween(-100, 100);
        int exponent = randomIntBetween(-10, -1);

        double expected = Math.pow(number, exponent);

        assertEquals(expected, process(number  + "e-" + exponent, null));
        assertEquals(expected, process(number  + "e-" + exponent, 10));
    }

    public void toNumberWithLocales() {
        assertEquals("Unable to convert [1,000] to number of base [10]",
            error("1,000", 7));
        assertEquals("Unable to convert [1,000] to number of base [10]",
            error("1,000,000", 7));
        assertEquals("Unable to convert [1,000] to number of base [10]",
            error("1.000.000", 7));
        assertEquals("Unable to convert [1,000] to number of base [10]",
            error("1,000.000.000", 7));
    }

    public void toNumberWithUnsupportedDoubleBase() {
        // test that only base 10 fractions are supported
        double decimal = randomDouble();
        assertEquals("Unable to convert [1.0] to number of base [7]",
            error(Double.toString(decimal), 7));
        assertEquals("Unable to convert [1.0] to number of base [8]",
            error(Double.toString(decimal), 8));
        assertEquals("Unable to convert [1.0] to number of base [16]",
            error(Double.toString(decimal), 16));
    }

    public void testNegativeBase16() {
        assertEquals("Unable to convert [-0x1] to number of base [16]",
            error("-0x1", 16));
    }

    public void testNumberInvalidDataType() {
        assertEquals("A string/char is required; received [false]",
            error(false, null));
        assertEquals("A string/char is required; received [1.0]",
            error(1.0, null));
        assertEquals("A string/char is required; received [1]",
            error(1, null));
    }

    public void testInvalidBase() {
        int number = randomIntBetween(-100, 100);

        assertEquals("An integer base is required; received [foo]",
            error(Integer.toString(number), "foo"));
        assertEquals("An integer base is required; received [1.0]",
            error(Integer.toString(number), 1.0));
        assertEquals("An integer base is required; received [false]",
            error(Integer.toString(number), false));
    }

    public void testInvalidSourceString() {
        assertEquals("Unable to convert [] to number of base [10]",
            error("", null));
        assertEquals("Unable to convert [] to number of base [16]",
            error("", 16));
        assertEquals("Unable to convert [foo] to number of base [10]",
            error("foo", null));
        assertEquals("Unable to convert [foo] to number of base [16]",
            error("foo", 16));
        assertEquals("Unable to convert [1.2.3.4] to number of base [10]",
            error("1.2.3.4", 10));
        assertEquals("Unable to convert [1.2.3.4] to number of base [16]",
            error("1.2.3.4", 16));
    }
}
