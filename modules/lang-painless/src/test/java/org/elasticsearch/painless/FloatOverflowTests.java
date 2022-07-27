/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

/** Tests floating point overflow cases */
public class FloatOverflowTests extends ScriptTestCase {

    public void testAssignmentAdditionOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x += 3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; x += -3.4028234663852886E38f; return x;"));

        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x += 1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = -1.7976931348623157E308; x += -1.7976931348623157E308; return x;"));
    }

    public void testAssignmentSubtractionOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x -= -3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; x -= 3.4028234663852886E38f; return x;"));

        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x -= -1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = -1.7976931348623157E308; x -= 1.7976931348623157E308; return x;"));
    }

    public void testAssignmentMultiplicationOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x *= 3.4028234663852886E38f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x *= -3.4028234663852886E38f; return x;"));

        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x *= 1.7976931348623157E308; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = 1.7976931348623157E308; x *= -1.7976931348623157E308; return x;"));
    }

    public void testAssignmentDivisionOverflow() {
        // float
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x /= 1.401298464324817E-45f; return x;"));
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = 3.4028234663852886E38f; x /= -1.401298464324817E-45f; return x;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 1.0f; x /= 0.0f; return x;"));

        // double
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; x /= 4.9E-324; return x;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("double x = 1.7976931348623157E308; x /= -4.9E-324; return x;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.0f; x /= 0.0; return x;"));
    }

    public void testAddition() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x + y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x + y;"));
    }

    public void testAdditionConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f + 3.4028234663852886E38f;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 + 1.7976931348623157E308;"));
    }

    public void testSubtraction() throws Exception {
        assertEquals(Float.NEGATIVE_INFINITY, exec("float x = -3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x - y;"));
        assertEquals(
            Double.NEGATIVE_INFINITY,
            exec("double x = -1.7976931348623157E308; double y = 1.7976931348623157E308; return x - y;")
        );
    }

    public void testSubtractionConst() throws Exception {
        assertEquals(Float.NEGATIVE_INFINITY, exec("return -3.4028234663852886E38f - 3.4028234663852886E38f;"));
        assertEquals(Double.NEGATIVE_INFINITY, exec("return -1.7976931348623157E308 - 1.7976931348623157E308;"));
    }

    public void testMultiplication() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 3.4028234663852886E38f; return x * y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 1.7976931348623157E308; return x * y;"));
    }

    public void testMultiplicationConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f * 3.4028234663852886E38f;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 * 1.7976931348623157E308;"));
    }

    public void testDivision() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 3.4028234663852886E38f; float y = 1.401298464324817E-45f; return x / y;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("float x = 1.0f; float y = 0.0f; return x / y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.7976931348623157E308; double y = 4.9E-324; return x / y;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("double x = 1.0; double y = 0.0; return x / y;"));
    }

    public void testDivisionConst() throws Exception {
        assertEquals(Float.POSITIVE_INFINITY, exec("return 3.4028234663852886E38f / 1.401298464324817E-45f;"));
        assertEquals(Float.POSITIVE_INFINITY, exec("return 1.0f / 0.0f;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.7976931348623157E308 / 4.9E-324;"));
        assertEquals(Double.POSITIVE_INFINITY, exec("return 1.0 / 0.0;"));
    }

    public void testDivisionNaN() throws Exception {
        // float division, constant division, and assignment
        assertTrue(Float.isNaN((Float) exec("float x = 0f; float y = 0f; return x / y;")));
        assertTrue(Float.isNaN((Float) exec("return 0f / 0f;")));
        assertTrue(Float.isNaN((Float) exec("float x = 0f; x /= 0f; return x;")));

        // double division, constant division, and assignment
        assertTrue(Double.isNaN((Double) exec("double x = 0.0; double y = 0.0; return x / y;")));
        assertTrue(Double.isNaN((Double) exec("return 0.0 / 0.0;")));
        assertTrue(Double.isNaN((Double) exec("double x = 0.0; x /= 0.0; return x;")));
    }

    public void testRemainderNaN() throws Exception {
        // float division, constant division, and assignment
        assertTrue(Float.isNaN((Float) exec("float x = 1f; float y = 0f; return x % y;")));
        assertTrue(Float.isNaN((Float) exec("return 1f % 0f;")));
        assertTrue(Float.isNaN((Float) exec("float x = 1f; x %= 0f; return x;")));

        // double division, constant division, and assignment
        assertTrue(Double.isNaN((Double) exec("double x = 1.0; double y = 0.0; return x % y;")));
        assertTrue(Double.isNaN((Double) exec("return 1.0 % 0.0;")));
        assertTrue(Double.isNaN((Double) exec("double x = 1.0; x %= 0.0; return x;")));
    }
}
