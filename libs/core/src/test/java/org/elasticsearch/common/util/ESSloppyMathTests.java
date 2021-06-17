/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.core.ESSloppyMath.atan;
import static org.elasticsearch.core.ESSloppyMath.sinh;

public class ESSloppyMathTests extends ESTestCase {

    // accuracy for atan(x)
    static double ATAN_DELTA = 1E-15;
    // accuracy for sinh(x)
    static double SINH_DELTA = 1E-15; // for small x

    public void testAtan() {
        assertTrue(Double.isNaN(atan(Double.NaN)));
        assertEquals(-Math.PI/2, atan(Double.NEGATIVE_INFINITY), ATAN_DELTA);
        assertEquals(Math.PI/2, atan(Double.POSITIVE_INFINITY), ATAN_DELTA);
        for (int i = 0; i < 10000; i++) {
            assertEquals(StrictMath.atan(i), atan(i), ATAN_DELTA);
            assertEquals(StrictMath.atan(-i), atan(-i), ATAN_DELTA);
        }
    }

    public void testSinh() {
        assertTrue(Double.isNaN(sinh(Double.NaN)));
        assertEquals(Double.NEGATIVE_INFINITY, sinh(Double.NEGATIVE_INFINITY), SINH_DELTA);
        assertEquals(Double.POSITIVE_INFINITY, sinh(Double.POSITIVE_INFINITY), SINH_DELTA);
        for (int i = 0; i < 10000; i++) {
            double d = randomDoubleBetween(-2 * Math.PI, 2 * Math.PI, true);
            if (random().nextBoolean()) {
                d = -d;
            }
            assertEquals(StrictMath.sinh(d), sinh(d), SINH_DELTA);
        }
    }
}
