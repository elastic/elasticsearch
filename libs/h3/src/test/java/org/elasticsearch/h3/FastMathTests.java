/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.h3;

import org.elasticsearch.test.ESTestCase;

import java.util.function.DoubleSupplier;
import java.util.function.DoubleUnaryOperator;

public class FastMathTests extends ESTestCase {

    // accuracy for cos(x)
    static double COS_DELTA = 1E-15;
    // accuracy for sin(x)
    static double SIN_DELTA = 1E-15;
    // accuracy for asin(x)
    static double ASIN_DELTA = 1E-14;
    // accuracy for acos(x)
    static double ACOS_DELTA = 1E-14;
    // accuracy for tan(x)
    static double TAN_DELTA = 1E-14;
    // accuracy for atan(x)
    static double ATAN_DELTA = 1E-14;
    // accuracy for atan2(x)
    static double ATAN2_DELTA = 1E-14;

    public void testSin() {
        doTest(Math::sin, FastMath::sin, d -> SIN_DELTA, () -> randomDoubleBetween(-2 * Math.PI, 2 * Math.PI, true));
    }

    public void testCos() {
        doTest(Math::cos, FastMath::cos, d -> COS_DELTA, () -> randomDoubleBetween(-2 * Math.PI, 2 * Math.PI, true));
    }

    public void testTan() {
        doTest(
            Math::tan,
            FastMath::tan,
            d -> Math.max(TAN_DELTA, Math.abs(Math.tan(d)) * TAN_DELTA),
            () -> randomDoubleBetween(-2 * Math.PI, 2 * Math.PI, true)
        );
    }

    public void testAsin() {
        doTest(Math::asin, FastMath::asin, d -> ASIN_DELTA, () -> randomDoubleBetween(-2, 2, true));
    }

    public void testAcos() {
        doTest(Math::acos, FastMath::acos, d -> ACOS_DELTA, () -> randomDoubleBetween(-2, 2, true));
    }

    public void testAtan() {
        doTest(
            Math::atan,
            FastMath::atan,
            d -> ATAN_DELTA,
            () -> randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true)
        );
    }

    private void doTest(DoubleUnaryOperator expected, DoubleUnaryOperator actual, DoubleUnaryOperator delta, DoubleSupplier supplier) {
        assertEquals(expected.applyAsDouble(Double.NaN), actual.applyAsDouble(Double.NaN), delta.applyAsDouble(Double.NaN));
        assertEquals(
            expected.applyAsDouble(Double.NEGATIVE_INFINITY),
            actual.applyAsDouble(Double.NEGATIVE_INFINITY),
            delta.applyAsDouble(Double.POSITIVE_INFINITY)
        );
        assertEquals(
            expected.applyAsDouble(Double.POSITIVE_INFINITY),
            actual.applyAsDouble(Double.POSITIVE_INFINITY),
            delta.applyAsDouble(Double.POSITIVE_INFINITY)
        );
        assertEquals(
            expected.applyAsDouble(Double.MAX_VALUE),
            actual.applyAsDouble(Double.MAX_VALUE),
            delta.applyAsDouble(Double.MAX_VALUE)
        );
        assertEquals(
            expected.applyAsDouble(Double.MIN_VALUE),
            actual.applyAsDouble(Double.MIN_VALUE),
            delta.applyAsDouble(Double.MIN_VALUE)
        );
        assertEquals(expected.applyAsDouble(0), actual.applyAsDouble(0), delta.applyAsDouble(0));
        for (int i = 0; i < 10000; i++) {
            double d = supplier.getAsDouble();
            assertEquals(expected.applyAsDouble(d), actual.applyAsDouble(d), delta.applyAsDouble(d));
        }
    }

    public void testAtan2() {
        assertEquals(Math.atan2(Double.NaN, Double.NaN), FastMath.atan2(Double.NaN, Double.NaN), ATAN2_DELTA);
        assertEquals(
            Math.atan2(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
            FastMath.atan2(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
            ATAN2_DELTA
        );
        assertEquals(
            Math.atan2(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
            FastMath.atan2(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
            ATAN2_DELTA
        );
        assertEquals(Math.atan2(Double.MAX_VALUE, Double.MAX_VALUE), FastMath.atan2(Double.MAX_VALUE, Double.MAX_VALUE), ATAN2_DELTA);
        assertEquals(Math.atan2(Double.MIN_VALUE, Double.MIN_VALUE), FastMath.atan2(Double.MIN_VALUE, Double.MIN_VALUE), ATAN2_DELTA);
        assertEquals(Math.atan2(0, 0), FastMath.atan2(0, 0), ATAN2_DELTA);
        for (int i = 0; i < 10000; i++) {
            double x = randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true);
            double y = randomDoubleBetween(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true);
            assertEquals(Math.atan2(x, y), FastMath.atan2(x, y), ATAN2_DELTA);
        }
    }
}
