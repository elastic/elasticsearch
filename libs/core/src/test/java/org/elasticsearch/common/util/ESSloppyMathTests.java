/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.util.ESSloppyMath.atan;
import static org.elasticsearch.common.util.ESSloppyMath.sinh;

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
