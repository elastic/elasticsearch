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

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.test.ESTestCase;

public class AnalyzerCasterTests extends ESTestCase {

    private static void assertCast(Class<?> actual, Class<?> expected, boolean mustBeExplicit) {
        Location location = new Location("dummy", 0);

        if (actual.equals(expected)) {
            assertFalse(mustBeExplicit);
            assertNull(AnalyzerCaster.getLegalCast(location, actual, expected, false, false));
            assertNull(AnalyzerCaster.getLegalCast(location, actual, expected, true, false));
            return;
        }

        PainlessCast cast = AnalyzerCaster.getLegalCast(location, actual, expected, true, false);
        assertEquals(actual, cast.originalType);
        assertEquals(expected, cast.targetType);

        if (mustBeExplicit) {
            ClassCastException error = expectThrows(ClassCastException.class,
                    () -> AnalyzerCaster.getLegalCast(location, actual, expected, false, false));
            assertTrue(error.getMessage().startsWith("Cannot cast"));
        } else {
            cast = AnalyzerCaster.getLegalCast(location, actual, expected, false, false);
            assertEquals(actual, cast.originalType);
            assertEquals(expected, cast.targetType);
        }
    }

    public void testNumericCasts() {
        assertCast(byte.class, byte.class, false);
        assertCast(byte.class, short.class, false);
        assertCast(byte.class, int.class, false);
        assertCast(byte.class, long.class, false);
        assertCast(byte.class, float.class, false);
        assertCast(byte.class, double.class, false);

        assertCast(short.class, byte.class, true);
        assertCast(short.class, short.class, false);
        assertCast(short.class, int.class, false);
        assertCast(short.class, long.class, false);
        assertCast(short.class, float.class, false);
        assertCast(short.class, double.class, false);

        assertCast(int.class, byte.class, true);
        assertCast(int.class, short.class, true);
        assertCast(int.class, int.class, false);
        assertCast(int.class, long.class, false);
        assertCast(int.class, float.class, false);
        assertCast(int.class, double.class, false);

        assertCast(long.class, byte.class, true);
        assertCast(long.class, short.class, true);
        assertCast(long.class, int.class, true);
        assertCast(long.class, long.class, false);
        assertCast(long.class, float.class, false);
        assertCast(long.class, double.class, false);

        assertCast(float.class, byte.class, true);
        assertCast(float.class, short.class, true);
        assertCast(float.class, int.class, true);
        assertCast(float.class, long.class, true);
        assertCast(float.class, float.class, false);
        assertCast(float.class, double.class, false);

        assertCast(double.class, byte.class, true);
        assertCast(double.class, short.class, true);
        assertCast(double.class, int.class, true);
        assertCast(double.class, long.class, true);
        assertCast(double.class, float.class, true);
        assertCast(double.class, double.class, false);
    }

}
