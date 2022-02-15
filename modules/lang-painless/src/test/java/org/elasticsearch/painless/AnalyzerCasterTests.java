/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
            ClassCastException error = expectThrows(
                ClassCastException.class,
                () -> AnalyzerCaster.getLegalCast(location, actual, expected, false, false)
            );
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
