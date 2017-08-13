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

import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.painless.Definition.BYTE_TYPE;
import static org.elasticsearch.painless.Definition.DOUBLE_TYPE;
import static org.elasticsearch.painless.Definition.FLOAT_TYPE;
import static org.elasticsearch.painless.Definition.INT_TYPE;
import static org.elasticsearch.painless.Definition.LONG_TYPE;
import static org.elasticsearch.painless.Definition.SHORT_TYPE;

public class AnalyzerCasterTests extends ESTestCase {

    private static void assertCast(Type actual, Type expected, boolean mustBeExplicit) {
        Location location = new Location("dummy", 0);

        if (actual.equals(expected)) {
            assertFalse(mustBeExplicit);
            assertNull(AnalyzerCaster.getLegalCast(location, actual, expected, false, false));
            assertNull(AnalyzerCaster.getLegalCast(location, actual, expected, true, false));
            return;
        }

        Cast cast = AnalyzerCaster.getLegalCast(location, actual, expected, true, false);
        assertEquals(actual, cast.from);
        assertEquals(expected, cast.to);

        if (mustBeExplicit) {
            ClassCastException error = expectThrows(ClassCastException.class,
                    () -> AnalyzerCaster.getLegalCast(location, actual, expected, false, false));
            assertTrue(error.getMessage().startsWith("Cannot cast"));
        } else {
            cast = AnalyzerCaster.getLegalCast(location, actual, expected, false, false);
            assertEquals(actual, cast.from);
            assertEquals(expected, cast.to);
        }
    }

    public void testNumericCasts() {
        assertCast(BYTE_TYPE, BYTE_TYPE, false);
        assertCast(BYTE_TYPE, SHORT_TYPE, false);
        assertCast(BYTE_TYPE, INT_TYPE, false);
        assertCast(BYTE_TYPE, LONG_TYPE, false);
        assertCast(BYTE_TYPE, FLOAT_TYPE, false);
        assertCast(BYTE_TYPE, DOUBLE_TYPE, false);

        assertCast(SHORT_TYPE, BYTE_TYPE, true);
        assertCast(SHORT_TYPE, SHORT_TYPE, false);
        assertCast(SHORT_TYPE, INT_TYPE, false);
        assertCast(SHORT_TYPE, LONG_TYPE, false);
        assertCast(SHORT_TYPE, FLOAT_TYPE, false);
        assertCast(SHORT_TYPE, DOUBLE_TYPE, false);

        assertCast(INT_TYPE, BYTE_TYPE, true);
        assertCast(INT_TYPE, SHORT_TYPE, true);
        assertCast(INT_TYPE, INT_TYPE, false);
        assertCast(INT_TYPE, LONG_TYPE, false);
        assertCast(INT_TYPE, FLOAT_TYPE, false);
        assertCast(INT_TYPE, DOUBLE_TYPE, false);

        assertCast(LONG_TYPE, BYTE_TYPE, true);
        assertCast(LONG_TYPE, SHORT_TYPE, true);
        assertCast(LONG_TYPE, INT_TYPE, true);
        assertCast(LONG_TYPE, LONG_TYPE, false);
        assertCast(LONG_TYPE, FLOAT_TYPE, false);
        assertCast(LONG_TYPE, DOUBLE_TYPE, false);

        assertCast(FLOAT_TYPE, BYTE_TYPE, true);
        assertCast(FLOAT_TYPE, SHORT_TYPE, true);
        assertCast(FLOAT_TYPE, INT_TYPE, true);
        assertCast(FLOAT_TYPE, LONG_TYPE, true);
        assertCast(FLOAT_TYPE, FLOAT_TYPE, false);
        assertCast(FLOAT_TYPE, DOUBLE_TYPE, false);

        assertCast(DOUBLE_TYPE, BYTE_TYPE, true);
        assertCast(DOUBLE_TYPE, SHORT_TYPE, true);
        assertCast(DOUBLE_TYPE, INT_TYPE, true);
        assertCast(DOUBLE_TYPE, LONG_TYPE, true);
        assertCast(DOUBLE_TYPE, FLOAT_TYPE, true);
        assertCast(DOUBLE_TYPE, DOUBLE_TYPE, false);
    }

}
