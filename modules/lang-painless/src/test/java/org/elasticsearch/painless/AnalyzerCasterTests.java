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

public class AnalyzerCasterTests extends ESTestCase {

    private static void assertCast(Type actual, Type expected, boolean mustBeExplicit) {
        Location location = new Location("dummy", 0);

        if (actual.equals(expected)) {
            assertFalse(mustBeExplicit);
            assertNull(Definition.DEFINITION.caster.getLegalCast(location, actual, expected, false, false));
            assertNull(Definition.DEFINITION.caster.getLegalCast(location, actual, expected, true, false));
            return;
        }

        Cast cast = Definition.DEFINITION.caster.getLegalCast(location, actual, expected, true, false);
        assertEquals(actual, cast.from);
        assertEquals(expected, cast.to);

        if (mustBeExplicit) {
            ClassCastException error = expectThrows(ClassCastException.class,
                    () -> Definition.DEFINITION.caster.getLegalCast(location, actual, expected, false, false));
            assertTrue(error.getMessage().startsWith("Cannot cast"));
        } else {
            cast = Definition.DEFINITION.caster.getLegalCast(location, actual, expected, false, false);
            assertEquals(actual, cast.from);
            assertEquals(expected, cast.to);
        }
    }

    public void testNumericCasts() {
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.byteType, false);
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.shortType, false);
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.intType, false);
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.longType, false);
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.floatType, false);
        assertCast(Definition.DEFINITION.byteType, Definition.DEFINITION.doubleType, false);

        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.byteType, true);
        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.shortType, false);
        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.intType, false);
        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.longType, false);
        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.floatType, false);
        assertCast(Definition.DEFINITION.shortType, Definition.DEFINITION.doubleType, false);

        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.byteType, true);
        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.shortType, true);
        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.intType, false);
        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.longType, false);
        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.floatType, false);
        assertCast(Definition.DEFINITION.intType, Definition.DEFINITION.doubleType, false);

        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.byteType, true);
        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.shortType, true);
        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.intType, true);
        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.longType, false);
        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.floatType, false);
        assertCast(Definition.DEFINITION.longType, Definition.DEFINITION.doubleType, false);

        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.byteType, true);
        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.shortType, true);
        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.intType, true);
        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.longType, true);
        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.floatType, false);
        assertCast(Definition.DEFINITION.floatType, Definition.DEFINITION.doubleType, false);

        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.byteType, true);
        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.shortType, true);
        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.intType, true);
        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.longType, true);
        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.floatType, true);
        assertCast(Definition.DEFINITION.doubleType, Definition.DEFINITION.doubleType, false);
    }

}
