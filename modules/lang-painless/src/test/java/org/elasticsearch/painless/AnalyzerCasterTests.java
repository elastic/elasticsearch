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
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.test.ESTestCase;

public class AnalyzerCasterTests extends ESTestCase {

    private static final Definition definition = new Definition(Whitelist.BASE_WHITELISTS);

    private static void assertCast(Type actual, Type expected, boolean mustBeExplicit) {
        Location location = new Location("dummy", 0);

        if (actual.equals(expected)) {
            assertFalse(mustBeExplicit);
            assertNull(definition.caster.getLegalCast(location, actual, expected, false, false));
            assertNull(definition.caster.getLegalCast(location, actual, expected, true, false));
            return;
        }

        Cast cast = definition.caster.getLegalCast(location, actual, expected, true, false);
        assertEquals(actual, cast.from);
        assertEquals(expected, cast.to);

        if (mustBeExplicit) {
            ClassCastException error = expectThrows(ClassCastException.class,
                    () -> definition.caster.getLegalCast(location, actual, expected, false, false));
            assertTrue(error.getMessage().startsWith("Cannot cast"));
        } else {
            cast = definition.caster.getLegalCast(location, actual, expected, false, false);
            assertEquals(actual, cast.from);
            assertEquals(expected, cast.to);
        }
    }

    public void testNumericCasts() {
        assertCast(definition.byteType, definition.byteType, false);
        assertCast(definition.byteType, definition.shortType, false);
        assertCast(definition.byteType, definition.intType, false);
        assertCast(definition.byteType, definition.longType, false);
        assertCast(definition.byteType, definition.floatType, false);
        assertCast(definition.byteType, definition.doubleType, false);

        assertCast(definition.shortType, definition.byteType, true);
        assertCast(definition.shortType, definition.shortType, false);
        assertCast(definition.shortType, definition.intType, false);
        assertCast(definition.shortType, definition.longType, false);
        assertCast(definition.shortType, definition.floatType, false);
        assertCast(definition.shortType, definition.doubleType, false);

        assertCast(definition.intType, definition.byteType, true);
        assertCast(definition.intType, definition.shortType, true);
        assertCast(definition.intType, definition.intType, false);
        assertCast(definition.intType, definition.longType, false);
        assertCast(definition.intType, definition.floatType, false);
        assertCast(definition.intType, definition.doubleType, false);

        assertCast(definition.longType, definition.byteType, true);
        assertCast(definition.longType, definition.shortType, true);
        assertCast(definition.longType, definition.intType, true);
        assertCast(definition.longType, definition.longType, false);
        assertCast(definition.longType, definition.floatType, false);
        assertCast(definition.longType, definition.doubleType, false);

        assertCast(definition.floatType, definition.byteType, true);
        assertCast(definition.floatType, definition.shortType, true);
        assertCast(definition.floatType, definition.intType, true);
        assertCast(definition.floatType, definition.longType, true);
        assertCast(definition.floatType, definition.floatType, false);
        assertCast(definition.floatType, definition.doubleType, false);

        assertCast(definition.doubleType, definition.byteType, true);
        assertCast(definition.doubleType, definition.shortType, true);
        assertCast(definition.doubleType, definition.intType, true);
        assertCast(definition.doubleType, definition.longType, true);
        assertCast(definition.doubleType, definition.floatType, true);
        assertCast(definition.doubleType, definition.doubleType, false);
    }

}
