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

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BooleansTests extends ESTestCase {
    private static final String[] NON_BOOLEANS = new String[]{"11", "00", "sdfsdfsf", "F", "T", "True", "False"};
    private static final String[] BOOLEANS = new String[]{"true", "false", "on", "off", "yes", "no", "0", "1"};
    private static final String[] TRUTHY = new String[]{"true", "on", "yes", "1"};
    private static final String[] FALSY = new String[]{"false", "off", "no", "0"};

    public void testIsNonBoolean() {
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String nb : NON_BOOLEANS) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isStrictlyBoolean(t));
        }
    }

    public void testParseBooleanWithFallback() {
        assertFalse(Booleans.parseBoolean(null, false));
        assertTrue(Booleans.parseBoolean(null, true));
        assertNull(Booleans.parseBoolean(null, null));
        assertFalse(Booleans.parseBoolean(null, Boolean.FALSE));
        assertTrue(Booleans.parseBoolean(null, Boolean.TRUE));

        assertTrue(Booleans.parseBoolean("true", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
        assertFalse(Booleans.parseBoolean("false", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
    }

    public void testParseBooleanExact() {
        assertTrue(Booleans.parseBooleanExact(randomFrom(TRUTHY)));
        assertFalse(Booleans.parseBooleanExact(randomFrom(FALSY)));
    }

    public void testParseNonBooleanExact() {
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBooleanExact(null));
        for (String nonBoolean : NON_BOOLEANS) {
            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBooleanExact(nonBoolean));
        }
    }

    public void testParseNonBooleanExactWithFallback() {
        for (String nonBoolean : NON_BOOLEANS) {
            boolean defaultValue = randomFrom(Boolean.TRUE, Boolean.FALSE);

            expectThrows(IllegalArgumentException.class,
                () -> Booleans.parseBooleanExact(nonBoolean, defaultValue));
        }
    }

    public void testIsBoolean() {
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            assertTrue("failed to recognize [" + b + "] as boolean",
                Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()));
            assertTrue("failed to recognize [" + b + "] as boolean",
                Booleans.isBoolean(new String(t.toCharArray(), "prefix".length(), b.length())));
        }

        for (String nb : NON_BOOLEANS) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean",
                Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean",
                Booleans.isBoolean(new String(t.toCharArray(), "prefix".length(), nb.length())));
        }
    }

    public void testParseBoolean() {
        assertThat(Booleans.parseBoolean(randomFrom(TRUTHY), randomBoolean()), is(true));
        assertThat(Booleans.parseBoolean(randomFrom(FALSY), randomBoolean()), is(false));
        assertThat(Booleans.parseBoolean(randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT), randomBoolean()), is(true));
        assertThat(Booleans.parseBoolean(null, false), is(false));
        assertThat(Booleans.parseBoolean(null, true), is(true));

        assertThat(Booleans.parseBoolean(
            randomFrom("true", "on", "yes", "1"), randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(true));
        assertThat(Booleans.parseBoolean(
            randomFrom("false", "off", "no", "0"), randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(false));
        assertThat(Booleans.parseBoolean(
            randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT),randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(true));
        assertThat(Booleans.parseBoolean(null, Boolean.FALSE), is(false));
        assertThat(Booleans.parseBoolean(null, Boolean.TRUE), is(true));
        assertThat(Booleans.parseBoolean(null, null), nullValue());
    }
}
