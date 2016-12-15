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
    public void testIsBoolean() {
        String[] booleans = new String[]{"true", "false"};
        String[] notBooleans = new String[]{"11", "00", "sdfsdfsf", "F", "T", "on", "off", "yes", "no", "0", "1"};
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String b : booleans) {
            String t = "prefix" + b + "suffix";
            assertThat("failed to recognize [" + b + "] as boolean",
                Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()), is(true));
        }

        for (String nb : notBooleans) {
            String t = "prefix" + nb + "suffix";
            assertThat("recognized [" + nb + "] as boolean",
                Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()), is(false));
        }
    }

    public void testParseBoolean() {
        assertThat(Booleans.parseBoolean(null, false), is(false));
        assertThat(Booleans.parseBoolean(null, true), is(true));

        assertThat(Booleans.parseBoolean("true", randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(true));
        assertThat(Booleans.parseBoolean("false", randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(false));
        expectThrows(IllegalArgumentException.class,
            () -> Booleans.parseBoolean("true".toUpperCase(Locale.ROOT),randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
        assertThat(Booleans.parseBoolean(null, Boolean.FALSE), is(false));
        assertThat(Booleans.parseBoolean(null, Boolean.TRUE), is(true));
        assertThat(Booleans.parseBoolean(null, null), nullValue());

        char[] chars = "true".toCharArray();
        assertThat(Booleans.parseBoolean(chars, 0, chars.length, randomBoolean()), is(true));
        chars = "false".toCharArray();
        assertThat(Booleans.parseBoolean(chars,0, chars.length, randomBoolean()), is(false));
        final char[] TRUE = "true".toUpperCase(Locale.ROOT).toCharArray();
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(TRUE, 0, TRUE.length, randomBoolean()));
    }

    public void testParseBooleanExact() {
        assertThat(Booleans.parseBoolean("true"), is(true));
        assertThat(Booleans.parseBoolean("false"), is(false));
        try {
            Booleans.parseBoolean(randomFrom("fred", "foo", "barney", null, "on", "yes", "1", "off", "no", "0", "True", "False"));
            fail("Expected exception while parsing invalid boolean value");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
    }

    public void testIsExplicit() {
        assertThat(Booleans.isExplicitFalse(randomFrom("true", "on", "yes", "1", "foo", null, "False")), is(false));
        assertThat(Booleans.isExplicitFalse("false"), is(true));
        assertThat(Booleans.isExplicitTrue("true"), is(true));
        assertThat(Booleans.isExplicitTrue(randomFrom("false", "off", "no", "0", "foo", null, "True")), is(false));
    }
}
