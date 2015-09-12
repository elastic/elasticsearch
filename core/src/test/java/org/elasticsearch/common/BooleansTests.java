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
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BooleansTests extends ESTestCase {

    @Test
    public void testIsBoolean() {
        String[] booleans = new String[]{"true", "false", "on", "off", "yes", "no", "0", "1"};
        String[] notBooleans = new String[]{"11", "00", "sdfsdfsf", "F", "T"};
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String b : booleans) {
            String t = "prefix" + b + "suffix";
            assertThat("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()), Matchers.equalTo(true));
        }

        for (String nb : notBooleans) {
            String t = "prefix" + nb + "suffix";
            assertThat("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()), Matchers.equalTo(false));
        }
    }
    @Test
    public void parseBoolean() {
        assertThat(Booleans.parseBoolean(randomFrom("true", "on", "yes", "1"), randomBoolean()), is(true));
        assertThat(Booleans.parseBoolean(randomFrom("false", "off", "no", "0"), randomBoolean()), is(false));
        assertThat(Booleans.parseBoolean(randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT), randomBoolean()), is(true));
        assertThat(Booleans.parseBoolean(null, false), is(false));
        assertThat(Booleans.parseBoolean(null, true), is(true));

        assertThat(Booleans.parseBoolean(randomFrom("true", "on", "yes", "1"), randomFrom(null, Boolean.TRUE, Boolean.FALSE)), is(true));
        assertThat(Booleans.parseBoolean(randomFrom("false", "off", "no", "0"), randomFrom(null, Boolean.TRUE, Boolean.FALSE)), is(false));
        assertThat(Booleans.parseBoolean(randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT),randomFrom(null, Boolean.TRUE, Boolean.FALSE)), is(true));
        assertThat(Booleans.parseBoolean(null, Boolean.FALSE), is(false));
        assertThat(Booleans.parseBoolean(null, Boolean.TRUE), is(true));
        assertThat(Booleans.parseBoolean(null, null), nullValue());

        char[] chars = randomFrom("true", "on", "yes", "1").toCharArray();
        assertThat(Booleans.parseBoolean(chars, 0, chars.length, randomBoolean()), is(true));
        chars = randomFrom("false", "off", "no", "0").toCharArray();
        assertThat(Booleans.parseBoolean(chars,0, chars.length, randomBoolean()), is(false));
        chars = randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT).toCharArray();
        assertThat(Booleans.parseBoolean(chars,0, chars.length, randomBoolean()), is(true));
    }

    @Test
    public void parseBooleanExact() {
        assertThat(Booleans.parseBooleanExact(randomFrom("true", "on", "yes", "1")), is(true));
        assertThat(Booleans.parseBooleanExact(randomFrom("false", "off", "no", "0")), is(false));
        try {
            Booleans.parseBooleanExact(randomFrom(null, "fred", "foo", "barney"));
            fail("Expected exception while parsing invalid boolean value ");
        } catch (Exception ex) {
            assertTrue(ex instanceof IllegalArgumentException);
        }
    }

    public void testIsExplicit() {
        assertThat(Booleans.isExplicitFalse(randomFrom("true", "on", "yes", "1", "foo", null)), is(false));
        assertThat(Booleans.isExplicitFalse(randomFrom("false", "off", "no", "0")), is(true));
        assertThat(Booleans.isExplicitTrue(randomFrom("true", "on", "yes", "1")), is(true));
        assertThat(Booleans.isExplicitTrue(randomFrom("false", "off", "no", "0", "foo", null)), is(false));
    }
}
