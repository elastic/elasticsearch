/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BooleansTests extends ESTestCase {
    private static final String[] NON_BOOLEANS = new String[]{"11", "00", "sdfsdfsf", "F", "T", "on", "off", "yes", "no", "0", "1",
        "True", "False"};
    private static final String[] BOOLEANS = new String[]{"true", "false"};

    public void testIsBoolean() {
        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            assertTrue("failed to recognize [" + b + "] as boolean",
                Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()));
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(b));
        }
    }

    public void testIsNonBoolean() {
        assertThat(Booleans.isBoolean(null, 0, 1), is(false));

        for (String nb : NON_BOOLEANS) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t));
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

    public void testParseNonBooleanWithFallback() {
        for (String nonBoolean : NON_BOOLEANS) {
            boolean defaultValue = randomFrom(Boolean.TRUE, Boolean.FALSE);

            expectThrows(IllegalArgumentException.class,
                () -> Booleans.parseBoolean(nonBoolean, defaultValue));
            expectThrows(IllegalArgumentException.class,
                () -> Booleans.parseBoolean(nonBoolean.toCharArray(), 0, nonBoolean.length(), defaultValue));
        }
    }

    public void testParseBoolean() {
        assertTrue(Booleans.parseBoolean("true"));
        assertFalse(Booleans.parseBoolean("false"));
    }

    public void testParseNonBoolean() {
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(null));
        for (String nonBoolean : NON_BOOLEANS) {
            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(nonBoolean));
        }
    }

    public void testIsBooleanLenient() {
        String[] booleans = new String[]{"true", "false", "on", "off", "yes", "no", "0", "1"};
        String[] notBooleans = new String[]{"11", "00", "sdfsdfsf", "F", "T"};
        assertThat(Booleans.isBooleanLenient(null, 0, 1), is(false));

        for (String b : booleans) {
            String t = "prefix" + b + "suffix";
            assertTrue("failed to recognize [" + b + "] as boolean",
                Booleans.isBooleanLenient(t.toCharArray(), "prefix".length(), b.length()));
        }

        for (String nb : notBooleans) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean",
                Booleans.isBooleanLenient(t.toCharArray(), "prefix".length(), nb.length()));
        }
    }

    public void testParseBooleanLenient() {
        assertThat(Booleans.parseBooleanLenient(randomFrom("true", "on", "yes", "1"), randomBoolean()), is(true));
        assertThat(Booleans.parseBooleanLenient(randomFrom("false", "off", "no", "0"), randomBoolean()), is(false));
        assertThat(Booleans.parseBooleanLenient(randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT), randomBoolean()), is(true));
        assertThat(Booleans.parseBooleanLenient(null, false), is(false));
        assertThat(Booleans.parseBooleanLenient(null, true), is(true));

        assertThat(Booleans.parseBooleanLenient(
            randomFrom("true", "on", "yes", "1"), randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(true));
        assertThat(Booleans.parseBooleanLenient(
            randomFrom("false", "off", "no", "0"), randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(false));
        assertThat(Booleans.parseBooleanLenient(
            randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT),randomFrom(Boolean.TRUE, Boolean.FALSE, null)), is(true));
        assertThat(Booleans.parseBooleanLenient(null, Boolean.FALSE), is(false));
        assertThat(Booleans.parseBooleanLenient(null, Boolean.TRUE), is(true));
        assertThat(Booleans.parseBooleanLenient(null, null), nullValue());

        char[] chars = randomFrom("true", "on", "yes", "1").toCharArray();
        assertThat(Booleans.parseBooleanLenient(chars, 0, chars.length, randomBoolean()), is(true));
        chars = randomFrom("false", "off", "no", "0").toCharArray();
        assertThat(Booleans.parseBooleanLenient(chars,0, chars.length, randomBoolean()), is(false));
        chars = randomFrom("true", "on", "yes").toUpperCase(Locale.ROOT).toCharArray();
        assertThat(Booleans.parseBooleanLenient(chars,0, chars.length, randomBoolean()), is(true));
    }
}
