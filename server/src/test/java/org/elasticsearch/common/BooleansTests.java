/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;

public class BooleansTests extends ESTestCase {
    private static final String[] NON_BOOLEANS = new String[] {
        "11",
        "00",
        "sdfsdfsf",
        "F",
        "T",
        "on",
        "off",
        "yes",
        "no",
        "0",
        "1",
        "True",
        "False" };
    private static final String[] BOOLEANS = new String[] { "true", "false" };

    public void testIsBoolean() {
        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), b.length()));
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(t.getBytes(StandardCharsets.UTF_8), "prefix".length(), b.length()));
            assertTrue("failed to recognize [" + b + "] as boolean", Booleans.isBoolean(b));
        }
    }

    public void testIsNonBoolean() {
        assertThat(Booleans.isBoolean((char[]) null, 0, 1), is(false));
        assertFalse(Booleans.isBoolean(new char[] { 't', 'r', 'u', 'e' }, 0, 0));
        assertFalse(Booleans.isBoolean((String) null));
        assertFalse(Booleans.isBoolean(""));
        assertFalse(Booleans.isBoolean((byte[]) null, 0, 1));
        assertFalse(Booleans.isBoolean(new byte[] { 't', 'r', 'u', 'e' }, 0, 0));

        for (String nb : NON_BOOLEANS) {
            String t = "prefix" + nb + "suffix";
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.toCharArray(), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t.getBytes(StandardCharsets.UTF_8), "prefix".length(), nb.length()));
            assertFalse("recognized [" + nb + "] as boolean", Booleans.isBoolean(t));
        }
    }

    public void testParseBooleanWithFallback() {
        assertFalse(Booleans.parseBoolean(null, false));
        assertTrue(Booleans.parseBoolean(null, true));
        assertNull(Booleans.parseBoolean(null, null));
        assertFalse(Booleans.parseBoolean(null, Boolean.FALSE));
        assertTrue(Booleans.parseBoolean(null, Boolean.TRUE));

        assertFalse(Booleans.parseBoolean("", false));
        assertTrue(Booleans.parseBoolean("", true));
        assertNull(Booleans.parseBoolean("", null));
        assertFalse(Booleans.parseBoolean("", Boolean.FALSE));
        assertTrue(Booleans.parseBoolean("", Boolean.TRUE));

        assertFalse(Booleans.parseBoolean("   ", false));
        assertTrue(Booleans.parseBoolean("   ", true));
        assertNull(Booleans.parseBoolean("   ", null));
        assertFalse(Booleans.parseBoolean("   ", Boolean.FALSE));
        assertTrue(Booleans.parseBoolean("   ", Boolean.TRUE));

        assertTrue(Booleans.parseBoolean("true", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
        assertFalse(Booleans.parseBoolean("false", randomFrom(Boolean.TRUE, Boolean.FALSE, null)));
    }

    public void testParseNonBooleanWithFallback() {
        for (String nonBoolean : NON_BOOLEANS) {
            boolean defaultValue = randomFrom(Boolean.TRUE, Boolean.FALSE);

            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(nonBoolean, defaultValue));
            expectThrows(
                IllegalArgumentException.class,
                () -> Booleans.parseBoolean(nonBoolean.toCharArray(), 0, nonBoolean.length(), defaultValue)
            );
            expectThrows(
                IllegalArgumentException.class,
                () -> Booleans.parseBoolean(nonBoolean.getBytes(StandardCharsets.UTF_8), 0, nonBoolean.length(), defaultValue)
            );
        }
    }

    public void testParseBoolean() {
        assertTrue(Booleans.parseBoolean("true"));
        assertFalse(Booleans.parseBoolean("false"));
    }

    public void testParseNonBoolean() {
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(null));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(""));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean("   "));
        for (String nonBoolean : NON_BOOLEANS) {
            expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(nonBoolean));
        }
    }

    public void testWhitespaceOnlyWithDefaultReturnsDefault() {
        // All overloads that accept a default treat whitespace-only input as absent and return the default.
        // This is consistent across String, char[], and byte[].
        assertFalse(Booleans.parseBoolean("   ", false));
        assertTrue(Booleans.parseBoolean("   ", true));
        assertFalse(Booleans.parseBoolean("   ".toCharArray(), 0, 3, false));
        assertTrue(Booleans.parseBoolean("   ".toCharArray(), 0, 3, true));
        assertFalse(Booleans.parseBoolean("   ".getBytes(StandardCharsets.UTF_8), 0, 3, false));
        assertTrue(Booleans.parseBoolean("   ".getBytes(StandardCharsets.UTF_8), 0, 3, true));
        // The strict (no-default) overloads still throw for whitespace-only input.
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean("   "));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean("   ".toCharArray(), 0, 3));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean("   ".getBytes(StandardCharsets.UTF_8), 0, 3));
    }

    public void testParseBooleanCharArray() {
        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            boolean expected = "true".equals(b);
            assertEquals(expected, Booleans.parseBoolean(t.toCharArray(), "prefix".length(), b.length(), !expected));
            assertEquals(expected, Booleans.parseBoolean(t.toCharArray(), "prefix".length(), b.length()));
        }
        assertFalse(Booleans.parseBoolean((char[]) null, 0, 0, false));
        assertTrue(Booleans.parseBoolean((char[]) null, 0, 0, true));
        assertFalse(Booleans.parseBoolean(new char[] { 't', 'r', 'u', 'e' }, 0, 0, false));
        assertTrue(Booleans.parseBoolean(new char[] { 't', 'r', 'u', 'e' }, 0, 0, true));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean((char[]) null, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(new char[] { 't', 'r', 'u', 'e' }, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(new char[] { 'x' }, 0, 1));
    }

    public void testParseBooleanByteArray() {
        for (String b : BOOLEANS) {
            String t = "prefix" + b + "suffix";
            boolean expected = "true".equals(b);
            assertEquals(expected, Booleans.parseBoolean(t.getBytes(StandardCharsets.UTF_8), "prefix".length(), b.length(), !expected));
            assertEquals(expected, Booleans.parseBoolean(t.getBytes(StandardCharsets.UTF_8), "prefix".length(), b.length()));
        }
        assertFalse(Booleans.parseBoolean((byte[]) null, 0, 0, false));
        assertTrue(Booleans.parseBoolean((byte[]) null, 0, 0, true));
        assertFalse(Booleans.parseBoolean(new byte[] { 't', 'r', 'u', 'e' }, 0, 0, false));
        assertTrue(Booleans.parseBoolean(new byte[] { 't', 'r', 'u', 'e' }, 0, 0, true));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean((byte[]) null, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(new byte[] { 't', 'r', 'u', 'e' }, 0, 0));
        expectThrows(IllegalArgumentException.class, () -> Booleans.parseBoolean(new byte[] { 'x' }, 0, 1));
    }

    public void testIsFalse() {
        assertTrue(Booleans.isFalse("false"));
        assertFalse(Booleans.isFalse("true"));
        assertFalse(Booleans.isFalse(null));
        assertFalse(Booleans.isFalse(""));
        assertFalse(Booleans.isFalse("False"));
        assertFalse(Booleans.isFalse("FALSE"));
    }

    public void testIsTrue() {
        assertTrue(Booleans.isTrue("true"));
        assertFalse(Booleans.isTrue("false"));
        assertFalse(Booleans.isTrue(null));
        assertFalse(Booleans.isTrue(""));
        assertFalse(Booleans.isTrue("True"));
        assertFalse(Booleans.isTrue("TRUE"));
    }

    public void testParseBooleanLenient() {
        assertThat(Booleans.parseBooleanLenient(randomFrom("true", "TRUE", "True"), randomBoolean()), is(true));
        assertThat(Booleans.parseBooleanLenient(randomFrom("false", "FALSE", "anything"), randomBoolean()), is(false));
        assertThat(Booleans.parseBooleanLenient("", randomBoolean()), is(false));
        assertThat(Booleans.parseBooleanLenient("   ", randomBoolean()), is(false));
        assertThat(Booleans.parseBooleanLenient(null, false), is(false));
        assertThat(Booleans.parseBooleanLenient(null, true), is(true));
    }
}
