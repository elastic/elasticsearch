/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.text;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class TextTests extends ESTestCase {
    public void testConvertToBytes() {
        String value = randomUnicodeOfLength(randomInt(128));
        var encoded = new BytesArray(value);

        var text = new Text(value);
        assertTrue(text.hasString());
        assertFalse(text.hasBytes());

        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertTrue(text.hasString());
        assertTrue(text.hasBytes());

        // Ensure the conversion didn't mess up subsequent calls
        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertSame(text.bytes(), text.bytes());
    }

    public void testConvertToString() {
        String value = randomUnicodeOfLength(randomInt(128));
        var encoded = new BytesArray(value);

        var text = new Text(encoded);
        assertFalse(text.hasString());
        assertTrue(text.hasBytes());

        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertFalse(text.hasString());
        assertTrue(text.hasBytes());

        // Ensure the conversion didn't mess up subsequent calls
        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertSame(encoded, text.bytes());
    }

    public void testEquals() {
        String value = randomUnicodeOfLength(randomInt(128));
        var encoded = new BytesArray(value);

        {
            var text1 = new Text(value);
            var text2 = new Text(value);
            assertTrue(text1.equals(text2));
        }

        {
            var text1 = new Text(value);
            var text2 = new Text(encoded);
            assertTrue(text1.equals(text2));
        }

        {
            var text1 = new Text(encoded);
            var text2 = new Text(encoded);
            assertTrue(text1.equals(text2));
        }
    }

    public void testCompareTo() {
        String value1 = randomUnicodeOfLength(randomInt(128));
        var encoded1 = new BytesArray(value1);

        {
            var text1 = new Text(value1);
            var text2 = new Text(value1);
            assertEquals(0, text1.compareTo(text2));
        }

        {
            var text1 = new Text(value1);
            var text2 = new Text(encoded1);
            assertEquals(0, text1.compareTo(text2));
        }

        {
            var text1 = new Text(encoded1);
            var text2 = new Text(encoded1);
            assertEquals(0, text1.compareTo(text2));
        }

        String value2 = randomUnicodeOfLength(randomInt(128));
        var encoded2 = new BytesArray(value2);

        int compSign = (int) Math.signum(encoded1.compareTo(encoded2));

        {
            var text1 = new Text(value1);
            var text2 = new Text(value2);
            assertEquals(compSign, (int) Math.signum(text1.compareTo(text2)));
        }

        {
            var text1 = new Text(value1);
            var text2 = new Text(encoded2);
            assertEquals(compSign, (int) Math.signum(text1.compareTo(text2)));
        }

        {
            var text1 = new Text(encoded1);
            var text2 = new Text(value2);
            assertEquals(compSign, (int) Math.signum(text1.compareTo(text2)));
        }

        {
            var text1 = new Text(encoded1);
            var text2 = new Text(encoded2);
            assertEquals(compSign, (int) Math.signum(text1.compareTo(text2)));
        }
    }

}
