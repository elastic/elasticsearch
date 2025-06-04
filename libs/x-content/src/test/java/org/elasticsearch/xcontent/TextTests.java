/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class TextTests extends ESTestCase {
    public void testConvertToBytes() {
        String value = randomUnicodeOfLength(randomInt(128));
        byte[] encodedArr = value.getBytes(StandardCharsets.UTF_8);
        var encoded = new XContentString.UTF8Bytes(encodedArr);

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
        byte[] encodedArr = value.getBytes(StandardCharsets.UTF_8);
        var encoded = new XContentString.UTF8Bytes(encodedArr);

        var text = new Text(encoded);
        assertFalse(text.hasString());
        assertTrue(text.hasBytes());

        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertTrue(text.hasString());
        assertTrue(text.hasBytes());

        // Ensure the conversion didn't mess up subsequent calls
        assertEquals(value, text.string());
        assertEquals(encoded, text.bytes());

        assertSame(encoded, text.bytes());
    }

    public void testStringLength() {
        int stringLength = randomInt(128);
        String value = randomUnicodeOfLength(stringLength);
        byte[] encodedArr = value.getBytes(StandardCharsets.UTF_8);
        var encoded = new XContentString.UTF8Bytes(encodedArr);

        {
            var text = new Text(value);
            assertTrue(text.hasString());
            assertEquals(stringLength, text.stringLength());
        }

        {
            var text = new Text(encoded);
            assertFalse(text.hasString());
            assertEquals(stringLength, text.stringLength());
            assertTrue(text.hasString());
        }

        {
            var text = new Text(encoded, stringLength);
            assertFalse(text.hasString());
            assertEquals(stringLength, text.stringLength());
            assertFalse(text.hasString());
        }
    }

    public void testEquals() {
        String value = randomUnicodeOfLength(randomInt(128));
        byte[] encodedArr = value.getBytes(StandardCharsets.UTF_8);
        var encoded = new XContentString.UTF8Bytes(encodedArr);

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
        byte[] encodedArr1 = value1.getBytes(StandardCharsets.UTF_8);
        var encoded1 = new XContentString.UTF8Bytes(encodedArr1);

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
        byte[] encodedArr2 = value2.getBytes(StandardCharsets.UTF_8);
        var encoded2 = new XContentString.UTF8Bytes(encodedArr2);

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

    public void testRandomized() {
        int stringLength = randomInt(128);
        String value = randomUnicodeOfLength(stringLength);
        byte[] encodedArr = value.getBytes(StandardCharsets.UTF_8);
        var encoded = new XContentString.UTF8Bytes(encodedArr);

        Text text = switch (randomInt(2)) {
            case 0 -> new Text(value);
            case 1 -> new Text(encoded);
            default -> new Text(encoded, stringLength);
        };

        for (int i = 0; i < 20; i++) {
            switch (randomInt(5)) {
                case 0 -> assertEquals(encoded, text.bytes());
                case 1 -> assertSame(text.bytes(), text.bytes());
                case 2 -> assertEquals(value, text.string());
                case 3 -> assertEquals(value, text.toString());
                case 4 -> assertEquals(stringLength, text.stringLength());
                case 5 -> assertEquals(new Text(value), text);
            }
        }
    }

}
