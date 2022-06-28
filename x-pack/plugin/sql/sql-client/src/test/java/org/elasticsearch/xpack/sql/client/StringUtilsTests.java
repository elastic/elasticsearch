/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.client.StringUtils.nullAsEmpty;
import static org.elasticsearch.xpack.sql.client.StringUtils.repeatString;

public class StringUtilsTests extends ESTestCase {
    public void testNullAsEmpty() {
        assertEquals("", nullAsEmpty(null));
        assertEquals("", nullAsEmpty(""));
        String rando = randomRealisticUnicodeOfCodepointLength(5);
        assertEquals(rando, nullAsEmpty(rando));
    }

    public void testRepeatString() {
        final int count = randomIntBetween(0, 100) <= 5 ? -1 : randomIntBetween(0, 100);
        final int len = randomIntBetween(0, 100);
        // final String in = randomUnicodeOfLength(len);
        final String in = randomAlphaOfLength(len);
        final String out;
        try {
            out = repeatString(in, count);
            assertEquals(count * len, out.length());
            for (int i = 0; i < count; i++) {
                assertEquals(in, out.substring(i * len, (i + 1) * len));
            }
        } catch (IllegalArgumentException iae) {
            assertTrue(count < 0);
        }
    }
}
