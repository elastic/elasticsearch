/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.elasticsearch.cli.internal.JsonUtils.quoteAsString;

public class JsonUtilsTests extends ESTestCase {

    public void testQuoteAsStringWithoutSpecialChars() {
        testQuoteAsString(randomAlphaOfLength(10), true);
    }

    public void testQuoteAsStringWithSpecialChars() {
        testQuoteAsString("text with \"quotes\" and \\backslashes\\", true);
    }

    public void testQuoteAsStringWithControlChars() {
        testQuoteAsString("Line\nbreak and \ttab", true);
    }

    public void testQuoteAsStringWithAllSpecialChars() {
        testQuoteAsString("\"\\/\b\f\r\n\t", false);
    }

    public void testQuoteAsStringWithSpecialCharsAtStartAndEnd() {
        testQuoteAsString("\nHello World\t", false);
    }

    private void testQuoteAsString(String str, boolean testRandomSubstrings) {
        StringBuilder sb = new StringBuilder();

        int start = 0;
        int count = str.length();
        do {
            if (randomBoolean()) quoteAsString(str, start, count, sb);
            else quoteAsString(str.toCharArray(), start, count, sb);

            String actual = sb.toString();
            sb.setLength(0);

            // Use escape utils in Log4j (test dependency only!) to validate the behavior.
            org.apache.logging.log4j.core.util.JsonUtils.quoteAsString(str.substring(start, start + count), sb);
            String expected = sb.toString();
            sb.setLength(0);

            assertEquals(String.format(Locale.ROOT, "quoteAsString(%s, %s, %s)", str, start, count), expected, actual);

            start = start + randomInt(count - 1);
            count = randomInt(Math.min(count - 1, str.length() - start));
        } while (count > 0 && testRandomSubstrings);
    }
}
