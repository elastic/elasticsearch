/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.internal;

import org.elasticsearch.test.ESTestCase;

public class JsonUtilsTests extends ESTestCase {

    public void testQuoteAsStringWithNull() {
        StringBuilder sb = new StringBuilder();
        JsonUtils.quoteAsString(null, sb);
        assertEquals("null", sb.toString());
    }

    public void testQuoteAsStringWithoutSpecialChars() {
        StringBuilder sb = new StringBuilder();
        String input = randomAlphaOfLengthBetween(0, 100);
        JsonUtils.quoteAsString(input, sb);
        assertEquals(input, sb.toString());
    }

    public void testQuoteAsStringWithSpecialChars() {
        StringBuilder sb = new StringBuilder();
        String input = "text with \"quotes\" and \\backslashes\\";
        JsonUtils.quoteAsString(input, sb);
        assertEquals(log4jQuoteAsJsonString(input), sb.toString());
    }

    public void testQuoteAsStringWithControlChars() {
        StringBuilder sb = new StringBuilder();
        String input = "Line\nbreak and \ttab";
        JsonUtils.quoteAsString(input, sb);
        assertEquals(log4jQuoteAsJsonString(input), sb.toString());
    }

    public void testQuoteAsStringWithAllSpecialChars() {
        StringBuilder sb = new StringBuilder();
        String input = "\"\\/\b\f\r\n\t";
        JsonUtils.quoteAsString(input, sb);
        assertEquals(log4jQuoteAsJsonString(input), sb.toString());
    }

    public void testQuoteAsStringWithSpecialCharsAtStartAndEnd() {
        StringBuilder sb = new StringBuilder();
        String input = "\nHello World\t";
        JsonUtils.quoteAsString(input, sb);
        assertEquals(log4jQuoteAsJsonString(input), sb.toString());
    }

    // Use escape utils in Log4j (test dependency only!) to validate the behavior.
    private String log4jQuoteAsJsonString(String str) {
        StringBuilder sb = new StringBuilder(64);
        org.apache.logging.log4j.core.util.JsonUtils.quoteAsString(str, sb);
        return sb.toString();
    }
}
