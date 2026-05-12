/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.terminal.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

public class EcsJsonUtilsTests extends ESTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode parseJson(String raw) {
        assertThat("JSON must be single line", raw.indexOf('\n'), either(is(-1)).or(is(raw.length() - 1)));
        try {
            return MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            throw new AssertionError("produced invalid JSON: " + raw, e);
        }
    }

    public void testFormatJsonProducesValidJson() {
        String raw = EcsJsonUtils.formatJson("INFO", "test.logger", "hello world");
        JsonNode json = parseJson(raw);

        assertTrue(json.has("@timestamp"));
        assertEquals("INFO", json.get("log.level").asText());
        assertEquals("test.logger", json.get("log.logger").asText());
        assertEquals("hello world", json.get("message").asText());
        assertEquals("1.2.0", json.get("ecs.version").asText());
        assertEquals("ES_ECS", json.get("service.name").asText());
    }

    public void testFormatJsonEscapesSpecialCharacters() {
        String raw = EcsJsonUtils.formatJson("INFO", "test", "line1\nline2\ttab \"quoted\"");
        JsonNode json = parseJson(raw);

        assertEquals("line1\nline2\ttab \"quoted\"", json.get("message").asText());
    }

    public void testFormatJsonEscapesControlCharacters() {
        String raw = EcsJsonUtils.formatJson("INFO", "test", "before\u0001after");
        JsonNode json = parseJson(raw);

        assertEquals("before\u0001after", json.get("message").asText());
    }

    public void testFormatJsonIncludesStackTrace() {
        Exception cause = new IllegalArgumentException("root cause");
        RuntimeException ex = new RuntimeException("top level", cause);

        String raw = EcsJsonUtils.formatJson("ERROR", "test", "something failed", ex);
        JsonNode json = parseJson(raw);

        assertEquals("ERROR", json.get("log.level").asText());
        assertEquals("something failed", json.get("message").asText());
        assertEquals("java.lang.RuntimeException", json.get("error.type").asText());
        String stackTrace = json.get("error.stack_trace").asText();
        assertTrue("stack trace should mention root cause", stackTrace.contains("root cause"));
    }

    public void testFormatJsonWithNullMessage() {
        Exception ex = new RuntimeException((String) null);

        String raw = EcsJsonUtils.formatJson("ERROR", "test", null, ex);
        JsonNode json = parseJson(raw);

        assertEquals("java.lang.RuntimeException", json.get("message").asText());
    }

    public void testLooksLikeJsonDetectsJsonObjects() {
        assertTrue(EcsJsonUtils.looksLikeJson("{\"@timestamp\":\"2024-01-01\"}"));
        assertTrue(EcsJsonUtils.looksLikeJson("{}"));
    }

    public void testLooksLikeJsonRejectsPlaintext() {
        assertFalse(EcsJsonUtils.looksLikeJson("Starting Elasticsearch..."));
        assertFalse(EcsJsonUtils.looksLikeJson("WARNING: something"));
        assertFalse(EcsJsonUtils.looksLikeJson(""));
    }

    public void testEscapeJsonHandlesBackslash() {
        StringBuilder sb = new StringBuilder();
        EcsJsonUtils.escapeJson(sb, "path\\to\\file");
        assertEquals("path\\\\to\\\\file", sb.toString());
    }

    public void testEscapeJsonWithoutSpecialChars() {
        testEscapeJson(randomAlphaOfLength(10), true);
    }

    public void testEscapeJsonWithSpecialChars() {
        testEscapeJson("text with \"quotes\" and \\backslashes\\", true);
    }

    public void testEscapeJsonWithControlChars() {
        testEscapeJson("Line\nbreak and \ttab", true);
    }

    public void testEscapeJsonWithAllSpecialChars() {
        testEscapeJson("\"\\/\b\f\r\n\t", false);
    }

    public void testEscapeJsonWithSpecialCharsAtStartAndEnd() {
        testEscapeJson("\nHello World\t", false);
    }

    public void testEscapeJsonHandlesNull() {
        StringBuilder sb = new StringBuilder();
        EcsJsonUtils.escapeJson(sb, null);
        assertEquals("", sb.toString());
    }

    private void testEscapeJson(String str, boolean testRandomSubstrings) {
        StringBuilder sb = new StringBuilder();

        int start = 0;
        int count = str.length();
        do {
            EcsJsonUtils.escapeJson(sb, str.subSequence(start, start + count));
            String actual = sb.toString();
            sb.setLength(0);

            String input = str.substring(start, start + count);
            String expected = new String(JsonStringEncoder.getInstance().quoteAsString(input));

            assertEquals(String.format(Locale.ROOT, "escapeJson(%s, %s, %s)", str, start, count), expected, actual);

            start = start + randomInt(count - 1);
            count = randomInt(Math.min(count - 1, str.length() - start));
        } while (count > 0 && testRandomSubstrings);
    }
}
