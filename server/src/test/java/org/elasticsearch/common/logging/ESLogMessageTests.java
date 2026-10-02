/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESLogMessageTests extends ESTestCase {

    public void testScalarsAreRenderedAsQuotedStrings() throws IOException {
        final Map<String, Object> fields = render(
            new ESLogMessage().field("text", "plain").field("number", 3).field("flag", true).field("absent", null)
        );

        assertEquals("plain", fields.get("text"));
        assertEquals("3", fields.get("number"));
        assertEquals("true", fields.get("flag"));
        assertFalse(fields.containsKey("absent"));
    }

    public void testCollectionsAreRenderedAsJsonArrays() throws IOException {
        final Map<String, Object> fields = render(
            new ESLogMessage().field("empty", List.of()).field("single", List.of("a")).field("several", List.of("a", "b", "c"))
        );

        assertEquals(List.of(), fields.get("empty"));
        assertEquals(List.of("a"), fields.get("single"));
        assertEquals(List.of("a", "b", "c"), fields.get("several"));
    }

    public void testMapsAreRenderedAsJsonObjects() throws IOException {
        final Map<String, Object> fields = render(
            new ESLogMessage().field("empty", Map.of()).field("single", Map.of("k", "v")).field("several", Map.of("a", "1", "b", "2"))
        );

        assertEquals(Map.of(), fields.get("empty"));
        assertEquals(Map.of("k", "v"), fields.get("single"));
        assertEquals(Map.of("a", "1", "b", "2"), fields.get("several"));
    }

    public void testNestedCollectionsAndMapsAreRendered() throws IOException {
        final Map<String, Object> fields = render(
            new ESLogMessage().field("objects", List.of(Map.of("k", "v"), Map.of("k", "w")))
                .field("lists", Map.of("inner", List.of("a", "b")))
        );

        assertEquals(List.of(Map.of("k", "v"), Map.of("k", "w")), fields.get("objects"));
        assertEquals(Map.of("inner", List.of("a", "b")), fields.get("lists"));
    }

    public void testNestedValuesAreJsonEscaped() throws IOException {
        final String awkward = "say \"hi\" \\ then\nnew\ttab é";

        final Map<String, Object> fields = render(
            new ESLogMessage().field("objects", List.of(Map.of(awkward, awkward))).field("scalar", awkward)
        );

        assertEquals(List.of(Map.of(awkward, awkward)), fields.get("objects"));
        assertEquals(awkward, fields.get("scalar"));
    }

    private Map<String, Object> render(ESLogMessage message) throws IOException {
        final StringBuilder builder = new StringBuilder("{");
        message.addJsonNoBrackets(builder);
        builder.append("}");
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), builder.toString())) {
            return parser.map();
        }
    }
}
