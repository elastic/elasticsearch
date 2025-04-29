/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class JsonArrayPartsEventParserTests extends ESTestCase {

    private void assertJsonParts(Deque<byte[]> actualParts, List<String> expectedJsonStrings) {
        assertThat("Number of parsed parts mismatch", actualParts.size(), equalTo(expectedJsonStrings.size()));
        var expectedIter = expectedJsonStrings.iterator();
        actualParts.forEach(part -> {
            String actualJsonString = new String(part, StandardCharsets.UTF_8);
            assertThat(actualJsonString, equalTo(expectedIter.next()));
        });
    }

    public void testParse_givenNullOrEmptyBytes_returnsEmptyDeque() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        assertTrue(parser.parse(null).isEmpty());
        assertTrue(parser.parse(new byte[0]).isEmpty());

        var incompletePart = "{".getBytes(StandardCharsets.UTF_8);
        parser.parse(incompletePart);
        assertTrue(parser.parse(null).isEmpty());
        assertTrue(parser.parse(new byte[0]).isEmpty());

        var missingPart = "}".getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(missingPart);
        assertJsonParts(parts, List.of("{}"));
    }

    public void testParse_singleCompleteObject_returnsOnePart() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json = "{\"key\":\"value\"}";
        byte[] input = json.getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json));
    }

    public void testParse_multipleCompleteObjectsInOneChunk_returnsMultipleParts() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"key1\":\"value1\"}";
        String json2 = "{\"key2\":\"value2\"}";

        byte[] input = ("[" + json1 + "," + json2 + "]").getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);

        assertJsonParts(parts, List.of(json1, json2));
    }

    public void testParse_twoObjectsBackToBack_extractsBoth() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"a\":1}";
        String json2 = "{\"b\":2}";

        byte[] input = (json1 + json2).getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);

        assertJsonParts(parts, List.of(json1, json2));
    }

    public void testParse_objectSplitAcrossChunks_returnsOnePartAfterAllChunks() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json = "{\"key\":\"very_long_value\"}";
        byte[] chunk1 = "{\"key\":\"very_long".getBytes(StandardCharsets.UTF_8);
        byte[] chunk2 = "_value\"}".getBytes(StandardCharsets.UTF_8);

        Deque<byte[]> parts1 = parser.parse(chunk1);
        assertTrue("Expected no parts from incomplete chunk", parts1.isEmpty());

        Deque<byte[]> parts2 = parser.parse(chunk2);
        assertJsonParts(parts2, List.of(json));
    }

    public void testParse_multipleObjectsSomeSplit_returnsPartsIncrementally() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"id\":1,\"name\":\"first\"}";
        String json2 = "{\"id\":2,\"name\":\"second_is_longer\"}";
        String json3 = "{\"id\":3,\"name\":\"third\"}";

        // Chunk 1: [{"id":1,"name":"first"},{"id":2,"name":"sec
        byte[] chunk1 = ("[" + json1 + ",{\"id\":2,\"name\":\"sec").getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts1 = parser.parse(chunk1);
        assertJsonParts(parts1, List.of(json1));

        // Chunk 2: ond_is_longer"},{"id":3,"name":"third"}]
        byte[] chunk2 = ("ond_is_longer\"}," + json3 + "]").getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts2 = parser.parse(chunk2);
        assertJsonParts(parts2, List.of(json2, json3));

        assertTrue("Expected no more parts from empty call", parser.parse(new byte[0]).isEmpty());
    }

    public void testParse_nestedObjects_extractsTopLevelObject() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json = "{\"outer_key\":{\"inner_key\":\"value\"},\"another_key\":\"val\"}";
        byte[] input = json.getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json));
    }

    public void testParse_nestedObjectSplit_extractsTopLevelObject() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json = "{\"outer_key\":{\"inner_key\":\"value\"},\"another_key\":\"val\"}";
        byte[] chunk1 = "{\"outer_key\":{\"inner_key\":\"val".getBytes(StandardCharsets.UTF_8);
        byte[] chunk2 = "ue\"},\"another_key\":\"val\"}".getBytes(StandardCharsets.UTF_8);

        Deque<byte[]> parts1 = parser.parse(chunk1);
        assertTrue(parts1.isEmpty());

        Deque<byte[]> parts2 = parser.parse(chunk2);
        assertJsonParts(parts2, List.of(json));
    }

    public void testParse_endsWithIncompleteObject_buffersCorrectly() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"complete\":\"done\"}";
        String partialJsonStart = "{\"incomplete_start\":\"";

        byte[] input = (json1 + "," + partialJsonStart).getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json1));

        String partialJsonEnd = "continue\"}";
        String json2 = partialJsonStart + partialJsonEnd;
        byte[] nextChunk = partialJsonEnd.getBytes(StandardCharsets.UTF_8);
        parts = parser.parse(nextChunk);
        assertJsonParts(parts, List.of(json2));
    }

    public void testParse_onlyOpenBrace_buffers() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        byte[] input = "{".getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertTrue(parts.isEmpty());

        byte[] nextInput = "\"key\":\"val\"}".getBytes(StandardCharsets.UTF_8);
        parts = parser.parse(nextInput);
        assertJsonParts(parts, List.of("{\"key\":\"val\"}"));
    }

    public void testParse_onlyCloseBrace_ignored() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        byte[] input = "}".getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertTrue(parts.isEmpty());

        parts = parser.parse("some data }".getBytes(StandardCharsets.UTF_8));
        assertTrue(parts.isEmpty());
    }

    public void testParse_mismatchedBraces_handlesGracefully() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();

        byte[] input1 = "{\"key\":\"val\"}}".getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts1 = parser.parse(input1);
        assertJsonParts(parts1, List.of("{\"key\":\"val\"}")); // First object is fine, extra '}' ignored

        parser = new JsonArrayPartsEventParser();
        byte[] input2 = "{\"key\":\"val\"}{".getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts2 = parser.parse(input2);
        assertJsonParts(parts2, List.of("{\"key\":\"val\"}")); // First object

        // The last '{' should be buffered
        Deque<byte[]> parts3 = parser.parse("}".getBytes(StandardCharsets.UTF_8));
        assertJsonParts(parts3, List.of("{}"));
    }

    public void testParse_objectWithMultiByteChars_handlesCorrectly() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json = "{\"key\":\"value_with_emoji_😊_and_résumé\"}";
        byte[] input = json.getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json));

        parser = new JsonArrayPartsEventParser();
        String part1Str = "{\"key\":\"value_with_emoji_😊";
        String part2Str = "_and_résumé\"}";
        byte[] chunk1 = part1Str.getBytes(StandardCharsets.UTF_8);
        byte[] chunk2 = part2Str.getBytes(StandardCharsets.UTF_8);

        Deque<byte[]> parts1 = parser.parse(chunk1);
        assertTrue(parts1.isEmpty());

        Deque<byte[]> parts2 = parser.parse(chunk2);
        assertJsonParts(parts2, List.of(json));
    }

    public void testParse_javadocExampleStream() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"key\":\"val1\"}";
        String json2 = "{\"key2\":\"val2\"}";
        String json3 = "{\"key3\":\"val3\"}";
        String json4 = "{\"some\":\"object\"}";

        Deque<byte[]> parts1 = parser.parse(("[{\"key\":\"val1\"}").getBytes(StandardCharsets.UTF_8));
        assertJsonParts(parts1, List.of(json1));

        Deque<byte[]> parts2 = parser.parse((",{\"key2\":\"val2\"}").getBytes(StandardCharsets.UTF_8));
        assertJsonParts(parts2, List.of(json2));

        Deque<byte[]> parts3 = parser.parse((",{\"key3\":\"val3\"}, {\"some\":\"object\"}]").getBytes(StandardCharsets.UTF_8));
        assertJsonParts(parts3, List.of(json3, json4));
    }

    public void testParse_emptyObjects() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{}";
        String json2 = "{\"a\":{}}";
        byte[] input = (json1 + " " + json2).getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json1, json2));
    }

    public void testParse_dataBeforeFirstObjectAndAfterLastObject() {
        JsonArrayPartsEventParser parser = new JsonArrayPartsEventParser();
        String json1 = "{\"key1\":\"value1\"}";
        String json2 = "{\"key2\":\"value2\"}";
        byte[] input = ("leading_garbage" + json1 + "middle_garbage" + json2 + "trailing_garbage").getBytes(StandardCharsets.UTF_8);
        Deque<byte[]> parts = parser.parse(input);
        assertJsonParts(parts, List.of(json1, json2));
    }
}
