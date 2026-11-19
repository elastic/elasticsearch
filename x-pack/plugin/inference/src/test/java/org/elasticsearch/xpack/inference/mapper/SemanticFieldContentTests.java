/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper.OffsetSource;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomInferenceString;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticInput;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SemanticFieldContentTests extends ESTestCase {

    public void testNullValue() {
        SemanticFieldContent content = new SemanticFieldContent(null);
        assertNull(content.getMapValue(randomIntBetween(0, 10)));
        assertThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(0, randomIntBetween(1, 10)));
    }

    public void testRandomListInput() throws IOException {
        for (int i = 0; i < 50; i++) {
            List<Object> inputs = randomList(1, 10, () -> randomSemanticInput(true));
            SemanticFieldContent content = new SemanticFieldContent(materialize(inputs));
            assertSemanticFieldContent(inputs, content);
        }
    }

    public void testRandomSingleInput() throws IOException {
        for (int i = 0; i < 10; i++) {
            Object input = randomSemanticInput(true);
            Object materialized = materialize(List.of(input)).getFirst();

            SemanticFieldContent content = new SemanticFieldContent(materialized);
            assertSemanticFieldContent(List.of(input), content);
        }
    }

    public void testGetChunkTextEdgeCases() {
        // Layout with "hello" and "world": indices 0-4 = "hello", 5 = separator, 6-10 = "world"
        SemanticFieldContent content = new SemanticFieldContent(List.of("hello", "world"));

        assertEquals("hello", content.getChunkText(0, 5));
        assertEquals("world", content.getChunkText(6, 11));

        IndexOutOfBoundsException separatorException = assertThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(5, 6));
        assertThat(separatorException.getMessage(), containsString("refers to a separator character"));

        IndexOutOfBoundsException outOfBoundsException = assertThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(11, 12));
        assertThat(outOfBoundsException.getMessage(), containsString("out of bounds"));

        IndexOutOfBoundsException crossesBoundaryException = assertThrows(
            IndexOutOfBoundsException.class,
            () -> content.getChunkText(0, 6)
        );
        assertThat(crossesBoundaryException.getMessage(), containsString("crosses a text value boundary"));
    }

    public void testGetChunkTextMapOnlyInput() throws IOException {
        SemanticFieldContent content = new SemanticFieldContent(materialize(List.of(randomInferenceString())));
        assertThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(0, 1));
    }

    public void testResolveErrors() {
        SemanticFieldContent content = new SemanticFieldContent(List.of("hello", "world"));
        String fieldName = "my_field";

        // Missing object: index 0 is a text value, so getMapValue(0) returns null
        IllegalArgumentException missingObject = assertThrows(
            IllegalArgumentException.class,
            () -> content.resolve(new OffsetSource(fieldName, 0))
        );
        assertThat(missingObject.getMessage(), containsString("Missing object value at input index [0]"));

        // Out-of-bounds text offset propagates as IndexOutOfBoundsException
        assertThrows(IndexOutOfBoundsException.class, () -> content.resolve(new OffsetSource(fieldName, 100, 101)));
    }

    /**
     * Converts each {@link InferenceString} in the input list to its xContent {@link Map} representation, simulating
     * the JSON round-trip that occurs when values are written to and read from {@code _source}.
     */
    private static List<Object> materialize(List<Object> inputs) throws IOException {
        List<Object> result = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof InferenceString infString) {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                infString.toXContent(builder, ToXContent.EMPTY_PARAMS);
                Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
                result.add(map);
            } else {
                result.add(input);
            }
        }
        return result;
    }

    private static void assertSemanticFieldContent(List<Object> inputs, SemanticFieldContent content) throws IOException {
        int i = 0;
        int cursor = 0;
        for (Object input : inputs) {
            if (input instanceof InferenceString infString) {
                Map<?, ?> mapValue = content.getMapValue(i);
                assertNotNull("expected map value at index " + i, mapValue);
                assertEquals(infString, parseInferenceStringValue(mapValue));

                Object resolvedValue = content.resolve(new OffsetSource("my_field", i));
                assertThat(resolvedValue, instanceOf(Map.class));
                assertEquals(infString, parseInferenceStringValue((Map<?, ?>) resolvedValue));
            } else {
                String expected = input.toString();
                int startOffset = cursor;
                int endOffset = cursor + expected.length();

                assertNull("expected no map value at index " + i, content.getMapValue(i));
                assertEquals(expected, content.getChunkText(startOffset, endOffset));

                Object resolvedValue = content.resolve(new OffsetSource("my_field", startOffset, endOffset));
                assertThat(resolvedValue, instanceOf(String.class));
                assertEquals(expected, resolvedValue);

                cursor += expected.length() + 1;
            }

            i++;
        }
    }

    private static InferenceString parseInferenceStringValue(Map<?, ?> map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> stringKeyedMap = (Map<String, Object>) map;
        return SemanticTextUtils.parseInferenceStringValue(stringKeyedMap);
    }
}
