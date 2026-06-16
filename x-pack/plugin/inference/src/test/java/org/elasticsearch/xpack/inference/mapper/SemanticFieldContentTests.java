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
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomInferenceString;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticInput;
import static org.hamcrest.Matchers.containsString;

public class SemanticFieldContentTests extends ESTestCase {

    public void testNullValue() {
        SemanticFieldContent content = new SemanticFieldContent(null);
        assertNull(content.getMapValue(randomIntBetween(0, 10)));
        assertNull(content.getInferenceStringValue(randomIntBetween(0, 10)));
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

    public void testGetChunkTextEdgeCases() {
        // Layout with "hello" and "world": indices 0-4 = "hello", 5 = separator, 6-10 = "world"
        SemanticFieldContent content = new SemanticFieldContent(List.of("hello", "world"));

        assertEquals("hello", content.getChunkText(0, 5));
        assertEquals("world", content.getChunkText(6, 11));

        IndexOutOfBoundsException separatorException = expectThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(5, 6));
        assertThat(separatorException.getMessage(), containsString("refers to a separator character"));

        IndexOutOfBoundsException outOfBoundsException = expectThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(11, 12));
        assertThat(outOfBoundsException.getMessage(), containsString("out of bounds"));

        IndexOutOfBoundsException crossesBoundaryException = expectThrows(
            IndexOutOfBoundsException.class,
            () -> content.getChunkText(0, 6)
        );
        assertThat(crossesBoundaryException.getMessage(), containsString("crosses a text value boundary"));
    }

    public void testGetChunkTextMapOnlyInput() throws IOException {
        SemanticFieldContent content = new SemanticFieldContent(materialize(List.of(randomInferenceString())));
        expectThrows(IndexOutOfBoundsException.class, () -> content.getChunkText(0, 1));
    }

    private static void assertSemanticFieldContent(List<Object> inputs, SemanticFieldContent content) throws IOException {
        int i = 0;
        int cursor = 0;
        for (Object input : inputs) {
            if (input instanceof InferenceString infString) {
                Map<?, ?> mapValue = content.getMapValue(i);
                assertNotNull("expected map value at index " + i, mapValue);

                InferenceString parsedMapValue = parseMapAsInferenceString(mapValue);
                assertEquals(infString, parsedMapValue);
                assertEquals(infString, content.getInferenceStringValue(i));
            } else {
                assertNull("expected no map value at index " + i, content.getMapValue(i));
                assertNull("expected no inference string value at index " + i, content.getInferenceStringValue(i));
                String expected = input.toString();
                assertEquals(expected, content.getChunkText(cursor, cursor + expected.length()));
                cursor += expected.length() + 1;
            }

            i++;
        }
    }

    private static InferenceString parseMapAsInferenceString(Map<?, ?> mapValue) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> stringKeyedMap = (Map<String, Object>) mapValue;

        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                stringKeyedMap,
                XContentType.JSON
            )
        ) {
            return InferenceString.PARSER.parse(parser, null);
        }
    }
}
