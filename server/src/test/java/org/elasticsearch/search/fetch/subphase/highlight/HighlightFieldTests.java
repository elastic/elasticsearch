/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class HighlightFieldTests extends ESTestCase {

    public static HighlightField createTestItem() {
        String name = frequently() ? randomAlphaOfLengthBetween(5, 20) : randomRealisticUnicodeOfCodepointLengthBetween(5, 20);
        Text[] fragments = null;
        if (frequently()) {
            int size = randomIntBetween(0, 5);
            fragments = new Text[size];
            for (int i = 0; i < size; i++) {
                fragments[i] = new Text(
                    frequently() ? randomAlphaOfLengthBetween(10, 30) : randomRealisticUnicodeOfCodepointLengthBetween(10, 30)
                );
            }
        }
        return new HighlightField(name, fragments);
    }

    public void testFromXContent() throws IOException {
        HighlightField highlightField = createTestItem();
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject(); // we need to wrap xContent output in proper object to create a parser for it
        builder = highlightField.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken(); // skip to the opening object token, fromXContent advances from here and starts with the field name
            parser.nextToken();
            HighlightField parsedField = SearchResponseUtils.parseHighlightField(parser);
            assertEquals(highlightField, parsedField);
            if (highlightField.fragments() != null) {
                assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() throws IOException {
        HighlightField field = new HighlightField("foo", new Text[] { new Text("bar"), new Text("baz") });
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        field.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {
              "foo" : [
                "bar",
                "baz"
              ]
            }""", Strings.toString(builder));

        field = new HighlightField("foo", null);
        builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        field.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {
              "foo" : null
            }""", Strings.toString(builder));
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createTestItem(), HighlightFieldTests::copy, HighlightFieldTests::mutate);
    }

    public void testSerialization() throws IOException {
        HighlightField testField = createTestItem();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testField.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                HighlightField deserializedCopy = new HighlightField(in);
                assertEquals(testField, deserializedCopy);
                assertEquals(testField.hashCode(), deserializedCopy.hashCode());
                assertNotSame(testField, deserializedCopy);
            }
        }
    }

    private static HighlightField mutate(HighlightField original) {
        Text[] fragments = original.fragments();
        if (randomBoolean()) {
            return new HighlightField(original.name() + "_suffix", fragments);
        } else {
            if (fragments == null) {
                fragments = new Text[] { new Text("field") };
            } else {
                fragments = Arrays.copyOf(fragments, fragments.length + 1);
                fragments[fragments.length - 1] = new Text("something new");
            }
            return new HighlightField(original.name(), fragments);
        }
    }

    private static HighlightField copy(HighlightField original) {
        return new HighlightField(original.name(), original.fragments());
    }

}
