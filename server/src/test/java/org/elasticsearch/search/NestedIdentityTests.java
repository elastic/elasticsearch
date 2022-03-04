/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchHit.NestedIdentity;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class NestedIdentityTests extends ESTestCase {

    public static NestedIdentity createTestItem(int depth) {
        String field = frequently() ? randomAlphaOfLengthBetween(1, 20) : randomRealisticUnicodeOfCodepointLengthBetween(1, 20);
        int offset = randomInt(10);
        NestedIdentity child = null;
        if (depth > 0) {
            child = createTestItem(depth - 1);
        }
        return new NestedIdentity(field, offset, child);
    }

    public void testFromXContent() throws IOException {
        NestedIdentity nestedIdentity = createTestItem(randomInt(3));
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder = nestedIdentity.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            NestedIdentity parsedNestedIdentity = NestedIdentity.fromXContent(parser);
            assertEquals(nestedIdentity, parsedNestedIdentity);
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() throws IOException {
        NestedIdentity nestedIdentity = new NestedIdentity("foo", 5, null);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {
              "_nested" : {
                "field" : "foo",
                "offset" : 5
              }
            }""", Strings.toString(builder));

        nestedIdentity = new NestedIdentity("foo", 5, new NestedIdentity("bar", 3, null));
        builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {
              "_nested" : {
                "field" : "foo",
                "offset" : 5,
                "_nested" : {
                  "field" : "bar",
                  "offset" : 3
                }
              }
            }""", Strings.toString(builder));
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createTestItem(randomInt(3)), NestedIdentityTests::copy, NestedIdentityTests::mutate);
    }

    public void testSerialization() throws IOException {
        NestedIdentity nestedIdentity = createTestItem(randomInt(3));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            nestedIdentity.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                NestedIdentity deserializedCopy = new NestedIdentity(in);
                assertEquals(nestedIdentity, deserializedCopy);
                assertEquals(nestedIdentity.hashCode(), deserializedCopy.hashCode());
                assertNotSame(nestedIdentity, deserializedCopy);
            }
        }
    }

    private static NestedIdentity mutate(NestedIdentity original) {
        if (original == null) {
            return createTestItem(0);
        }
        List<Supplier<NestedIdentity>> mutations = new ArrayList<>();
        int offset = original.getOffset();
        NestedIdentity child = original.getChild();
        String fieldName = original.getField().string();
        mutations.add(() -> new NestedIdentity(original.getField().string() + "_prefix", offset, child));
        mutations.add(() -> new NestedIdentity(fieldName, offset + 1, child));
        mutations.add(() -> new NestedIdentity(fieldName, offset, mutate(child)));
        return randomFrom(mutations).get();
    }

    private static NestedIdentity copy(NestedIdentity original) {
        NestedIdentity child = original.getChild();
        return new NestedIdentity(original.getField().string(), original.getOffset(), child != null ? copy(child) : null);
    }

}
