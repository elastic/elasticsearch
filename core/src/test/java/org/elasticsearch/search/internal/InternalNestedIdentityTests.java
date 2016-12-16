/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.internal.InternalSearchHit.InternalNestedIdentity;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class InternalNestedIdentityTests extends ESTestCase {

    public static InternalNestedIdentity createTestItem(int depth) {
        String field = frequently() ? randomAsciiOfLengthBetween(1, 20) : randomRealisticUnicodeOfCodepointLengthBetween(1, 20);
        int offset = randomInt(10);
        InternalNestedIdentity child = null;
        if (depth > 0) {
            child = createTestItem(depth - 1);
        }
        return new InternalNestedIdentity(field, offset, child);
    }

    public void testFromXContent() throws IOException {
        InternalNestedIdentity nestedIdentity = createTestItem(randomInt(3));
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder = nestedIdentity.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        InternalNestedIdentity parsedNestedIdentity = InternalNestedIdentity.fromXContent(parser);
        assertEquals(nestedIdentity, parsedNestedIdentity);
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        InternalNestedIdentity nestedIdentity = new InternalNestedIdentity("foo", 5, null);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
              "{\n" +
              "  \"_nested\" : {\n" +
              "    \"field\" : \"foo\",\n" +
              "    \"offset\" : 5\n" +
              "  }\n" +
              "}", builder.string());

        nestedIdentity = new InternalNestedIdentity("foo", 5, new InternalNestedIdentity("bar", 3, null));
        builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        nestedIdentity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
              "{\n" +
              "  \"_nested\" : {\n" +
              "    \"field\" : \"foo\",\n" +
              "    \"offset\" : 5,\n" +
              "    \"_nested\" : {\n" +
              "      \"field\" : \"bar\",\n" +
              "      \"offset\" : 3\n" +
              "    }\n" +
              "  }\n" +
              "}", builder.string());
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createTestItem(randomInt(3)), InternalNestedIdentityTests::copy, InternalNestedIdentityTests::mutate);
    }

    public void testSerialization() throws IOException {
        InternalNestedIdentity nestedIdentity = createTestItem(randomInt(3));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            nestedIdentity.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                InternalNestedIdentity deserializedCopy = new InternalNestedIdentity(in);
                assertEquals(nestedIdentity, deserializedCopy);
                assertEquals(nestedIdentity.hashCode(), deserializedCopy.hashCode());
                assertNotSame(nestedIdentity, deserializedCopy);
            }
        }
    }

    private static InternalNestedIdentity mutate(InternalNestedIdentity original) {
        if (original == null) {
            return createTestItem(0);
        }
        List<Supplier<InternalNestedIdentity>> mutations = new ArrayList<>();
        int offset = original.getOffset();
        InternalNestedIdentity child = (InternalNestedIdentity) original.getChild();
        String fieldName = original.getField().string();
        mutations.add(() ->
            new InternalNestedIdentity(original.getField().string() + "_prefix", offset, child));
        mutations.add(() -> new InternalNestedIdentity(fieldName, offset + 1, child));
        mutations.add(() -> new InternalNestedIdentity(fieldName, offset, mutate(child)));
        return randomFrom(mutations).get();
    }

    private static InternalNestedIdentity copy(InternalNestedIdentity original) {
        InternalNestedIdentity child = (InternalNestedIdentity) original.getChild();
        return new InternalNestedIdentity(original.getField().string(), original.getOffset(), child != null ? copy(child) : null);
    }

}
