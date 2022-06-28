/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TermsLookupTests extends ESTestCase {
    public void testTermsLookup() {
        String index = randomAlphaOfLengthBetween(1, 10);
        String type = randomAlphaOfLengthBetween(1, 10);
        String id = randomAlphaOfLengthBetween(1, 10);
        String path = randomAlphaOfLengthBetween(1, 10);
        String routing = randomAlphaOfLengthBetween(1, 10);
        TermsLookup termsLookup = new TermsLookup(index, type, id, path);
        termsLookup.routing(routing);
        assertEquals(index, termsLookup.index());
        assertEquals(type, termsLookup.type());
        assertEquals(id, termsLookup.id());
        assertEquals(path, termsLookup.path());
        assertEquals(routing, termsLookup.routing());
    }

    public void testIllegalArguments() {
        String type = randomAlphaOfLength(5);
        String id = randomAlphaOfLength(5);
        String path = randomAlphaOfLength(5);
        String index = randomAlphaOfLength(5);
        switch (randomIntBetween(0, 3)) {
            case 0:
                type = null;
                break;
            case 1:
                id = null;
                break;
            case 2:
                path = null;
                break;
            case 3:
                index = null;
                break;
            default:
                fail("unknown case");
        }
        try {
            new TermsLookup(index, type, id, path);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[terms] query lookup element requires specifying"));
        }
    }

    public void testSerialization() throws IOException {
        TermsLookup termsLookup = randomTermsLookup();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_6_7_0);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> termsLookup.writeTo(output));
            assertEquals(
                "Typeless [terms] lookup queries are not supported if any " + "node is running a version before 7.0.",
                e.getMessage()
            );
        }
    }

    public void testSerializationWithTypes() throws IOException {
        TermsLookup termsLookup = randomTermsLookupWithTypes();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }
    }

    public void testXContentParsingWithType() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"index\" : \"index\", \"id\" : \"id\", \"type\" : \"type\", \"path\" : \"path\", \"routing\" : \"routing\" }"
        );

        TermsLookup tl = TermsLookup.parseTermsLookup(parser);
        assertEquals("index", tl.index());
        assertEquals("type", tl.type());
        assertEquals("id", tl.id());
        assertEquals("path", tl.path());
        assertEquals("routing", tl.routing());

        assertWarnings("Deprecated field [type] used, this field is unused and will be removed entirely");
    }

    public void testXContentParsing() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"index\" : \"index\", \"id\" : \"id\", \"path\" : \"path\", \"routing\" : \"routing\" }"
        );

        TermsLookup tl = TermsLookup.parseTermsLookup(parser);
        assertEquals("index", tl.index());
        assertNull(tl.type());
        assertEquals("id", tl.id());
        assertEquals("path", tl.path());
        assertEquals("routing", tl.routing());
    }

    public static TermsLookup randomTermsLookup() {
        return new TermsLookup(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10).replace('.', '_')).routing(
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    public static TermsLookup randomTermsLookupWithTypes() {
        return new TermsLookup(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10).replace('.', '_')
        ).routing(randomBoolean() ? randomAlphaOfLength(10) : null);
    }
}
