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

package org.elasticsearch.index.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.index.get.DocumentFieldTests.randomDocumentField;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetResultTests extends ESTestCase {

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        GetResult expectedGetResult = tuple.v2();
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(getResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable, "_source");
        //test that we can parse what we print out
        GetResult parsedGetResult;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedGetResult = GetResult.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResult, parsedGetResult);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResult, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        //check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResult.sourceAsString(), parsedGetResult.sourceAsString());
    }

    public void testToXContent() throws IOException {
        {
            GetResult getResult = new GetResult("index", "type", "id", 1, true, new BytesArray("{ \"field1\" : " +
                    "\"value1\", \"field2\":\"value2\"}"), singletonMap("field1", new DocumentField("field1",
                    singletonList("value1"))));
            String output = Strings.toString(getResult);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":1,\"found\":true,\"_source\":{ \"field1\" " +
                    ": \"value1\", \"field2\":\"value2\"},\"fields\":{\"field1\":[\"value1\"]}}", output);
        }
        {
            GetResult getResult = new GetResult("index", "type", "id", 1, false, null, null);
            String output = Strings.toString(getResult);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"found\":false}", output);
        }
    }

    public void testToAndFromXContentEmbedded() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();

        // We don't expect to retrieve the index/type/id of the GetResult because they are not rendered
        // by the toXContentEmbedded method.
        GetResult expectedGetResult = new GetResult(null, null, null, -1,
                tuple.v2().isExists(), tuple.v2().sourceRef(), tuple.v2().getFields());

        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContentEmbedded(getResult, xContentType, humanReadable);

        // Test that we can parse the result of toXContentEmbedded()
        GetResult parsedEmbeddedGetResult;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            parsedEmbeddedGetResult = GetResult.fromXContentEmbedded(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(expectedGetResult, parsedEmbeddedGetResult);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContentEmbedded(parsedEmbeddedGetResult, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        //check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResult.sourceAsString(), parsedEmbeddedGetResult.sourceAsString());
    }

    public void testToXContentEmbedded() throws IOException {
        Map<String, DocumentField> fields = new HashMap<>();
        fields.put("foo", new DocumentField("foo", singletonList("bar")));
        fields.put("baz", new DocumentField("baz", Arrays.asList("baz_0", "baz_1")));

        GetResult getResult = new GetResult("index", "type", "id", 2, true,
                new BytesArray("{\"foo\":\"bar\",\"baz\":[\"baz_0\",\"baz_1\"]}"), fields);

        BytesReference originalBytes = toXContentEmbedded(getResult, XContentType.JSON, false);
        assertEquals("{\"found\":true,\"_source\":{\"foo\":\"bar\",\"baz\":[\"baz_0\",\"baz_1\"]}," +
                "\"fields\":{\"foo\":[\"bar\"],\"baz\":[\"baz_0\",\"baz_1\"]}}", originalBytes.utf8ToString());
    }

    public void testToXContentEmbeddedNotFound() throws IOException {
        GetResult getResult = new GetResult("index", "type", "id", 1, false, null, null);

        BytesReference originalBytes = toXContentEmbedded(getResult, XContentType.JSON, false);
        assertEquals("{\"found\":false}", originalBytes.utf8ToString());
    }

    public void testGetSourceAsBytes() {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        if (getResult.isExists() && getResult.isSourceEmpty() == false) {
            assertNotNull(getResult.sourceRef());
        } else {
            assertNull(getResult.sourceRef());
        }
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetResult(XContentType.JSON).v1(), GetResultTests::copyGetResult, GetResultTests::mutateGetResult);
    }

    public static GetResult copyGetResult(GetResult getResult) {
        return new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields());
    }

    public static GetResult mutateGetResult(GetResult getResult) {
        List<Supplier<GetResult>> mutations = new ArrayList<>();
        mutations.add(() -> new GetResult(randomUnicodeOfLength(15), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), randomUnicodeOfLength(15), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), randomUnicodeOfLength(15), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), randomNonNegativeLong(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists() == false, getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), RandomObjects.randomSource(random()), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), randomDocumentFields(XContentType.JSON).v1()));
        return randomFrom(mutations).get();
    }

    public static Tuple<GetResult, GetResult> randomGetResult(XContentType xContentType) {
        final String index = randomAlphaOfLengthBetween(3, 10);
        final String type = randomAlphaOfLengthBetween(3, 10);
        final String id = randomAlphaOfLengthBetween(3, 10);
        final long version;
        final boolean exists;
        BytesReference source = null;
        Map<String, DocumentField> fields = null;
        Map<String, DocumentField> expectedFields = null;
        if (frequently()) {
            version = randomNonNegativeLong();
            exists = true;
            if (frequently()) {
                source = RandomObjects.randomSource(random());
            }
            if (randomBoolean()) {
                Tuple<Map<String, DocumentField>, Map<String, DocumentField>> tuple = randomDocumentFields(xContentType);
                fields = tuple.v1();
                expectedFields = tuple.v2();
            }
        } else {
            version = -1;
            exists = false;
        }
        GetResult getResult = new GetResult(index, type, id, version, exists, source, fields);
        GetResult expectedGetResult = new GetResult(index, type, id, version, exists, source, expectedFields);
        return Tuple.tuple(getResult, expectedGetResult);
    }

    private static Tuple<Map<String, DocumentField>,Map<String, DocumentField>> randomDocumentFields(XContentType xContentType) {
        int numFields = randomIntBetween(2, 10);
        Map<String, DocumentField> fields = new HashMap<>(numFields);
        Map<String, DocumentField> expectedFields = new HashMap<>(numFields);
        for (int i = 0; i < numFields; i++) {
            Tuple<DocumentField, DocumentField> tuple = randomDocumentField(xContentType);
            DocumentField getField = tuple.v1();
            DocumentField expectedGetField = tuple.v2();
            fields.put(getField.getName(), getField);
            expectedFields.put(expectedGetField.getName(), expectedGetField);
        }
        return Tuple.tuple(fields, expectedFields);
    }

    private static BytesReference toXContentEmbedded(GetResult getResult, XContentType xContentType, boolean humanReadable)
            throws IOException {
        return XContentHelper.toXContent(getResult::toXContentEmbedded, xContentType, humanReadable);
    }
}
