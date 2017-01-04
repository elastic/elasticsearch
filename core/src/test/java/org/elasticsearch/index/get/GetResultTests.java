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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.index.get.GetFieldTests.randomGetField;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetResultTests extends ESTestCase {

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResult getResult = tuple.v1();
        GetResult expectedGetResult = tuple.v2();
        BytesReference originalBytes = toXContent(getResult, xContentType, false);
        //test that we can parse what we print out
        GetResult parsedGetResult;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedGetResult = GetResult.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResult, parsedGetResult);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResult, xContentType, false);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        //check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResult.sourceAsString(), parsedGetResult.sourceAsString());
    }

    public void testToXContent() throws IOException {
        {
            GetResult getResult = new GetResult("index", "type", "id", 1, true, new BytesArray("{ \"field1\" : " +
                    "\"value1\", \"field2\":\"value2\"}"), Collections.singletonMap("field1", new GetField("field1",
                    Collections.singletonList("value1"))));
            String output = Strings.toString(getResult, false);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":1,\"found\":true,\"_source\":{ \"field1\" " +
                    ": \"value1\", \"field2\":\"value2\"},\"fields\":{\"field1\":[\"value1\"]}}", output);
        }
        {
            GetResult getResult = new GetResult("index", "type", "id", 1, false, null, null);
            String output = Strings.toString(getResult, false);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"found\":false}", output);
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
                getResult.isExists(), getResult.internalSourceRef(), randomGetFields(XContentType.JSON).v1()));
        return randomFrom(mutations).get();
    }

    public static Tuple<GetResult, GetResult> randomGetResult(XContentType xContentType) {
        final String index = randomAsciiOfLengthBetween(3, 10);
        final String type = randomAsciiOfLengthBetween(3, 10);
        final String id = randomAsciiOfLengthBetween(3, 10);
        final long version;
        final boolean exists;
        BytesReference source = null;
        Map<String, GetField> fields = null;
        Map<String, GetField> expectedFields = null;
        if (frequently()) {
            version = randomNonNegativeLong();
            exists = true;
            if (frequently()) {
                source = RandomObjects.randomSource(random());
            }
            if (randomBoolean()) {
                Tuple<Map<String, GetField>, Map<String, GetField>> tuple = randomGetFields(xContentType);
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

    private static Tuple<Map<String, GetField>,Map<String, GetField>> randomGetFields(XContentType xContentType) {
        int numFields = randomIntBetween(2, 10);
        Map<String, GetField> fields = new HashMap<>(numFields);
        Map<String, GetField> expectedFields = new HashMap<>(numFields);
        for (int i = 0; i < numFields; i++) {
            Tuple<GetField, GetField> tuple = randomGetField(xContentType);
            GetField getField = tuple.v1();
            GetField expectedGetField = tuple.v2();
            fields.put(getField.getName(), getField);
            expectedFields.put(expectedGetField.getName(), expectedGetField);
        }
        return Tuple.tuple(fields, expectedFields);
    }
}
