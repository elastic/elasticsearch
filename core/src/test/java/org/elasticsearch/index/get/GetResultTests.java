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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.index.get.GetFieldTests.assertSameOutput;
import static org.elasticsearch.index.get.GetFieldTests.randomGetField;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

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
        assertSameOutput(originalBytes, finalBytes, xContentType);
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
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), randomPositiveLong(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists() == false, getResult.internalSourceRef(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), randomSource(), getResult.getFields()));
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
            version = randomPositiveLong();
            exists = true;
            if (frequently()) {
                source = randomSource();
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

    //TODO move this to some utility class, this should be useful whenever we have to test parsing _source
    public static BytesReference randomSource() {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            addFields(builder, 0);
            builder.endObject();
            return builder.bytes();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addFields(XContentBuilder builder, int currentDepth) throws IOException {
        int numFields = randomIntBetween(1, 5);
        for (int i = 0; i < numFields; i++) {
            if (frequently()) {
                builder.field(randomAsciiOfLengthBetween(3, 10), randomFieldValue());
            }
            if (randomBoolean() && currentDepth < 5) {
                builder.startObject(randomAsciiOfLengthBetween(3, 10));
                addFields(builder, currentDepth + 1);
                builder.endObject();
            }
            if (randomBoolean() && currentDepth < 5) {
                builder.startArray(randomAsciiOfLengthBetween(3, 10));
                int numElements = randomIntBetween(1, 5);
                boolean object = randomBoolean();
                for (int j = 0; j < numElements; j++) {
                    if (object) {
                        builder.startObject();
                        addFields(builder, currentDepth + 1);
                        builder.endObject();
                    } else {
                        builder.value(randomFieldValue());
                    }
                }
                builder.endArray();
            }
        }
    }

    private static Object randomFieldValue() {
        switch(randomIntBetween(0, 3)) {
            case 0:
                return randomAsciiOfLengthBetween(3, 10);
            case 1:
                return randomUnicodeOfLengthBetween(3, 10);
            case 2:
                return randomLong();
            case 3:
                return randomDouble();
            default:
                throw new UnsupportedOperationException();
        }
    }
}
