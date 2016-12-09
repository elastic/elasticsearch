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

package org.elasticsearch.action.get;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentBuilder.DEFAULT_DATE_PRINTER;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class GetResponseTests extends ESTestCase {

    public void testGetFieldToAndFromXContent() throws Exception {
        Tuple<GetField, GetField> tuple = randomGetField();
        GetField getField = tuple.v1();
        GetField expectedGetField = tuple.v2();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(getField, xContentType, true);
        //test that we can parse what we print out
        GetField parsedGetField;
        try (XContentParser parser = xContentType.xContent().createParser(originalBytes)) {
            //we need to move to the next token, the start object one that we manually added is not expected
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedGetField = GetField.fromXContent(parser);
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetField, parsedGetField);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetField, xContentType, true);
        assertEquals(originalBytes, finalBytes);
    }

    public void testGetFieldToXContent() throws IOException {
        GetField getField = new GetField("field", Arrays.asList("value1", "value2"));
        String output = toXContent(getField, XContentType.JSON, true).utf8ToString();
        assertEquals("{\"field\":[\"value1\",\"value2\"]}", output);
    }

    public void testGetFieldEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetField().v1(), GetResponseTests::copyGetField, GetResponseTests::mutateGetField);
    }

    public void testGetResultToAndFromXContent() throws Exception {
        Tuple<GetResult, GetResult> tuple = randomGetResult();
        GetResult getResult = tuple.v1();
        GetResult expectedGetResult = tuple.v2();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(getResult, xContentType, false);
        //test that we can parse what we print out
        GetResult parsedGetResult;
        try (XContentParser parser = xContentType.xContent().createParser(originalBytes)) {
            parsedGetResult = GetResult.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResult, parsedGetResult);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResult, xContentType, false);
        Map<String, Object> originalMap = XContentHelper.convertToMap(originalBytes, false).v2();
        Map<String, Object> finalMap = XContentHelper.convertToMap(finalBytes, false).v2();
        assertEquals(originalMap, finalMap);
    }

    public void testGetResultEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetResult().v1(), GetResponseTests::copyGetResult, GetResponseTests::mutateGetResult);
    }

    public void testGetResponseToAndFromXContent() throws Exception {
        Tuple<GetResult, GetResult> tuple = randomGetResult();
        GetResponse getResponse = new GetResponse(tuple.v1());
        GetResponse expectedGetResponse = new GetResponse(tuple.v2());
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(getResponse, xContentType, false);
        //test that we can parse what we print out
        GetResponse parsedGetResponse;
        try (XContentParser parser = xContentType.xContent().createParser(originalBytes)) {
            parsedGetResponse = GetResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResponse, parsedGetResponse);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResponse, xContentType, false);
        Map<String, Object> originalMap = XContentHelper.convertToMap(originalBytes, false).v2();
        Map<String, Object> finalMap = XContentHelper.convertToMap(finalBytes, false).v2();
        assertEquals(originalMap, finalMap);
    }

    public void testGetResponseToXContent() throws IOException {
        {
            GetResponse getResponse = new GetResponse(new GetResult("index", "type", "id", 1, true, new BytesArray("{ \"field1\" : " +
                    "\"value1\", \"field2\":\"value2\"}"), Collections.singletonMap("field1", new GetField("field1",
                    Collections.singletonList("value1")))));
            String output = toXContent(getResponse, XContentType.JSON, false).utf8ToString();
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":1,\"found\":true,\"_source\":{ \"field1\" " +
                    ": \"value1\", \"field2\":\"value2\"},\"fields\":{\"field1\":[\"value1\"]}}", output);
        }
        {
            GetResponse getResponse = new GetResponse(new GetResult("index", "type", "id", 1, false, null, null));
            String output = toXContent(getResponse, XContentType.JSON, false).utf8ToString();
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"found\":false}", output);
        }
    }

    public void testGetResponseEqualsAndHashcode() {
        checkEqualsAndHashCode(new GetResponse(randomGetResult().v1()), GetResponseTests::copyGetResponse,
                GetResponseTests::mutateGetResponse);
    }

    private static Tuple<GetResult, GetResult> randomGetResult() {
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
                source = randomDocument();
            }
            if (randomBoolean()) {
                Tuple<Map<String, GetField>, Map<String, GetField>> tuple = randomGetFields();
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

    private static BytesReference randomDocument() {
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
                builder.field(randomAsciiOfLengthBetween(3, 10), randomAsciiOfLengthBetween(3, 10));
            }
            if (randomBoolean() && currentDepth < 5) {
                builder.startObject(randomAsciiOfLengthBetween(3, 10));
                addFields(builder, currentDepth + 1);
                builder.endObject();
            }
            if (randomBoolean() && currentDepth < 5) {
                builder.startArray(randomAsciiOfLengthBetween(3, 10));
                int numElements = randomIntBetween(1, 5);
                for (int j = 0; j < numElements; j++) {
                    if (frequently()) {
                        builder.value(randomBoolean() ? randomLong() : randomAsciiOfLengthBetween(3, 10));
                    } else {
                        builder.startObject();
                        addFields(builder, currentDepth + 1);
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
        }
    }

    private static Tuple<Map<String, GetField>,Map<String, GetField>> randomGetFields() {
        int numFields = randomIntBetween(1, 10);
        Map<String, GetField> fields = new HashMap<>(numFields);
        Map<String, GetField> expectedFields = new HashMap<>(numFields);
        for (int i = 0; i < numFields; i++) {
            Tuple<GetField, GetField> tuple = randomGetField();
            GetField getField = tuple.v1();
            GetField expectedGetField = tuple.v2();
            fields.put(getField.getName(), getField);
            expectedFields.put(expectedGetField.getName(), expectedGetField);
        }
        return Tuple.tuple(fields, expectedFields);
    }

    private static Tuple<GetField, GetField> randomGetField() {
        if (randomBoolean()) {
            String fieldName = randomFrom(ParentFieldMapper.NAME, RoutingFieldMapper.NAME, UidFieldMapper.NAME);
            GetField getField = new GetField(fieldName, Collections.singletonList(randomAsciiOfLengthBetween(3, 10)));
            return Tuple.tuple(getField, getField);
        }
        String fieldName = randomAsciiOfLengthBetween(3, 10);
        int numValues = randomIntBetween(1, 5);
        List<Object> values = new ArrayList<>();
        List<Object> expectedValues = new ArrayList<>();
        int dataType = randomIntBetween(0, 8);
        for (int i = 0; i < numValues; i++) {
            switch(dataType) {
                case 0:
                    long randomLong = randomLong();
                    values.add(randomLong);
                    expectedValues.add(randomLong);
                    break;
                case 1:
                    int randomInt = randomInt();
                    values.add(randomInt);
                    expectedValues.add(randomInt);
                    break;
                case 2:
                    Short randomShort = randomShort();
                    values.add(randomShort);
                    expectedValues.add(randomShort.intValue());
                    break;
                case 3:
                    Byte randomByte = randomByte();
                    values.add(randomByte);
                    expectedValues.add(randomByte.intValue());
                    break;
                case 4:
                    double randomDouble = randomDouble();
                    values.add(randomDouble);
                    expectedValues.add(randomDouble);
                    break;
                case 5:
                    boolean randomBoolean = randomBoolean();
                    values.add(randomBoolean);
                    expectedValues.add(randomBoolean);
                    break;
                case 6:
                    Date randomDate = new Date(randomPositiveLong());
                    values.add(randomDate);
                    expectedValues.add(DEFAULT_DATE_PRINTER.print(randomDate.getTime()));
                    break;
                case 7:
                    String randomString = randomUnicodeOfLengthBetween(3, 10);
                    values.add(randomString);
                    expectedValues.add(randomString);
                    break;
                case 8:
                    String randomText = randomUnicodeOfLengthBetween(3, 10);
                    values.add(new Text(randomText));
                    expectedValues.add(randomText);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        GetField input = new GetField(fieldName, values);
        GetField expected = new GetField(fieldName, expectedValues);
        return Tuple.tuple(input, expected);
    }

    private static GetResponse copyGetResponse(GetResponse getResponse) {
        return new GetResponse(copyGetResult(getResponse.getResult));
    }

    private static GetResponse mutateGetResponse(GetResponse getResponse) {
        return new GetResponse(mutateGetResult(getResponse.getResult));
    }

    private static GetResult copyGetResult(GetResult getResult) {
        return new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), getResult.getFields());
    }

    private static GetResult mutateGetResult(GetResult getResult) {
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
                getResult.isExists(), randomDocument(), getResult.getFields()));
        mutations.add(() -> new GetResult(getResult.getIndex(), getResult.getType(), getResult.getId(), getResult.getVersion(),
                getResult.isExists(), getResult.internalSourceRef(), randomGetFields().v1()));
        return randomFrom(mutations).get();
    }

    private static GetField copyGetField(GetField getField) {
        return new GetField(getField.getName(), getField.getValues());
    }

    private static GetField mutateGetField(GetField getField) {
        List<Supplier<GetField>> mutations = new ArrayList<>();
        mutations.add(() -> new GetField(randomUnicodeOfCodepointLength(15), getField.getValues()));
        mutations.add(() -> new GetField(getField.getName(), randomGetField().v1().getValues()));
        return randomFrom(mutations).get();
    }

    private static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, boolean wrapInObject) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            if (wrapInObject) {
                builder.startObject();
            }
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            if (wrapInObject) {
                builder.endObject();
            }
            return builder.bytes();
        }
    }
}
