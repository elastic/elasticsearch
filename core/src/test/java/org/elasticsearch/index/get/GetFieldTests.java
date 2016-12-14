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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class GetFieldTests extends ESTestCase {

    public void testToXContent() throws IOException {
        GetField getField = new GetField("field", Arrays.asList("value1", "value2"));
        String output = Strings.toString(getField, true);
        assertEquals("{\"field\":[\"value1\",\"value2\"]}", output);
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetField(XContentType.JSON).v1(), GetFieldTests::copyGetField, GetFieldTests::mutateGetField);
    }

    public void testGetFieldToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetField, GetField> tuple = randomGetField(xContentType);
        GetField getField = tuple.v1();
        GetField expectedGetField = tuple.v2();
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
        BytesReference finalBytes = toXContent(parsedGetField, xContentType, true);
        assertSameOutput(originalBytes, finalBytes, xContentType);
    }

    private static GetField copyGetField(GetField getField) {
        return new GetField(getField.getName(), getField.getValues());
    }

    private static GetField mutateGetField(GetField getField) {
        List<Supplier<GetField>> mutations = new ArrayList<>();
        mutations.add(() -> new GetField(randomUnicodeOfCodepointLength(15), getField.getValues()));
        mutations.add(() -> new GetField(getField.getName(), randomGetField(XContentType.JSON).v1().getValues()));
        return randomFrom(mutations).get();
    }

    public static Tuple<GetField, GetField> randomGetField(XContentType xContentType) {
        if (randomBoolean()) {
            String fieldName = randomFrom(ParentFieldMapper.NAME, RoutingFieldMapper.NAME, UidFieldMapper.NAME);
            GetField getField = new GetField(fieldName, Collections.singletonList(randomAsciiOfLengthBetween(3, 10)));
            return Tuple.tuple(getField, getField);
        }
        String fieldName = randomAsciiOfLengthBetween(3, 10);
        Tuple<List<Object>, List<Object>> tuple = randomStoredFieldValues(xContentType);
        GetField input = new GetField(fieldName, tuple.v1());
        GetField expected = new GetField(fieldName, tuple.v2());
        return Tuple.tuple(input, expected);
    }

    //TODO move this to some utility class
    /**
     * Asserts that the provided {@link BytesReference}s hold the same content. The comparison is done byte per byte, unless we know that
     * the content type is SMILE, in which case the map representation of the provided objects will be compared.
     */
    public static void assertSameOutput(BytesReference expected, BytesReference actual, XContentType xContentType) throws IOException {
        if (xContentType == XContentType.SMILE) {
            //Jackson SMILE parser parses floats as double, which then get printed out as double (with double precision),
            //hence the byte per byte comparison fails. We rather re-parse both expected and actual bytes into a map and compare those two.
            try (XContentParser parser = xContentType.xContent().createParser(actual)) {
                Map<String, Object> finalMap = parser.map();
                try (XContentParser parser2 = xContentType.xContent().createParser(expected)) {
                    Map<String, Object> originalMap = parser2.map();
                    assertEquals(originalMap, finalMap);
                }
            }
        } else {
            assertEquals(expected, actual);
        }
    }

    //TODO move this to some utility class? It should be useful in other tests too, whenever we parse stored fields
    /**
     * Returns a tuple containing random stored field values and their corresponding expected values once printed out
     * via {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} and parsed back via {@link XContentParser#objectText()}.
     * Generates values based on what can get printed out. Stored fields values are retrieved from lucene and converted via
     * {@link org.elasticsearch.index.mapper.MappedFieldType#valueForDisplay(Object)} to either strings, numbers or booleans.
     *
     * @param xContentType the content type, used to determine what the expected values are for float numbers.
     */
    public static Tuple<List<Object>, List<Object>> randomStoredFieldValues(XContentType xContentType) {
        int numValues = randomIntBetween(1, 5);
        List<Object> originalValues = new ArrayList<>();
        List<Object> expectedParsedValues = new ArrayList<>();
        int dataType = randomIntBetween(0, 7);
        for (int i = 0; i < numValues; i++) {
            switch(dataType) {
                case 0:
                    long randomLong = randomLong();
                    originalValues.add(randomLong);
                    expectedParsedValues.add(randomLong);
                    break;
                case 1:
                    int randomInt = randomInt();
                    originalValues.add(randomInt);
                    expectedParsedValues.add(randomInt);
                    break;
                case 2:
                    Short randomShort = randomShort();
                    originalValues.add(randomShort);
                    expectedParsedValues.add(randomShort.intValue());
                    break;
                case 3:
                    Byte randomByte = randomByte();
                    originalValues.add(randomByte);
                    expectedParsedValues.add(randomByte.intValue());
                    break;
                case 4:
                    double randomDouble = randomDouble();
                    originalValues.add(randomDouble);
                    expectedParsedValues.add(randomDouble);
                    break;
                case 5:
                    Float randomFloat = randomFloat();
                    originalValues.add(randomFloat);
                    if (xContentType == XContentType.CBOR) {
                        //with CBOR we get back a float
                        expectedParsedValues.add(randomFloat);
                    } else if (xContentType == XContentType.SMILE) {
                        //with SMILE we get back a double
                        expectedParsedValues.add(randomFloat.doubleValue());
                    } else {
                        //with JSON AND YAML we get back a double, but with float precision.
                        expectedParsedValues.add(Double.parseDouble(randomFloat.toString()));
                    }
                    break;
                case 6:
                    boolean randomBoolean = randomBoolean();
                    originalValues.add(randomBoolean);
                    expectedParsedValues.add(randomBoolean);
                    break;
                case 7:
                    String randomString = randomUnicodeOfLengthBetween(3, 10);
                    originalValues.add(randomString);
                    expectedParsedValues.add(randomString);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return Tuple.tuple(originalValues, expectedParsedValues);
    }
}
