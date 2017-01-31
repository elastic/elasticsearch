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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetFieldTests extends ESTestCase {

    public void testToXContent() {
        GetField getField = new GetField("field", Arrays.asList("value1", "value2"));
        String output = Strings.toString(getField);
        assertEquals("{\"field\":[\"value1\",\"value2\"]}", output);
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(randomGetField(XContentType.JSON).v1(), GetFieldTests::copyGetField, GetFieldTests::mutateGetField);
    }

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetField, GetField> tuple = randomGetField(xContentType);
        GetField getField = tuple.v1();
        GetField expectedGetField = tuple.v2();
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(getField, xContentType, humanReadable);
        //test that we can parse what we print out
        GetField parsedGetField;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            //we need to move to the next token, the start object one that we manually added is not expected
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parsedGetField = GetField.fromXContent(parser);
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetField, parsedGetField);
        BytesReference finalBytes = toXContent(parsedGetField, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
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
        Tuple<List<Object>, List<Object>> tuple = RandomObjects.randomStoredFieldValues(random(), xContentType);
        GetField input = new GetField(fieldName, tuple.v1());
        GetField expected = new GetField(fieldName, tuple.v2());
        return Tuple.tuple(input, expected);
    }
}
