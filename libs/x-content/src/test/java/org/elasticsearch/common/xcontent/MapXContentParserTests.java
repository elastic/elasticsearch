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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserTests.generateRandomObject;

public class MapXContentParserTests extends ESTestCase {

    public void testSimpleMap() throws IOException {
        compareTokens(builder -> {
            builder.startObject();
            builder.field("string", "foo");
            builder.field("number", 42);
            builder.field("double", 42.5);
            builder.field("bool", false);
            builder.startArray("arr");
            {
                builder.value(10).value(20.0).value("30");
                builder.startArray();
                builder.value(30);
                builder.endArray();
            }
            builder.endArray();
            builder.startArray("nested_arr");
            {
                builder.startArray();
                builder.value(10);
                builder.endArray();
            }
            builder.endArray();
            builder.startObject("obj");
            {
                builder.field("inner_string", "bar");
                builder.startObject("inner_empty_obj");
                builder.field("f", "a");
                builder.endObject();
            }
            builder.endObject();
            builder.field("bytes", new byte[]{1, 2, 3});
            builder.nullField("nothing");
            builder.endObject();
        });
    }


    public void testRandomObject() throws IOException {
        compareTokens(builder -> generateRandomObject(builder, randomIntBetween(0, 10)));
    }

    public void compareTokens(CheckedConsumer<XContentBuilder, IOException> consumer) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            consumer.accept(builder);
            final Map<String, Object> map;
            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
                map = parser.mapOrdered();
            }

            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
                try (XContentParser mapParser = new MapXContentParser(
                    xContentRegistry(), LoggingDeprecationHandler.INSTANCE, map, xContentType)) {
                    assertEquals(parser.contentType(), mapParser.contentType());
                    XContentParser.Token token;
                    assertEquals(parser.currentToken(), mapParser.currentToken());
                    assertEquals(parser.currentName(), mapParser.currentName());
                    do {
                        token = parser.nextToken();
                        XContentParser.Token mapToken = mapParser.nextToken();
                        assertEquals(token, mapToken);
                        assertEquals(parser.currentName(), mapParser.currentName());
                        if (token != null && (token.isValue() || token == XContentParser.Token.VALUE_NULL)) {
                            assertEquals(parser.textOrNull(), mapParser.textOrNull());
                            switch (token) {
                                case VALUE_STRING:
                                    assertEquals(parser.text(), mapParser.text());
                                    break;
                                case VALUE_NUMBER:
                                    assertEquals(parser.numberType(), mapParser.numberType());
                                    assertEquals(parser.numberValue(), mapParser.numberValue());
                                    if (parser.numberType() == XContentParser.NumberType.LONG ||
                                        parser.numberType() == XContentParser.NumberType.INT) {
                                        assertEquals(parser.longValue(), mapParser.longValue());
                                        if (parser.longValue() <= Integer.MAX_VALUE && parser.longValue() >= Integer.MIN_VALUE) {
                                            assertEquals(parser.intValue(), mapParser.intValue());
                                            if (parser.longValue() <= Short.MAX_VALUE && parser.longValue() >= Short.MIN_VALUE) {
                                                assertEquals(parser.shortValue(), mapParser.shortValue());
                                            }
                                        }
                                    } else {
                                        assertEquals(parser.doubleValue(), mapParser.doubleValue(), 0.000001);
                                    }
                                    break;
                                case VALUE_BOOLEAN:
                                    assertEquals(parser.booleanValue(), mapParser.booleanValue());
                                    break;
                                case VALUE_EMBEDDED_OBJECT:
                                    assertArrayEquals(parser.binaryValue(), mapParser.binaryValue());
                                    break;
                                case VALUE_NULL:
                                    assertNull(mapParser.textOrNull());
                                    break;
                            }
                            assertEquals(parser.currentName(), mapParser.currentName());
                            assertEquals(parser.isClosed(), mapParser.isClosed());
                        } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                            if (randomInt(5) == 0) {
                                parser.skipChildren();
                                mapParser.skipChildren();
                            }
                        }
                    } while (token != null);
                    assertEquals(parser.nextToken(), mapParser.nextToken());
                    parser.close();
                    mapParser.close();
                    assertEquals(parser.isClosed(), mapParser.isClosed());
                    assertTrue(mapParser.isClosed());
                }
            }

        }
    }
}
