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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseTypedKeysObject;

public class XContentParserUtilsTests extends ESTestCase {

    public void testEnsureExpectedToken() throws IOException {
        final XContentParser.Token randomToken = randomFrom(XContentParser.Token.values());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            // Parser current token is null
            assertNull(parser.currentToken());
            ParsingException e = expectThrows(ParsingException.class,
                    () -> ensureExpectedToken(randomToken, parser.currentToken(), parser::getTokenLocation));
            assertEquals("Failed to parse object: expecting token of type [" + randomToken + "] but found [null]", e.getMessage());
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        }
    }

    public void testParseTypedKeysObject() throws IOException {
        final String delimiter = randomFrom("#", ":", "/", "-", "_", "|", "_delim_");
        final XContentType xContentType = randomFrom(XContentType.values());

        final ObjectParser<SetOnce<Boolean>, Void> BOOLPARSER = new ObjectParser<>("bool", () -> new SetOnce<>());
        BOOLPARSER.declareBoolean(SetOnce::set, new ParseField("field"));
        final ObjectParser<SetOnce<Long>, Void> LONGPARSER = new ObjectParser<>("long", () -> new SetOnce<>());
        LONGPARSER.declareLong(SetOnce::set, new ParseField("field"));

        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        namedXContents.add(new NamedXContentRegistry.Entry(Boolean.class, new ParseField("bool"), p -> BOOLPARSER.parse(p, null).get()));
        namedXContents.add(new NamedXContentRegistry.Entry(Long.class, new ParseField("long"), p -> LONGPARSER.parse(p, null).get()));
        final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(namedXContents);

        BytesReference bytes = toXContent((builder, params) -> builder.startObject("name").field("field", 0).endObject(), xContentType,
                randomBoolean());
        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            SetOnce<Boolean> booleanConsumer = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Boolean.class, booleanConsumer::set);
            // because of the missing type to identify the parser, we expect no return value, but also no exception
            assertNull(booleanConsumer.get());
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
            assertNull(parser.nextToken());
        }

        bytes = toXContent((builder, params) -> builder.startObject("type" + delimiter + "name").field("bool", true).endObject(),
                xContentType, randomBoolean());
        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            NamedXContentRegistry.UnknownNamedObjectException e = expectThrows(NamedXContentRegistry.UnknownNamedObjectException.class,
                    () -> parseTypedKeysObject(parser, delimiter, Boolean.class, a -> {}));
            assertEquals("Unknown Boolean [type]", e.getMessage());
            assertEquals("type", e.getName());
            assertEquals("java.lang.Boolean", e.getCategoryClass());
        }

        final long longValue = randomLong();
        final boolean boolValue = randomBoolean();
        bytes = toXContent((builder, params) -> {
            builder.startObject("long" + delimiter + "l").field("field", longValue).endObject();
            builder.startObject("bool" + delimiter + "l").field("field", boolValue).endObject();
            return builder;
        }, xContentType, randomBoolean());

        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            SetOnce<Long> parsedLong = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Long.class, parsedLong::set);
            assertNotNull(parsedLong);
            assertEquals(longValue, parsedLong.get().longValue());

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            SetOnce<Boolean> parsedBoolean = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Boolean.class, parsedBoolean::set);
            assertNotNull(parsedBoolean);
            assertEquals(boolValue, parsedBoolean.get());

            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        }
    }
}
