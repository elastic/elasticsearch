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

        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        namedXContents.add(new NamedXContentRegistry.Entry(Boolean.class, new ParseField("bool"), parser -> {
            ensureExpectedToken(XContentParser.Token.VALUE_BOOLEAN, parser.nextToken(), parser::getTokenLocation);
            return parser.booleanValue();
        }));
        namedXContents.add(new NamedXContentRegistry.Entry(Long.class, new ParseField("long"), parser -> {
            ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser::getTokenLocation);
            return parser.longValue();
        }));
        final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(namedXContents);

        BytesReference bytes = toXContent((builder, params) -> builder.field("test", 0), xContentType, randomBoolean());
        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            parser.nextToken();
            ParsingException e = expectThrows(ParsingException.class, () -> parseTypedKeysObject(parser, delimiter, Boolean.class));
            assertEquals("Failed to parse object: expecting token of type [FIELD_NAME] but found [START_OBJECT]", e.getMessage());

            parser.nextToken();
            e = expectThrows(ParsingException.class, () -> parseTypedKeysObject(parser, delimiter, Boolean.class));
            assertEquals("Cannot parse object of class [Boolean] without type information. Set [typed_keys] parameter " +
                    "on the request to ensure the type information is added to the response output", e.getMessage());
        }

        bytes = toXContent((builder, params) -> builder.field("type" + delimiter + "name", 0), xContentType, randomBoolean());
        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);

            NamedXContentRegistry.UnknownNamedObjectException e = expectThrows(NamedXContentRegistry.UnknownNamedObjectException.class,
                    () -> parseTypedKeysObject(parser, delimiter, Boolean.class));
            assertEquals("Unknown Boolean [type]", e.getMessage());
            assertEquals("type", e.getName());
            assertEquals("java.lang.Boolean", e.getCategoryClass());
        }

        final long longValue = randomLong();
        final boolean boolValue = randomBoolean();
        bytes = toXContent((builder, params) -> {
            builder.field("long" + delimiter + "l", longValue);
            builder.field("bool" + delimiter + "b", boolValue);
            return builder;
        }, xContentType, randomBoolean());

        try (XContentParser parser = xContentType.xContent().createParser(namedXContentRegistry, bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            Long parsedLong = parseTypedKeysObject(parser, delimiter, Long.class);
            assertNotNull(parsedLong);
            assertEquals(longValue, parsedLong.longValue());

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            Boolean parsedBoolean = parseTypedKeysObject(parser, delimiter, Boolean.class);
            assertNotNull(parsedBoolean);
            assertEquals(boolValue, parsedBoolean);

            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        }
    }
}
