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
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseTypedKeysObject;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;

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

    public void testStoredFieldsValueString() throws IOException {
        final String value = randomAlphaOfLengthBetween(0, 10);
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, result));
    }

    public void testStoredFieldsValueInt() throws IOException {
        final Integer value = randomInt();
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, result));
    }

    public void testStoredFieldsValueLong() throws IOException {
        final Long value = randomLong();
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, result));
    }

    public void testStoredFieldsValueDouble() throws IOException {
        final Double value = randomDouble();
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, ((Number) result).doubleValue(), 0.0d));
    }

    public void testStoredFieldsValueFloat() throws IOException {
        final Float value = randomFloat();
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, ((Number) result).floatValue(), 0.0f));
    }

    public void testStoredFieldsValueBoolean() throws IOException {
        final Boolean value = randomBoolean();
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> assertEquals(value, result));
    }

    public void testStoredFieldsValueBinary() throws IOException {
        final byte[] value = randomUnicodeOfLength(scaledRandomIntBetween(10, 1000)).getBytes("UTF-8");
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> {
            if (xcontentType == XContentType.JSON || xcontentType == XContentType.YAML) {
                //binary values will be parsed back and returned as base64 strings when reading from json and yaml
                assertArrayEquals(value, Base64.getDecoder().decode((String) result));
            } else {
                //binary values will be parsed back and returned as BytesArray when reading from cbor and smile
                assertArrayEquals(value, ((BytesArray) result).array());
            }
        });
    }

    public void testStoredFieldsValueNull() throws IOException {
        assertParseFieldsSimpleValue(null, (xcontentType, result) -> assertNull(result));
    }

    public void testStoredFieldsValueObject() throws IOException {
        assertParseFieldsValue((builder) -> builder.startObject().endObject(),
                (xcontentType, result) -> assertThat(result, instanceOf(Map.class)));
    }

    public void testStoredFieldsValueArray() throws IOException {
        assertParseFieldsValue((builder) -> builder.startArray().endArray(),
                (xcontentType, result) -> assertThat(result, instanceOf(List.class)));
    }

    public void testParseFieldsValueUnknown() {
        ParsingException e = expectThrows(ParsingException.class, () ->
                assertParseFieldsValue((builder) -> {}, (x, r) -> fail("Should have thrown a parsing exception")));
        assertThat(e.getMessage(), containsString("unexpected token"));
    }

    private void assertParseFieldsSimpleValue(final Object value, final CheckedBiConsumer<XContentType, Object, IOException> assertConsumer)
            throws IOException {
        assertParseFieldsValue((builder) -> builder.value(value), assertConsumer);
    }

    private void assertParseFieldsValue(final CheckedConsumer<XContentBuilder, IOException> fieldBuilder,
                                        final CheckedBiConsumer<XContentType, Object, IOException> assertConsumer) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            final String fieldName = randomAlphaOfLengthBetween(0, 10);

            builder.startObject();
            builder.startArray(fieldName);
            fieldBuilder.accept(builder);
            builder.endArray();
            builder.endObject();

            try (XContentParser parser = createParser(builder)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ensureFieldName(parser, parser.nextToken(), fieldName);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser::getTokenLocation);
                assertNotNull(parser.nextToken());
                assertConsumer.accept(xContentType, XContentParserUtils.parseFieldsValue(parser));
                ensureExpectedToken(XContentParser.Token.END_ARRAY, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
                assertNull(parser.nextToken());
            }
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
        try (XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
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
        try (XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            NamedObjectNotFoundException e = expectThrows(NamedObjectNotFoundException.class,
                    () -> parseTypedKeysObject(parser, delimiter, Boolean.class, a -> {}));
            assertThat(e.getMessage(), endsWith("unknown field [type]"));
        }

        final long longValue = randomLong();
        final boolean boolValue = randomBoolean();
        bytes = toXContent((builder, params) -> {
            builder.startObject("long" + delimiter + "l").field("field", longValue).endObject();
            builder.startObject("bool" + delimiter + "l").field("field", boolValue).endObject();
            return builder;
        }, xContentType, randomBoolean());

        try (XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
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

    public void testParseTypedKeysObjectErrors() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        {
            BytesReference bytes = toXContent((builder, params) -> builder.startObject("name").field("field", 0).endObject(), xContentType,
                    randomBoolean());
            try (XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
                ParsingException exception = expectThrows(ParsingException.class,
                        () -> parseTypedKeysObject(parser, "#", Boolean.class, o -> {
                        }));
                assertEquals("Failed to parse object: unexpected token [FIELD_NAME] found", exception.getMessage());
            }
        }
        {
            BytesReference bytes = toXContent((builder, params) -> builder.startObject("").field("field", 0).endObject(), xContentType,
                    randomBoolean());
            try (XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
                ParsingException exception = expectThrows(ParsingException.class,
                        () -> parseTypedKeysObject(parser, "#", Boolean.class, o -> {
                        }));
                assertEquals("Failed to parse object: empty key", exception.getMessage());
            }
        }
    }
}
