/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
            ParsingException e = expectThrows(
                ParsingException.class,
                () -> ensureExpectedToken(randomToken, parser.currentToken(), parser)
            );
            assertEquals("Failed to parse object: expecting token of type [" + randomToken + "] but found [null]", e.getMessage());
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
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
        final byte[] value = randomUnicodeOfLength(scaledRandomIntBetween(10, 1000)).getBytes(StandardCharsets.UTF_8);
        assertParseFieldsSimpleValue(value, (xcontentType, result) -> {
            if (xcontentType.canonical() == XContentType.JSON) {
                // binary values will be parsed back and returned as base64 strings when reading from json
                assertArrayEquals(value, Base64.getDecoder().decode((String) result));
            } else {
                // cbor, smile, and yaml support binary
                assertArrayEquals(value, ((BytesArray) result).array());
            }
        });
    }

    public void testStoredFieldsValueNull() throws IOException {
        assertParseFieldsSimpleValue(null, (xcontentType, result) -> assertNull(result));
    }

    public void testStoredFieldsValueObject() throws IOException {
        assertParseFieldsValue(
            (builder) -> builder.startObject().endObject(),
            (xcontentType, result) -> assertThat(result, instanceOf(Map.class))
        );
    }

    public void testStoredFieldsValueArray() throws IOException {
        assertParseFieldsValue(
            (builder) -> builder.startArray().endArray(),
            (xcontentType, result) -> assertThat(result, instanceOf(List.class))
        );
    }

    public void testParseFieldsValueUnknown() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> assertParseFieldsValue((builder) -> {}, (x, r) -> fail("Should have thrown a parsing exception"))
        );
        assertThat(e.getMessage(), containsString("unexpected token"));
    }

    private void assertParseFieldsSimpleValue(final Object value, final CheckedBiConsumer<XContentType, Object, IOException> assertConsumer)
        throws IOException {
        assertParseFieldsValue((builder) -> builder.value(value), assertConsumer);
    }

    private void assertParseFieldsValue(
        final CheckedConsumer<XContentBuilder, IOException> fieldBuilder,
        final CheckedBiConsumer<XContentType, Object, IOException> assertConsumer
    ) throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            final String fieldName = randomAlphaOfLengthBetween(0, 10);

            builder.startObject();
            builder.startArray(fieldName);
            fieldBuilder.accept(builder);
            builder.endArray();
            builder.endObject();

            try (XContentParser parser = createParser(builder)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ensureFieldName(parser, parser.nextToken(), fieldName);
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                assertNotNull(parser.nextToken());
                assertConsumer.accept(xContentType, XContentParserUtils.parseFieldsValue(parser));
                ensureExpectedToken(XContentParser.Token.END_ARRAY, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
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

        BytesReference bytes = toXContent(
            (builder, params) -> builder.startObject("name").field("field", 0).endObject(),
            xContentType,
            randomBoolean()
        );
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            SetOnce<Boolean> booleanConsumer = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Boolean.class, booleanConsumer::set);
            // because of the missing type to identify the parser, we expect no return value, but also no exception
            assertNull(booleanConsumer.get());
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            assertNull(parser.nextToken());
        }

        bytes = toXContent(
            (builder, params) -> builder.startObject("type" + delimiter + "name").field("bool", true).endObject(),
            xContentType,
            randomBoolean()
        );
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            NamedObjectNotFoundException e = expectThrows(
                NamedObjectNotFoundException.class,
                () -> parseTypedKeysObject(parser, delimiter, Boolean.class, a -> {})
            );
            assertThat(e.getMessage(), endsWith("unknown field [type]"));
        }

        final long longValue = randomLong();
        final boolean boolValue = randomBoolean();
        bytes = toXContent((builder, params) -> {
            builder.startObject("long" + delimiter + "l").field("field", longValue).endObject();
            builder.startObject("bool" + delimiter + "l").field("field", boolValue).endObject();
            return builder;
        }, xContentType, randomBoolean());

        try (
            XContentParser parser = xContentType.xContent()
                .createParser(namedXContentRegistry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            SetOnce<Long> parsedLong = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Long.class, parsedLong::set);
            assertNotNull(parsedLong);
            assertEquals(longValue, parsedLong.get().longValue());

            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            SetOnce<Boolean> parsedBoolean = new SetOnce<>();
            parseTypedKeysObject(parser, delimiter, Boolean.class, parsedBoolean::set);
            assertNotNull(parsedBoolean);
            assertEquals(boolValue, parsedBoolean.get());

            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        }
    }

    public void testParseTypedKeysObjectErrors() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        {
            BytesReference bytes = toXContent(
                (builder, params) -> builder.startObject("name").field("field", 0).endObject(),
                xContentType,
                randomBoolean()
            );
            try (
                XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                ParsingException exception = expectThrows(
                    ParsingException.class,
                    () -> parseTypedKeysObject(parser, "#", Boolean.class, o -> {})
                );
                assertEquals("Failed to parse object: unexpected token [FIELD_NAME] found", exception.getMessage());
            }
        }
        {
            BytesReference bytes = toXContent(
                (builder, params) -> builder.startObject("").field("field", 0).endObject(),
                xContentType,
                randomBoolean()
            );
            try (
                XContentParser parser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ParsingException exception = expectThrows(
                    ParsingException.class,
                    () -> parseTypedKeysObject(parser, "#", Boolean.class, o -> {})
                );
                assertEquals("Failed to parse object: empty key", exception.getMessage());
            }
        }
    }

    public void testParseListWithIndex_IncrementsIndexBy1ForEachEntryInList() throws IOException {
        String jsonString = """
            ["a", "b", "c"]
            """;

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        List<String> results;
        var indices = new ArrayList<Integer>();

        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(parserConfig, jsonString.getBytes(StandardCharsets.UTF_8))
        ) {
            if (jsonParser.currentToken() == null) {
                jsonParser.nextToken();
            }

            results = XContentParserUtils.parseList(jsonParser, (parser, index) -> {
                XContentParser.Token token = parser.currentToken();
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, token, parser);
                indices.add(index);

                return parser.text();
            });
        }

        assertThat(results, Matchers.is(List.of("a", "b", "c")));
        assertThat(indices, Matchers.is(List.of(0, 1, 2)));
    }
}
