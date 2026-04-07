/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class HeadersTests extends AbstractBWCWireSerializationTestCase<Headers> {

    public static Headers createRandom() {
        return randomFrom(
            Headers.UNDEFINED_INSTANCE,
            Headers.NULL_INSTANCE,
            new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))))
        );
    }

    public static Headers createRandomNonNull() {
        return randomFrom(
            Headers.UNDEFINED_INSTANCE,
            new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))))
        );
    }

    @Override
    protected Writeable.Reader<Headers> instanceReader() {
        return Headers::new;
    }

    @Override
    protected Headers createTestInstance() {
        return createRandom();
    }

    @Override
    protected Headers mutateInstance(Headers instance) throws IOException {
        return doMutateInstance(instance);
    }

    public static Headers doMutateInstance(Headers instance) {
        var statefulValue = instance.mapValue();
        if (statefulValue.isPresent()) {
            var newHeaders = new HashMap<>(statefulValue.get());
            newHeaders.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
            var withNewKey = new Headers(StatefulValue.of(newHeaders));
            return randomFrom(withNewKey, Headers.NULL_INSTANCE, Headers.UNDEFINED_INSTANCE);
        }
        if (statefulValue.isNull()) {
            var withValue = new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))));
            return randomFrom(withValue, Headers.UNDEFINED_INSTANCE);
        }
        var withValue = new Headers(StatefulValue.of(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))));
        return randomFrom(withValue, Headers.NULL_INSTANCE);
    }

    @Override
    protected Headers mutateInstanceForVersion(Headers instance, TransportVersion version) {
        return instance;
    }

    private static String toXContentString(Headers headers) throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        headers.toXContent(builder, null);
        builder.endObject();
        return Strings.toString(builder);
    }

    public void testConstructor_WhenNull_ThrowsNullPointerException() {
        expectThrows(NullPointerException.class, () -> new Headers((StatefulValue<Map<String, String>>) null));
    }

    public void testIsPresent_WhenAbsent() {
        assertFalse(Headers.UNDEFINED_INSTANCE.isPresent());
    }

    public void testIsPresent_WhenNull() {
        assertFalse(Headers.NULL_INSTANCE.isPresent());
    }

    public void testState_WhenWithValue() {
        var headers = new Headers(StatefulValue.of(Map.of("k", "v")));
        assertTrue(headers.isPresent());
        assertFalse(headers.isNull());
        assertFalse(headers.isEmpty());
    }

    public void testIsNull_WhenAbsent() {
        assertFalse(Headers.UNDEFINED_INSTANCE.isNull());
    }

    public void testIsNull_WhenNull() {
        assertTrue(Headers.NULL_INSTANCE.isNull());
    }

    public void testIsNull_WhenWithValue() {
        assertFalse(new Headers(StatefulValue.of(Map.of("k", "v"))).isNull());
    }

    public void testIsEmpty_WhenAbsent() {
        assertTrue(Headers.UNDEFINED_INSTANCE.isEmpty());
    }

    public void testIsEmpty_WhenNull() {
        assertTrue(Headers.NULL_INSTANCE.isEmpty());
    }

    public void testIsEmpty_WhenPresentWithEmptyMap() {
        var headers = new Headers(StatefulValue.of(Map.of()));
        assertTrue(headers.isEmpty());
        assertTrue(headers.isPresent());
    }

    public void testIsEmpty_WhenPresentWithEntries() {
        assertFalse(new Headers(StatefulValue.of(Map.of("k", "v"))).isEmpty());
    }

    public void testToXContent_WhenEmptyMap() throws IOException {
        var headers = new Headers(StatefulValue.of(Map.of()));
        assertThat(toXContentString(headers), is(XContentHelper.stripWhitespace("""
            {}
            """)));
    }

    public void testToXContent_WhenWithEntries() throws IOException {
        var headerMap = Map.of("key", "value");
        var headers = new Headers(StatefulValue.of(headerMap));
        assertThat(toXContentString(headers), is(XContentHelper.stripWhitespace("""
            {
              "headers": {
                "key": "value"
              }
            }
            """)));
    }

    public void testParse_WithHeaders() throws IOException {
        var json = """
            {
              "headers": {
                "key": "value"
              }
            }
            """;
        parseJson(json, parsed -> {
            assertTrue(parsed.mapValue().isPresent());
            assertThat(parsed.mapValue().get(), is(Map.of("key", "value")));
        });
    }

    public void testParse_WhenHeadersMissing_ReturnsNullHeaders() throws IOException {
        var json = """
            {
            }
            """;
        parseJson(json, parsed -> assertThat(parsed, sameInstance(Headers.UNDEFINED_INSTANCE)));
    }

    public void testParse_WhenHeadersEmptyMap() throws IOException {
        var json = """
            {
              "headers": {}
            }
            """;
        parseJson(json, parsed -> assertThat(parsed, sameInstance(Headers.NULL_INSTANCE)));
    }

    public void testParse_WhenHeadersIsSetToNull() throws IOException {
        var json = """
            {
              "headers": null
            }
            """;
        parseJson(json, parsed -> assertThat(parsed, sameInstance(Headers.NULL_INSTANCE)));
    }

    public void testParse_ThrowsWhenValueNotString() {
        var json = """
            {
              "headers": {
                "key": 1
              }
            }
            """;
        var exception = expectThrows(XContentParseException.class, () -> parseJson(json, parsed -> {}));
        assertThat(exception.getMessage(), containsString("[headers_parser] failed to parse field [headers]"));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "Map field [headers] has an entry that is not valid, [key => 1]. Value type of [Integer] is not one of [String].;"
            )
        );
    }

    public void testParse_ThrowsWhenValueIsAnObject() {
        var json = """
            {
              "headers": {
                "key": {}
              }
            }
            """;
        var exception = expectThrows(XContentParseException.class, () -> parseJson(json, parsed -> {}));
        assertThat(exception.getMessage(), containsString("[headers_parser] failed to parse field [headers]"));
        assertThat(
            exception.getCause().getMessage(),
            containsString("Map field [headers] has an entry that is not valid, [key => {}]. Value type of [Map] is not one of [String].;")
        );
    }

    public void testParse_Roundtrip() throws IOException {
        // The reason we don't allow null here is that when a Headers::NULL_INSTANCE is serialized to xContent
        // it is not written (aka would look like this {}) instead of it being written {"headers": null}.
        // This is because it's only used for the update API to indicate that the existing headers should be removed.
        var original = createRandomNonNull();
        var json = toXContentString(original);
        parseJson(json, parsedHeaders -> assertThat(parsedHeaders, is(original)));
    }

    public void testParse_RoundtripNull() throws IOException {
        // When a null headers is serialized to xContent, it is not written at all
        // (aka would look like this {}) instead of it being written {"headers": null}. This is because it's only used for
        // the update API to indicate that the existing headers should be removed.
        var json = toXContentString(Headers.NULL_INSTANCE);
        parseJson(json, parsedHeaders -> assertThat(parsedHeaders, is(Headers.UNDEFINED_INSTANCE)));
    }

    private static void parseJson(String jsonInput, Consumer<Headers> assertCallback) throws IOException {
        ConstructingObjectParser<Headers, Void> constructingObjectParser = new ConstructingObjectParser<>(
            "headers_parser",
            false,
            args -> Headers.create(args[0], "root")
        );
        Headers.initParser(constructingObjectParser);

        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, jsonInput)
        ) {
            parser.nextToken();
            var parsed = constructingObjectParser.parse(parser, null);
            assertCallback.accept(parsed);
        }
    }
}
