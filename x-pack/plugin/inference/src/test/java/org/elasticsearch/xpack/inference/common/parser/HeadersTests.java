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
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HeadersTests extends AbstractBWCWireSerializationTestCase<Headers> {

    private enum HeadersDefinition {
        EMPTY(Map.of()),
        DEFINED(Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15)));

        private final Map<String, String> headers;

        HeadersDefinition(Map<String, String> headers) {
            this.headers = headers;
        }
    }

    public static Headers createRandom() {
        var headers = randomFrom(HeadersDefinition.values()).headers;
        return new Headers(headers);
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
        var currentHeaders = instance.headersMap();
        var newHeaders = new HashMap<>(currentHeaders);

        newHeaders.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
        return new Headers(newHeaders);
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

    public void testToXContent_WhenNull_OmitsField() {
        expectThrows(NullPointerException.class, () -> new Headers((Map<String, String>) null));
    }

    public void testToXContent_WhenEmptyMap() throws IOException {
        var headers = new Headers(Map.of());
        assertThat(toXContentString(headers), is(XContentHelper.stripWhitespace("""
                {
                  "headers": {}
                }
            """)));
    }

    public void testToXContent_WhenWithEntries() throws IOException {
        var headerMap = Map.of("key", "value");
        var headers = new Headers(headerMap);
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
        parseJson(json, parsed -> assertThat(parsed.headersMap(), is(Map.of("key", "value"))));
    }

    public void testParse_WhenHeadersMissing_ReturnsNullHeaders() throws IOException {
        var json = """
            {
            }
            """;
        parseJson(json, Assert::assertNull);
    }

    public void testParse_WhenHeadersEmptyMap() throws IOException {
        var json = """
            {
              "headers": {}
            }
            """;
        parseJson(json, parsed -> assertThat(parsed.headersMap(), anEmptyMap()));
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
        assertThat(exception.getCause().getMessage(), containsString("Failed to build [headers_parser] after last required field arrived"));
        assertThat(
            exception.getCause().getCause().getMessage(),
            containsString("Map field [headers] has an entry that is not valid, [key => 1]. Value type of [1] is not one of [String].;")
        );
    }

    public void testParse_Roundtrip() throws IOException {
        var original = createRandom();
        var json = toXContentString(original);
        parseJson(json, parsedHeaders -> assertThat(parsedHeaders, is(original)));
    }

    private static void parseJson(String jsonInput, Consumer<Headers> assertCallback) throws IOException {
        ConstructingObjectParser<Headers, Void> constructingObjectParser = new ConstructingObjectParser<>(
            "headers_parser",
            false,
            args -> Headers.create(args[0])
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
