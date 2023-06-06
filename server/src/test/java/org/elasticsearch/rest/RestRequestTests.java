/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestRequestTests extends ESTestCase {

    public void testContentConsumesContent() {
        runConsumesContentTest(RestRequest::content, true);
    }

    public void testRequiredContentConsumesContent() {
        runConsumesContentTest(RestRequest::requiredContent, true);
    }

    public void testContentParserConsumesContent() {
        runConsumesContentTest(RestRequest::contentParser, true);
    }

    public void testContentOrSourceParamConsumesContent() {
        runConsumesContentTest(RestRequest::contentOrSourceParam, true);
    }

    public void testContentOrSourceParamsParserConsumesContent() {
        runConsumesContentTest(RestRequest::contentOrSourceParamParser, true);
    }

    public void testWithContentOrSourceParamParserOrNullConsumesContent() {
        @SuppressWarnings("unchecked")
        CheckedConsumer<XContentParser, IOException> consumer = mock(CheckedConsumer.class);
        runConsumesContentTest(request -> request.withContentOrSourceParamParserOrNull(consumer), true);
    }

    public void testApplyContentParserConsumesContent() {
        @SuppressWarnings("unchecked")
        CheckedConsumer<XContentParser, IOException> consumer = mock(CheckedConsumer.class);
        runConsumesContentTest(request -> request.applyContentParser(consumer), true);
    }

    public void testHasContentDoesNotConsumesContent() {
        runConsumesContentTest(RestRequest::hasContent, false);
    }

    public void testContentLengthDoesNotConsumesContent() {
        runConsumesContentTest(RestRequest::contentLength, false);
    }

    private <T extends Exception> void runConsumesContentTest(final CheckedConsumer<RestRequest, T> consumer, final boolean expected) {
        final HttpRequest httpRequest = mock(HttpRequest.class);
        when(httpRequest.uri()).thenReturn("");
        when(httpRequest.content()).thenReturn(new BytesArray(new byte[1]));
        when(httpRequest.getHeaders()).thenReturn(
            Collections.singletonMap("Content-Type", Collections.singletonList(randomFrom("application/json", "application/x-ndjson")))
        );
        final RestRequest request = RestRequest.request(XContentParserConfiguration.EMPTY, httpRequest, mock(HttpChannel.class));
        assertFalse(request.isContentConsumed());
        try {
            consumer.accept(request);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(request.isContentConsumed(), equalTo(expected));
    }

    public void testContentParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> contentRestRequest("", emptyMap()).contentParser());
        assertEquals("request body is required", e.getMessage());
        e = expectThrows(ElasticsearchParseException.class, () -> contentRestRequest("", singletonMap("source", "{}")).contentParser());
        assertEquals("request body is required", e.getMessage());
        assertEquals(emptyMap(), contentRestRequest("{}", emptyMap()).contentParser().map());
        e = expectThrows(ElasticsearchParseException.class, () -> contentRestRequest("", emptyMap(), emptyMap()).contentParser());
        assertEquals("request body is required", e.getMessage());
    }

    public void testApplyContentParser() throws IOException {
        contentRestRequest("", emptyMap()).applyContentParser(p -> fail("Shouldn't have been called"));
        contentRestRequest("", singletonMap("source", "{}")).applyContentParser(p -> fail("Shouldn't have been called"));
        AtomicReference<Object> source = new AtomicReference<>();
        contentRestRequest("{}", emptyMap()).applyContentParser(p -> source.set(p.map()));
        assertEquals(emptyMap(), source.get());
    }

    public void testContentOrSourceParam() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> contentRestRequest("", emptyMap()).contentOrSourceParam());
        assertEquals("request body or source parameter is required", e.getMessage());
        assertEquals(new BytesArray("stuff"), contentRestRequest("stuff", emptyMap()).contentOrSourceParam().v2());
        assertEquals(
            new BytesArray("stuff"),
            contentRestRequest("stuff", Map.of("source", "stuff2", "source_content_type", "application/json")).contentOrSourceParam().v2()
        );
        assertEquals(
            new BytesArray("{\"foo\": \"stuff\"}"),
            contentRestRequest("", Map.of("source", "{\"foo\": \"stuff\"}", "source_content_type", "application/json"))
                .contentOrSourceParam()
                .v2()
        );
        e = expectThrows(IllegalStateException.class, () -> contentRestRequest("", Map.of("source", "stuff2")).contentOrSourceParam());
        assertEquals("source and source_content_type parameters are required", e.getMessage());
    }

    public void testHasContentOrSourceParam() throws IOException {
        assertEquals(false, contentRestRequest("", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, contentRestRequest("stuff", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, contentRestRequest("stuff", singletonMap("source", "stuff2")).hasContentOrSourceParam());
        assertEquals(true, contentRestRequest("", singletonMap("source", "stuff")).hasContentOrSourceParam());
    }

    public void testContentOrSourceParamParser() throws IOException {
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> contentRestRequest("", emptyMap()).contentOrSourceParamParser()
        );
        assertEquals("request body or source parameter is required", e.getMessage());
        assertEquals(emptyMap(), contentRestRequest("{}", emptyMap()).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), contentRestRequest("{}", singletonMap("source", "stuff2")).contentOrSourceParamParser().map());
        assertEquals(
            emptyMap(),
            contentRestRequest("", Map.of("source", "{}", "source_content_type", "application/json")).contentOrSourceParamParser().map()
        );
    }

    public void testWithContentOrSourceParamParserOrNull() throws IOException {
        contentRestRequest("", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertNull(parser));
        contentRestRequest("{}", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertEquals(emptyMap(), parser.map()));
        contentRestRequest("{}", singletonMap("source", "stuff2")).withContentOrSourceParamParserOrNull(
            parser -> assertEquals(emptyMap(), parser.map())
        );
        contentRestRequest("", Map.of("source_content_type", "application/json", "source", "{}")).withContentOrSourceParamParserOrNull(
            parser -> assertEquals(emptyMap(), parser.map())
        );
    }

    public void testContentTypeParsing() {
        for (XContentType xContentType : XContentType.values()) {
            Map<String, List<String>> map = new HashMap<>();
            map.put("Content-Type", Collections.singletonList(xContentType.mediaType()));
            RestRequest restRequest = contentRestRequest("", Collections.emptyMap(), map);
            assertEquals(xContentType, restRequest.getXContentType());

            map = new HashMap<>();
            map.put("Content-Type", Collections.singletonList(xContentType.mediaTypeWithoutParameters()));
            restRequest = contentRestRequest("", Collections.emptyMap(), map);
            assertEquals(xContentType, restRequest.getXContentType());
        }
    }

    public void testPlainTextSupport() {
        RestRequest restRequest = contentRestRequest(
            randomAlphaOfLengthBetween(1, 30),
            Collections.emptyMap(),
            Collections.singletonMap(
                "Content-Type",
                Collections.singletonList(randomFrom("text/plain", "text/plain; charset=utf-8", "text/plain;charset=utf-8"))
            )
        );
        assertNull(restRequest.getXContentType());
    }

    public void testMalformedContentTypeHeader() {
        final String type = randomFrom("text", "text/:ain; charset=utf-8", "text/plain\";charset=utf-8", ":", "/", "t:/plain");
        final RestRequest.MediaTypeHeaderException e = expectThrows(RestRequest.MediaTypeHeaderException.class, () -> {
            final Map<String, List<String>> headers = Collections.singletonMap("Content-Type", Collections.singletonList(type));
            contentRestRequest("", Collections.emptyMap(), headers);
        });
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getMessage(), equalTo("invalid media-type [" + type + "]"));
        assertThat(e.getMessage(), equalTo("Invalid media-type value on headers [Content-Type]"));
    }

    public void testInvalidMediaTypeCharacter() {
        List<String> headers = List.of("a/b[", "a/b]", "a/b\\");
        for (String header : headers) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RestRequest.parseContentType(Collections.singletonList(header))
            );
            assertThat(e.getMessage(), equalTo("invalid Content-Type header [" + header + "]"));
        }
    }

    public void testNoContentTypeHeader() {
        RestRequest contentRestRequest = contentRestRequest("", Collections.emptyMap(), Collections.emptyMap());
        assertNull(contentRestRequest.getXContentType());
    }

    public void testMultipleContentTypeHeaders() {
        List<String> headers = new ArrayList<>(randomUnique(() -> randomAlphaOfLengthBetween(1, 16), randomIntBetween(2, 10)));
        final RestRequest.MediaTypeHeaderException e = expectThrows(
            RestRequest.MediaTypeHeaderException.class,
            () -> contentRestRequest("", Collections.emptyMap(), Collections.singletonMap("Content-Type", headers))
        );
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf((IllegalArgumentException.class)));
        assertThat(e.getCause().getMessage(), equalTo("Incorrect header [Content-Type]. Only one value should be provided"));
        assertThat(e.getMessage(), equalTo("Invalid media-type value on headers [Content-Type]"));
    }

    public void testRequiredContent() {
        Exception e = expectThrows(ElasticsearchParseException.class, () -> contentRestRequest("", emptyMap()).requiredContent());
        assertEquals("request body is required", e.getMessage());
        assertEquals(new BytesArray("stuff"), contentRestRequest("stuff", emptyMap()).requiredContent());
        assertEquals(
            new BytesArray("stuff"),
            contentRestRequest("stuff", Map.of("source", "stuff2", "source_content_type", "application/json")).requiredContent()
        );
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> contentRestRequest("", Map.of("source", "{\"foo\": \"stuff\"}", "source_content_type", "application/json"))
                .requiredContent()
        );
        assertEquals("request body is required", e.getMessage());
        e = expectThrows(IllegalStateException.class, () -> contentRestRequest("test", null, Collections.emptyMap()).requiredContent());
        assertEquals("unknown content type", e.getMessage());
    }

    public void testCopyRequestWithAdditionalParams() {
        RestRequest request = contentRestRequest("content", new HashMap<>(Map.of("foo", "bar", "baz", "blah")));
        assertEquals("bar", request.param("foo"));
        assertEquals("blah", request.param("baz"));
        RestRequest.copyRequestWithAdditionalParams(request, Map.of("foo", "updated", "something", "new"));
        assertEquals("updated", request.param("foo"));
        assertEquals("blah", request.param("baz"));
        assertEquals("new", request.param("something"));
    }

    public static RestRequest contentRestRequest(String content, Map<String, String> params) {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Content-Type", Collections.singletonList("application/json"));
        return contentRestRequest(content, params, headers);
    }

    private static RestRequest contentRestRequest(String content, Map<String, String> params, Map<String, List<String>> headers) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withHeaders(headers);
        builder.withContent(new BytesArray(content), null);
        builder.withParams(params);
        return new ContentRestRequest(builder.build());
    }

    public static final class ContentRestRequest extends RestRequest {

        private final RestRequest restRequest;

        private ContentRestRequest(RestRequest restRequest) {
            super(
                restRequest.contentParserConfig(),
                restRequest.params(),
                restRequest.path(),
                restRequest.getHeaders(),
                restRequest.getHttpRequest(),
                restRequest.getHttpChannel()
            );
            this.restRequest = restRequest;
        }

        @Override
        public Method method() {
            return restRequest.method();
        }

        @Override
        public String uri() {
            return restRequest.uri();
        }

        @Override
        public BytesReference content() {
            return restRequest.content();
        }
    }

}
