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

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class RestRequestTests extends ESTestCase {
    public void testContentParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentParser());
        assertEquals("request body is required", e.getMessage());
        e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", singletonMap("source", "{}")).contentParser());
        assertEquals("request body is required", e.getMessage());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentParser().map());
        e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap(), emptyMap()).contentParser());
        assertEquals("request body is required", e.getMessage());
    }

    public void testApplyContentParser() throws IOException {
        new ContentRestRequest("", emptyMap()).applyContentParser(p -> fail("Shouldn't have been called"));
        new ContentRestRequest("", singletonMap("source", "{}")).applyContentParser(p -> fail("Shouldn't have been called"));
        AtomicReference<Object> source = new AtomicReference<>();
        new ContentRestRequest("{}", emptyMap()).applyContentParser(p -> source.set(p.map()));
        assertEquals(emptyMap(), source.get());
    }

    public void testContentOrSourceParam() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentOrSourceParam());
        assertEquals("request body or source parameter is required", e.getMessage());
        assertEquals(new BytesArray("stuff"), new ContentRestRequest("stuff", emptyMap()).contentOrSourceParam().v2());
        assertEquals(new BytesArray("stuff"),
            new ContentRestRequest("stuff", MapBuilder.<String, String>newMapBuilder()
                .put("source", "stuff2").put("source_content_type", "application/json").immutableMap()).contentOrSourceParam().v2());
        assertEquals(new BytesArray("{\"foo\": \"stuff\"}"),
            new ContentRestRequest("", MapBuilder.<String, String>newMapBuilder()
                .put("source", "{\"foo\": \"stuff\"}").put("source_content_type", "application/json").immutableMap())
                .contentOrSourceParam().v2());
        e = expectThrows(IllegalStateException.class, () ->
            new ContentRestRequest("", MapBuilder.<String, String>newMapBuilder()
                .put("source", "stuff2").immutableMap()).contentOrSourceParam());
        assertEquals("source and source_content_type parameters are required", e.getMessage());
    }

    public void testHasContentOrSourceParam() throws IOException {
        assertEquals(false, new ContentRestRequest("", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("stuff", emptyMap()).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("stuff", singletonMap("source", "stuff2")).hasContentOrSourceParam());
        assertEquals(true, new ContentRestRequest("", singletonMap("source", "stuff")).hasContentOrSourceParam());
    }

    public void testContentOrSourceParamParser() throws IOException {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).contentOrSourceParamParser());
        assertEquals("request body or source parameter is required", e.getMessage());
        assertEquals(emptyMap(), new ContentRestRequest("{}", emptyMap()).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), new ContentRestRequest("{}", singletonMap("source", "stuff2")).contentOrSourceParamParser().map());
        assertEquals(emptyMap(), new ContentRestRequest("", MapBuilder.<String, String>newMapBuilder()
            .put("source", "{}").put("source_content_type", "application/json").immutableMap()).contentOrSourceParamParser().map());
    }

    public void testWithContentOrSourceParamParserOrNull() throws IOException {
        new ContentRestRequest("", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertNull(parser));
        new ContentRestRequest("{}", emptyMap()).withContentOrSourceParamParserOrNull(parser -> assertEquals(emptyMap(), parser.map()));
        new ContentRestRequest("{}", singletonMap("source", "stuff2")).withContentOrSourceParamParserOrNull(parser ->
                assertEquals(emptyMap(), parser.map()));
        new ContentRestRequest("", MapBuilder.<String, String>newMapBuilder().put("source_content_type", "application/json")
            .put("source", "{}").immutableMap())
        .withContentOrSourceParamParserOrNull(parser ->
                assertEquals(emptyMap(), parser.map()));
    }

    public void testContentTypeParsing() {
        for (XContentType xContentType : XContentType.values()) {
            Map<String, List<String>> map = new HashMap<>();
            map.put("Content-Type", Collections.singletonList(xContentType.mediaType()));
            ContentRestRequest restRequest = new ContentRestRequest("", Collections.emptyMap(), map);
            assertEquals(xContentType, restRequest.getXContentType());

            map = new HashMap<>();
            map.put("Content-Type", Collections.singletonList(xContentType.mediaTypeWithoutParameters()));
            restRequest = new ContentRestRequest("", Collections.emptyMap(), map);
            assertEquals(xContentType, restRequest.getXContentType());
        }
    }

    public void testPlainTextSupport() {
        ContentRestRequest restRequest = new ContentRestRequest(randomAlphaOfLengthBetween(1, 30), Collections.emptyMap(),
            Collections.singletonMap("Content-Type",
                Collections.singletonList(randomFrom("text/plain", "text/plain; charset=utf-8", "text/plain;charset=utf-8"))));
        assertNull(restRequest.getXContentType());
    }

    public void testMalformedContentTypeHeader() {
        final String type = randomFrom("text", "text/:ain; charset=utf-8", "text/plain\";charset=utf-8", ":", "/", "t:/plain");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ContentRestRequest("", Collections.emptyMap(),
            Collections.singletonMap("Content-Type", Collections.singletonList(type))));
        assertEquals("invalid Content-Type header [" + type + "]", e.getMessage());
    }

    public void testNoContentTypeHeader() {
        ContentRestRequest contentRestRequest = new ContentRestRequest("", Collections.emptyMap(), Collections.emptyMap());
        assertNull(contentRestRequest.getXContentType());
    }

    public void testMultipleContentTypeHeaders() {
        List<String> headers = new ArrayList<>(randomUnique(() -> randomAlphaOfLengthBetween(1, 16), randomIntBetween(2, 10)));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ContentRestRequest("", Collections.emptyMap(),
            Collections.singletonMap("Content-Type", headers)));
        assertEquals("only one Content-Type header should be provided", e.getMessage());
    }

    public void testRequiredContent() {
        Exception e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", emptyMap()).requiredContent());
        assertEquals("request body is required", e.getMessage());
        assertEquals(new BytesArray("stuff"), new ContentRestRequest("stuff", emptyMap()).requiredContent());
        assertEquals(new BytesArray("stuff"),
            new ContentRestRequest("stuff", MapBuilder.<String, String>newMapBuilder()
                .put("source", "stuff2").put("source_content_type", "application/json").immutableMap()).requiredContent());
        e = expectThrows(ElasticsearchParseException.class, () ->
            new ContentRestRequest("", MapBuilder.<String, String>newMapBuilder()
                .put("source", "{\"foo\": \"stuff\"}").put("source_content_type", "application/json").immutableMap())
                .requiredContent());
        assertEquals("request body is required", e.getMessage());
        e = expectThrows(IllegalStateException.class, () ->
            new ContentRestRequest("test", null, Collections.emptyMap()).requiredContent());
        assertEquals("unknown content type", e.getMessage());
    }

    private static final class ContentRestRequest extends RestRequest {
        private final BytesArray content;

        ContentRestRequest(String content, Map<String, String> params) {
            this(content, params, Collections.singletonMap("Content-Type", Collections.singletonList("application/json")));
        }

        ContentRestRequest(String content, Map<String, String> params, Map<String, List<String>> headers) {
            super(NamedXContentRegistry.EMPTY, params, "not used by this test", headers);
            this.content = new BytesArray(content);
        }

        @Override
        public boolean hasContent() {
            return Strings.hasLength(content);
        }

        @Override
        public BytesReference content() {
            return content;
        }

        @Override
        public String uri() {
            throw new UnsupportedOperationException("Not used by this test");
        }

        @Override
        public Method method() {
            throw new UnsupportedOperationException("Not used by this test");
        }
    }
}
